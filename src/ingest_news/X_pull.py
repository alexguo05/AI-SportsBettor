import sys
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
import tempfile
import time
import os

import requests
from dotenv import dotenv_values
from google.cloud import storage
from google.oauth2 import service_account
from zoneinfo import ZoneInfo


def main() -> int:
    """Fetch recent tweets continuously from specified accounts and upload to GCS.

    - Builds batches of handles within X API 512-char query limit.
    - Runs an infinite loop, executing ONE batch per interval to respect rate limits.
    - Accumulates tweets in-memory for the full cycle of batches.
    - Uploads ONE consolidated JSONL file at the end of each cycle.
    - Persists global since_id to GCS only at the end of each full cycle.
    """

    # 1. Load configuration from config.json
    config_path = Path(__file__).resolve().parents[1] / "config" / "config.json"
    try:
        with config_path.open("r", encoding="utf-8") as cf:
            app_cfg = json.load(cf)
    except Exception as e:
        print(f"ERROR: failed to load config.json: {e}", file=sys.stderr)
        return 1

    base_handles = app_cfg.get("x_base_handles", [])
    tweet_max_results = int(app_cfg.get("tweet_max_results", 100))
    poll_interval_sec = int(app_cfg.get("x_poll_interval_sec", 30))
    gcs_bucket_name = app_cfg.get("gcs_bucket", "ai-sports-bettor")

    if not base_handles:
        print("ERROR: x_base_handles missing in config.json", file=sys.stderr)
        return 1

    # 2. Load secrets from src/.env (X_BEARER_TOKEN)
    src_dir = Path(__file__).resolve().parents[1]
    dotenv_path = src_dir / ".env"
    env_vars = dotenv_values(dotenv_path) if dotenv_path.exists() else {}
    bearer_token = env_vars.get("X_BEARER_TOKEN") or env_vars.get("BEARER_TOKEN") or os.getenv("X_BEARER_TOKEN")

    if not bearer_token:
        print("ERROR: X_BEARER_TOKEN not set in src/.env", file=sys.stderr)
        return 1

    # 3. GCS setup using Service Account from src/
    # Local dev convenience: default to repo SA JSON if GOOGLE_APPLICATION_CREDENTIALS not set
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        default_sa_path = src_dir / "ai-sports-bettor-559e8837739f.json"
        if default_sa_path.exists():
            # Set it for Google libraries to pick up automatically if needed
            # But we also load explicitly below for clarity
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(default_sa_path)

    try:
        # Prefer loading explicit credentials if the file exists in src/
        sa_path = src_dir / "ai-sports-bettor-559e8837739f.json"
        if sa_path.exists():
            creds = service_account.Credentials.from_service_account_file(str(sa_path))
            gcs_client = storage.Client(credentials=creds, project=creds.project_id)
        else:
            # Fallback to ADC
            gcs_client = storage.Client()
    except Exception as e:
        print(f"ERROR: Failed to initialize GCS client: {e}", file=sys.stderr)
        return 1

    gcs_bucket = gcs_client.bucket(gcs_bucket_name)

    # 4. Build query batches
    suffix = " -is:retweet -is:reply"
    clauses = [f"from:{h}" for h in base_handles]
    query_strings: list[str] = []
    current: list[str] = []
    for clause in clauses:
        tentative = (" OR ".join(current + [clause]) + suffix) if current else (clause + suffix)
        if len(tentative) <= 512:
            current.append(clause)
        else:
            if current:
                query_strings.append(" OR ".join(current) + suffix)
            current = [clause]
    if current:
        query_strings.append(" OR ".join(current) + suffix)

    common_params = {
        "max_results": str(tweet_max_results),
        "tweet.fields": "created_at,author_id,attachments",
        "expansions": "author_id,attachments.media_keys",
        "user.fields": "username",
        "media.fields": "media_key,type,url,preview_image_url,width,height,alt_text,variants,duration_ms",
    }

    url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {"Authorization": f"Bearer {bearer_token}"}

    # In-memory cycle state
    next_index = 0
    cycle_since_id: str | None = None      # The baseline ID used for queries in the current cycle
    cycle_max_seen_id: str | None = None   # The highest ID seen so far during the current cycle
    cycle_tweets: list[str] = []           # JSONL lines accumulated for the current cycle

    # Load initial since_id from GCS
    try:
        since_blob = gcs_bucket.blob("ref/x_recent_since_id.json")
        if since_blob.exists():
            raw = since_blob.download_as_text()
            obj = json.loads(raw) if raw else {}
            cycle_since_id = obj.get("since_id")
    except Exception:
        cycle_since_id = None

    print(f"Starting continuous poll. Batches: {len(query_strings)}. Interval: {poll_interval_sec}s. Bucket: {gcs_bucket_name}")

    while True:
        num_batches = len(query_strings)
        if num_batches == 0:
            print("No queries constructed. Sleeping...", file=sys.stderr)
            time.sleep(poll_interval_sec)
            continue

        selected_index = next_index % num_batches
        
        # Start of a new cycle: freeze the baseline since_id for this pass and clear buffer
        if selected_index == 0:
            if cycle_max_seen_id is not None:
                # Use the max seen from the previous cycle as the new baseline
                cycle_since_id = cycle_max_seen_id
            
            # Reset running max and buffer for the new cycle
            cycle_max_seen_id = cycle_since_id
            cycle_tweets = []

        selected_query = query_strings[selected_index]

        # Prepare request
        params = {"query": selected_query, **common_params}
        if cycle_since_id:
            params["since_id"] = cycle_since_id

        payload = {}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
        except requests.HTTPError as e:
            print(f"ERROR: HTTP error: {e}", file=sys.stderr)
            try:
                print(resp.text, file=sys.stderr)
            except Exception:
                pass
        except Exception as e:
            print(f"ERROR: request failed: {e}", file=sys.stderr)

        # Process results
        includes = payload.get("includes", {}) if isinstance(payload, dict) else {}
        id_to_username = {}
        for u in includes.get("users", []) or []:
            uid = u.get("id")
            uname = u.get("username")
            if uid and uname:
                id_to_username[uid] = uname

        media_key_to_media = {}
        for m in includes.get("media", []) or []:
            mk = m.get("media_key")
            if mk:
                media_key_to_media[mk] = m

        tweets = payload.get("data", []) if isinstance(payload, dict) else []
        
        # Process retrieved tweets and add to cycle buffer
        if tweets:
            now_utc = datetime.now(timezone.utc)
            now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
            date_dir = now_et.strftime("%Y-%m-%d")

            for t in tweets:
                # Update running max ID
                tid = t.get("id")
                if tid:
                    try:
                        curr = int(cycle_max_seen_id) if cycle_max_seen_id and str(cycle_max_seen_id).isdigit() else 0
                        new_id = int(tid)
                        if new_id > curr:
                            cycle_max_seen_id = tid
                    except Exception:
                        cycle_max_seen_id = tid

                # Build record
                author_id = t.get("author_id")
                created_at_raw = t.get("created_at")
                created_at_et = None
                try:
                    if created_at_raw:
                        created_dt_utc = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                        created_at_et = created_dt_utc.astimezone(ZoneInfo("America/New_York")).isoformat()
                except Exception:
                    created_at_et = None

                # Media handling
                media_urls = []
                media_gcs_paths = []
                attachments = t.get("attachments") or {}
                used_names = set()
                
                for mk in attachments.get("media_keys", []) or []:
                    m = media_key_to_media.get(mk)
                    if not m:
                        continue
                    download_url = None
                    mtype = m.get("type")
                    if mtype == "photo":
                        download_url = m.get("url")
                    elif mtype in ("video", "animated_gif"):
                        variants = m.get("variants", []) or []
                        mp4s = [v for v in variants if v.get("content_type") == "video/mp4"]
                        if mp4s:
                            best = max(mp4s, key=lambda v: v.get("bit_rate", 0))
                            download_url = best.get("url")
                        if not download_url:
                            download_url = m.get("preview_image_url")
                    else:
                        download_url = m.get("url") or m.get("preview_image_url")

                    if not download_url:
                        continue
                    
                    media_urls.append(download_url)
                    
                    # Media upload (happens immediately)
                    try:
                        parsed = urlparse(download_url)
                        original_name = Path(parsed.path).name or "media"
                    except Exception:
                        original_name = "media"
                    
                    base_name = original_name
                    name_candidate = base_name
                    suffix_count = 1
                    while name_candidate in used_names:
                        parts = base_name.rsplit(".", 1)
                        if len(parts) == 2:
                            name_candidate = f"{parts[0]}_{suffix_count}.{parts[1]}"
                        else:
                            name_candidate = f"{base_name}_{suffix_count}"
                        suffix_count += 1
                    used_names.add(name_candidate)

                    tweet_id_str = tid or ""
                    media_blob_path = f"raw/X_news/{date_dir}/media/{tweet_id_str}_{name_candidate}" if tweet_id_str else f"raw/X_news/{date_dir}/media/{name_candidate}"
                    
                    try:
                        dresp = requests.get(download_url, stream=True, timeout=60)
                        dresp.raise_for_status()
                        content_type = dresp.headers.get("Content-Type")
                        with tempfile.NamedTemporaryFile(prefix="x_media_", suffix=".bin", delete=True) as tmp:
                            for chunk in dresp.iter_content(chunk_size=1024 * 1024):
                                if not chunk: continue
                                tmp.write(chunk)
                            tmp.flush()
                            tmp.seek(0)
                            media_blob = gcs_bucket.blob(media_blob_path)
                            media_blob.upload_from_file(tmp, content_type=content_type)
                        media_gcs_paths.append(f"gs://{gcs_bucket_name}/{media_blob_path}")
                        print(f"Uploaded media for tweet {tid} to gs://{gcs_bucket_name}/{media_blob_path}")
                    except Exception as media_err:
                        print(f"WARNING: failed to upload media {download_url}: {media_err}", file=sys.stderr)

                record = {
                    "id": tid,
                    "text": t.get("text"),
                    "author_username": id_to_username.get(author_id),
                    "created_at": created_at_raw,
                    "created_at_et": created_at_et,
                    "pulled_at_et": now_et.isoformat(),
                    "media_urls": media_urls,
                    "media_gcs_paths": media_gcs_paths,
                }
                cycle_tweets.append(json.dumps(record, ensure_ascii=False))

        # Check if cycle is complete (this was the last batch)
        if selected_index == num_batches - 1:
            # End of cycle: upload buffered tweets and update checkpoint
            if cycle_tweets:
                now_utc = datetime.now(timezone.utc)
                now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
                date_dir = now_et.strftime("%Y-%m-%d")
                ts = now_et.strftime("%Y%m%dT%H%M%S%z")
                gcs_blob_path = f"raw/X_news/{date_dir}/tweets_recent_{ts}.jsonl"
                
                try:
                    blob = gcs_bucket.blob(gcs_blob_path)
                    blob.upload_from_string("\n".join(cycle_tweets) + "\n", content_type="application/json")
                    print(f"Uploaded {len(cycle_tweets)} tweets (cycle complete) to gs://{gcs_bucket_name}/{gcs_blob_path}")
                except Exception as write_err:
                    print(f"WARNING: failed to upload consolidated tweets: {write_err}", file=sys.stderr)
            else:
                print("Cycle complete. No new tweets found.")

            # Persist checkpoint if advanced
            if cycle_max_seen_id is not None and cycle_max_seen_id != cycle_since_id:
                try:
                    since_blob = gcs_bucket.blob("ref/x_recent_since_id.json")
                    since_blob.upload_from_string(json.dumps({"since_id": cycle_max_seen_id}), content_type="application/json")
                    print(f"Updated checkpoint since_id to {cycle_max_seen_id}")
                except Exception as e:
                    print(f"WARNING: failed to persist checkpoint: {e}", file=sys.stderr)

        # Prepare for next batch
        next_index += 1
        time.sleep(poll_interval_sec)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
