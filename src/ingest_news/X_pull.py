import sys
import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import dotenv_values
from google.cloud import storage
from google.oauth2 import service_account
from zoneinfo import ZoneInfo
import os


def main() -> int:
    """Fetch recent tweets from specified accounts and save minimal fields to data/raw/news/.

    - Uses Twitter API v2 recent search with a single OR-combined query
    - Excludes retweets and replies
    - Requests up to 100 tweets
    - Saves text, author username, and creation time as JSONL
    """

    # Load bearer token from src/.env (prefer X_BEARER_TOKEN; fallback to BEARER_TOKEN)
    src_dir = Path(__file__).resolve().parents[1]
    dotenv_path = src_dir / ".env"
    config = dotenv_values(dotenv_path) if dotenv_path.exists() else {}
    bearer_token = config.get("X_BEARER_TOKEN") or config.get("BEARER_TOKEN")
    if not bearer_token:
        print("ERROR: X_BEARER_TOKEN (or BEARER_TOKEN) not set in src/.env", file=sys.stderr)
        return 1

    # Load config for base handles
    config_path = Path(__file__).resolve().parents[1] / "config" / "config.json"
    try:
        with config_path.open("r", encoding="utf-8") as cf:
            app_cfg = json.load(cf)
        base_handles = app_cfg.get("x_base_handles", ["NFLCharean", "AdamSchefter", "LauraRutledge"])
        tweet_max_results = int(app_cfg.get("tweet_max_results", 100))
    except Exception:
        base_handles = ["NFLCharean", "AdamSchefter", "LauraRutledge"]
        tweet_max_results = 100
    # Build one or more queries within X API 512-char limit, maximizing handles per query
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
            # Start new batch with this clause
            current = [clause]
    if current:
        query_strings.append(" OR ".join(current) + suffix)

    # GCS setup (reuse pattern from odds script)
    GCS_BUCKET = "ai-sports-bettor"
    GCS_SA_KEY_PATH = (Path(__file__).resolve().parents[1] / "ai-sports-bettor-559e8837739f.json")
    creds = service_account.Credentials.from_service_account_file(str(GCS_SA_KEY_PATH))
    gcs_client = storage.Client(credentials=creds, project=creds.project_id)
    gcs_bucket = gcs_client.bucket(GCS_BUCKET)

    # Load checkpoint: since_id from GCS ref path
    since_id: str | None = None
    try:
        since_blob = gcs_bucket.blob("ref/x_recent_since_id.json")
        if since_blob.exists():
            raw = since_blob.download_as_text()
            obj = json.loads(raw) if raw else {}
            since_id = obj.get("since_id")
    except Exception:
        since_id = None

    # Request parameters: up to 100 results, include created_at and author info
    # Prepare common params (query will be set per batch)
    common_params = {
        "max_results": str(tweet_max_results),
        "tweet.fields": "created_at,author_id",
        "expansions": "author_id",
        "user.fields": "username",
    }
    if since_id:
        common_params["since_id"] = since_id

    url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {"Authorization": f"Bearer {bearer_token}"}

    # Fetch all batches and aggregate
    id_to_username = {}
    all_tweets: list[dict] = []
    for q in query_strings:
        params = {"query": q, **common_params}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            resp.raise_for_status()
        except requests.HTTPError as e:
            print(f"ERROR: HTTP error: {e}", file=sys.stderr)
            try:
                print(resp.text, file=sys.stderr)
            except Exception:
                pass
            continue
        except Exception as e:
            print(f"ERROR: request failed: {e}", file=sys.stderr)
            continue

        payload = resp.json()

        includes = payload.get("includes", {})
        for u in includes.get("users", []) or []:
            uid = u.get("id")
            uname = u.get("username")
            if uid and uname:
                id_to_username[uid] = uname

        batch_tweets = payload.get("data", []) or []
        if batch_tweets:
            all_tweets.extend(batch_tweets)

    # Prepare upload path in GCS under raw/X_news/YYYY-MM-DD/ using Eastern Time
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
    date_dir = now_et.strftime("%Y-%m-%d")
    ts = now_et.strftime("%Y%m%dT%H%M%S%z")  # ET timestamp with offset
    gcs_blob_path = f"raw/X_news/{date_dir}/tweets_recent_{ts}.jsonl"

    # Write minimal fields for each tweet to JSONL (upload to GCS if non-empty)
    tweets = all_tweets
    try:
        if not tweets:
            print("No new tweets; skipping upload")
        else:
            lines: list[str] = []
            for t in tweets:
                author_id = t.get("author_id")
                # Convert tweet created_at (UTC) to ET for standardized output
                created_at_raw = t.get("created_at")
                created_at_et = None
                try:
                    if created_at_raw:
                        # Handle trailing 'Z'
                        created_dt_utc = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                        created_at_et = created_dt_utc.astimezone(ZoneInfo("America/New_York")).isoformat()
                except Exception:
                    created_at_et = None

                record = {
                    "id": t.get("id"),
                    "text": t.get("text"),
                    "author_username": id_to_username.get(author_id),
                    "created_at": created_at_raw,
                    "created_at_et": created_at_et,
                    "pulled_at_et": now_et.isoformat(),
                }
                lines.append(json.dumps(record, ensure_ascii=False))
            blob = gcs_bucket.blob(gcs_blob_path)
            blob.upload_from_string("\n".join(lines) + "\n", content_type="application/json")
            print(f"Uploaded {len(tweets)} tweets to gs://{GCS_BUCKET}/{gcs_blob_path}")
    except Exception as write_err:
        print(f"WARNING: failed to upload tweets to GCS: {write_err}", file=sys.stderr)

    # Update checkpoint with the highest tweet ID seen (write to GCS ref path)
    try:
        tweet_ids = [t.get("id") for t in tweets if t.get("id")]
        if tweet_ids:
            max_id = max(tweet_ids, key=lambda s: int(s))
            prev = int(since_id) if since_id and since_id.isdigit() else 0
            if int(max_id) > prev:
                since_blob = gcs_bucket.blob("ref/x_recent_since_id.json")
                since_blob.upload_from_string(json.dumps({"since_id": max_id}), content_type="application/json")
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


