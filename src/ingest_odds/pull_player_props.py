import sys
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
import tempfile
import time
import os

import httpx
from dotenv import dotenv_values
from google.cloud import storage
from google.oauth2 import service_account
from zoneinfo import ZoneInfo


API_BASE = "https://api.the-odds-api.com/v4"


def implied_prob_from_decimal(decimal_odds: float) -> float:
    if not decimal_odds:
        return None
    return 1.0 / float(decimal_odds)


def get_current_interval(app_cfg: dict) -> int:
    """Determine poll interval based on current ET time and config."""
    peak_sec = int(app_cfg.get("poll_interval_peak_sec", 45))
    offpeak_sec = int(app_cfg.get("poll_interval_offpeak_sec", 300))
    start_hr = int(app_cfg.get("peak_start_hour_et", 6))
    end_hr = int(app_cfg.get("peak_end_hour_et", 24))  # 24 means midnight, i.e., [6, 24)

    now_et = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
    # Check if current hour is within [start, end)
    if start_hr <= now_et.hour < end_hr:
        return peak_sec
    return offpeak_sec


def main() -> int:
    # 1. Load configuration from odds_config.json
    config_path = Path(__file__).resolve().parents[1] / "config" / "odds_config.json"
    try:
        with config_path.open("r", encoding="utf-8") as cf:
            app_cfg = json.load(cf)
    except Exception as e:
        print(f"ERROR: failed to load odds_config.json: {e}", file=sys.stderr)
        return 1

    gcs_bucket_name = app_cfg.get("gcs_bucket", "ai-sports-bettor")
    sport = app_cfg.get("sport", "americanfootball_nfl")
    markets_list = app_cfg.get("markets_list", [])

    if not markets_list:
        print("ERROR: markets_list missing in odds_config.json", file=sys.stderr)
        return 1
    
    markets = ",".join(markets_list)

    # 2. Load secrets from src/.env (ODDS_API_KEY)
    # Prefer environment variables; fall back to src/.env
    api_key = os.getenv("ODDS_API_KEY")
    src_dir = Path(__file__).resolve().parents[1]
    
    if not api_key:
        dotenv_path = src_dir / ".env"
        env_vars = dotenv_values(dotenv_path) if dotenv_path.exists() else {}
        api_key = env_vars.get("ODDS_API_KEY")
    
    if not api_key:
        print("ERROR: ODDS_API_KEY not set in environment or src/.env", file=sys.stderr)
        return 1

    # 3. GCS setup using Service Account from src/
    # Local dev convenience: default to repo SA JSON if GOOGLE_APPLICATION_CREDENTIALS not set
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        default_sa_path = src_dir / "ai-sports-bettor-559e8837739f.json"
        if default_sa_path.exists():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(default_sa_path)

    try:
        # Prefer loading explicit credentials if the file exists in src/
        sa_path = src_dir / "ai-sports-bettor-559e8837739f.json"
        if sa_path.exists():
            creds = service_account.Credentials.from_service_account_file(str(sa_path))
            client = storage.Client(credentials=creds, project=creds.project_id)
        else:
            # Fallback to ADC
            client = storage.Client()
    except Exception as e:
        print(f"ERROR: Failed to initialize GCS client: {e}", file=sys.stderr)
        return 1

    bucket = client.bucket(gcs_bucket_name)

    print(f"Starting odds poller. Sport: {sport}. Bucket: {gcs_bucket_name}")

    while True:
        # Determine sleep time for THIS cycle
        interval = get_current_interval(app_cfg)
        
        dt_utc = datetime.now(timezone.utc)
        dt_et = dt_utc.astimezone(ZoneInfo("America/New_York"))
        ts = dt_et.strftime("%Y%m%dT%H%M%S%z")  # ET timestamp with offset

        try:
            with httpx.Client(timeout=30) as http_client:
                # 1) Fetch upcoming events (no odds, no credit cost per provider docs)
                events_url = f"{API_BASE}/sports/{sport}/events"
                ev_resp = http_client.get(events_url, params={"apiKey": api_key, "dateFormat": "iso"})
                ev_resp.raise_for_status()
                events = ev_resp.json()
                if not isinstance(events, list):
                    print("ERROR: Unexpected events payload", file=sys.stderr)
                    # If fetch fails, sleep and retry
                    time.sleep(interval)
                    continue

                # Optional: cap number of events via env (ODDS_MAX_EVENTS)
                max_events_str = (os.getenv("ODDS_MAX_EVENTS") or "").strip()
                if max_events_str.isdigit():
                    events = events[: int(max_events_str)]

                # print(f"Fetching {len(events)} events...")

                all_results = []
                last_remaining = None
                last_used = None
                last_limit = None

                # 2) For each event, fetch specified markets of odds
                for ev in events:
                    event_id = ev.get("id")
                    if not event_id:
                        continue
                    odds_url = f"{API_BASE}/sports/{sport}/events/{event_id}/odds"
                    params = {
                        "apiKey": api_key,
                        "regions": "us",
                        "markets": markets,
                        "dateFormat": "iso",
                        "oddsFormat": "decimal",
                    }
                    try:
                        resp = http_client.get(odds_url, params=params)
                        resp.raise_for_status()
                    except httpx.HTTPStatusError as e:
                        print(f"ERROR: event {event_id} request failed: {e}", file=sys.stderr)
                        if e.response is not None:
                            try:
                                print(e.response.text, file=sys.stderr)
                            except Exception:
                                pass
                        continue

                    data = resp.json()
                    # Capture credit headers (will reflect cumulative usage)
                    hdrs = resp.headers
                    last_remaining = hdrs.get("x-requests-remaining") or hdrs.get("X-Requests-Remaining")
                    last_used = hdrs.get("x-requests-used") or hdrs.get("X-Requests-Used")
                    last_limit = hdrs.get("x-requests-limit") or hdrs.get("X-Requests-Limit")

                    all_results.append(
                        {
                            "event": {
                                "id": ev.get("id"),
                                "commence_time": ev.get("commence_time"),
                                "home_team": ev.get("home_team"),
                                "away_team": ev.get("away_team"),
                            },
                            "bookmakers": data[0].get("bookmakers", []) if isinstance(data, list) and data else data.get("bookmakers", []),
                        }
                    )

        except Exception as e:
            print(f"ERROR: request failed: {e}", file=sys.stderr)
            time.sleep(interval)
            continue

        # Build payload and upload directly to GCS (no local write)
        try:
            payload = {
                "fetched_at": dt_utc.isoformat(),
                "fetched_at_et": dt_et.isoformat(),
                "sport": sport,
                "regions": ["us"],
                "markets": markets_list,
                "events_count": len(all_results),
                "results": all_results,
            }

            date_dir = dt_et.strftime("%Y-%m-%d")
            blob_name = f"player_props_events_{ts}.json"
            blob_path = f"raw/odds/{date_dir}/{blob_name}"
            blob = bucket.blob(blob_path)
            blob.upload_from_string(
                data=json.dumps(payload, ensure_ascii=False, indent=2),
                content_type="application/json",
            )
            print(f"Uploaded to gs://{gcs_bucket_name}/{blob_path} ({len(all_results)} events)")
        except Exception as gcs_err:
            print(f"WARNING: failed to upload to GCS: {gcs_err}", file=sys.stderr)

        # Print last known credit usage info if present
        if last_remaining is not None:
            remaining_str = last_remaining
            used_str = last_used if last_used is not None else "unknown"
            limit_suffix = f", limit: {last_limit}" if last_limit is not None else ""
            print(f"API credits — remaining: {remaining_str}, used: {used_str}{limit_suffix}")

        time.sleep(interval)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
