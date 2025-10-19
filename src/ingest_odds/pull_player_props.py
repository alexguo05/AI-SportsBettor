import sys
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import dotenv_values
from google.cloud import storage
from google.oauth2 import service_account
from zoneinfo import ZoneInfo
import os


API_BASE = "https://api.the-odds-api.com/v4"
SPORT = "americanfootball_nfl"
GCS_BUCKET = "ai-sports-bettor"
# Service account key path relative to this script (../ai-sports-bettor-*.json under src/)
GCS_SA_KEY_PATH = (Path(__file__).resolve().parents[1] / "ai-sports-bettor-559e8837739f.json")


def implied_prob_from_decimal(decimal_odds: float) -> float:
    if not decimal_odds:
        return None
    return 1.0 / float(decimal_odds)


def main() -> int:
    # Load key/value pairs directly from .env in this directory (no process env usage)
    dotenv_path = Path(__file__).parent / "../.env"
    config = dotenv_values(dotenv_path) if dotenv_path.exists() else {}

    api_key = config.get("ODDS_API_KEY")
    if not api_key:
        print("ERROR: ODDS_API_KEY not set in environment", file=sys.stderr)
        return 1

    # Load sport and markets_list from config file
    config_path = Path(__file__).resolve().parents[1] / "config" / "config.json"
    try:
        with config_path.open("r", encoding="utf-8") as cf:
            app_cfg = json.load(cf)
        sport = app_cfg.get("sport", "americanfootball_nfl")
        markets_list = app_cfg.get("markets_list", [])
        if not markets_list:
            print("ERROR: markets_list missing in config", file=sys.stderr)
            return 1
    except Exception as e:
        print(f"ERROR: failed to load config: {e}", file=sys.stderr)
        return 1
    markets = ",".join(markets_list)

    dt_utc = datetime.now(timezone.utc)
    dt_et = dt_utc.astimezone(ZoneInfo("America/New_York"))
    ts = dt_et.strftime("%Y%m%dT%H%M%S%z")  # ET timestamp with offset

    try:
        with httpx.Client(timeout=30) as client:
            # 1) Fetch upcoming events (no odds, no credit cost per provider docs)
            events_url = f"{API_BASE}/sports/{sport}/events"
            ev_resp = client.get(events_url, params={"apiKey": api_key, "dateFormat": "iso"})
            ev_resp.raise_for_status()
            events = ev_resp.json()
            if not isinstance(events, list):
                print("ERROR: Unexpected events payload", file=sys.stderr)
                return 2

            # Optional: cap number of events via .env (ODDS_MAX_EVENTS)
            max_events_str = (config.get("ODDS_MAX_EVENTS") or "").strip()
            if max_events_str.isdigit():
                events = events[: int(max_events_str)]

            num_events = len(events)
            est_cost = num_events * len(markets_list)  # per docs: cost = markets * regions (1)
            print(f"Preparing to fetch {num_events} events, markets={len(markets_list)} (est. cost={est_cost})")

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
                    resp = client.get(odds_url, params=params)
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
        return 2

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

        # Use explicit service account credentials for GCS
        creds = service_account.Credentials.from_service_account_file(str(GCS_SA_KEY_PATH))
        client = storage.Client(credentials=creds, project=creds.project_id)
        bucket = client.bucket(GCS_BUCKET)
        date_dir = dt_et.strftime("%Y-%m-%d")
        blob_name = f"player_props_events_{ts}.json"
        blob_path = f"raw/odds/{date_dir}/{blob_name}"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(
            data=json.dumps(payload, ensure_ascii=False, indent=2),
            content_type="application/json",
        )
        print(f"Uploaded to gs://{GCS_BUCKET}/{blob_path} ({len(all_results)} events)")
    except Exception as gcs_err:
        print(f"WARNING: failed to upload to GCS: {gcs_err}", file=sys.stderr)

    # Print last known credit usage info if present
    remaining_str = last_remaining if last_remaining is not None else "unknown"
    used_str = last_used if last_used is not None else "unknown"
    limit_suffix = f", limit: {last_limit}" if last_limit is not None else ""
    print(f"API credits — remaining: {remaining_str}, used: {used_str}{limit_suffix}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


