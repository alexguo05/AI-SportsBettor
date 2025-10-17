import sys
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import dotenv_values
from google.cloud import storage
from google.oauth2 import service_account


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

    # HARD-CODED player prop markets (based on provider keys)
    markets_list = [
        "player_receptions",
        "player_reception_yds",
        "player_reception_longest",
        "player_rush_yds",
        "player_rush_attempts",
        "player_rush_longest",
        "player_rush_reception_yds",
        "player_pass_attempts",
        "player_pass_completions",
        "player_pass_yds",
        "player_pass_rush_yds",
        "player_pass_tds",
        "player_pass_interceptions",
        "player_pass_longest_completion",
        "player_anytime_td",
        "player_1st_td",
        "player_tackles_assists",
        "player_solo_tackles",
        "player_field_goals",
        "player_kicking_points",
    ]
    markets = ",".join(markets_list)

    dt_utc = datetime.now(timezone.utc)
    ts = dt_utc.strftime("%Y%m%dT%H%M%SZ")

    try:
        with httpx.Client(timeout=30) as client:
            # 1) Fetch upcoming events (no odds, no credit cost per provider docs)
            events_url = f"{API_BASE}/sports/{SPORT}/events"
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
                odds_url = f"{API_BASE}/sports/{SPORT}/events/{event_id}/odds"
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
            "sport": SPORT,
            "regions": ["us"],
            "markets": markets_list,
            "events_count": len(all_results),
            "results": all_results,
        }

        # Use explicit service account credentials for GCS
        creds = service_account.Credentials.from_service_account_file(str(GCS_SA_KEY_PATH))
        client = storage.Client(credentials=creds, project=creds.project_id)
        bucket = client.bucket(GCS_BUCKET)
        blob_name = f"player_props_events_{ts}.json"
        blob_path = f"raw/odds/{blob_name}"
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


