import os
import sys
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv


API_BASE = "https://api.the-odds-api.com/v4"
SPORT = "americanfootball_nfl"


def implied_prob_from_american(american_odds: int) -> float:
    if american_odds is None:
        return None
    if american_odds >= 0:
        return 100.0 / (american_odds + 100.0)
    return (-american_odds) / ((-american_odds) + 100.0)


def implied_prob_from_decimal(decimal_odds: float) -> float:
    if not decimal_odds:
        return None
    return 1.0 / float(decimal_odds)


def main() -> int:
    # Load .env from the same directory as this file (if present)
    dotenv_path = Path(__file__).parent / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path)

    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        print("ERROR: ODDS_API_KEY not set in environment", file=sys.stderr)
        return 1

    params = {
        "apiKey": api_key,
        "regions": "us",
        "bookmakers": "draftkings",
        "markets": "totals",
        "oddsFormat": "decimal",  # request decimal odds
        "dateFormat": "iso",
        "daysFrom": "2",  # limit to near-term games
    }

    url = f"{API_BASE}/sports/{SPORT}/odds"
    try:
        with httpx.Client(timeout=20) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            # Capture header-based credit info from The Odds API
            hdrs = resp.headers
            remaining_hdr = hdrs.get("x-requests-remaining") or hdrs.get("X-Requests-Remaining")
            used_hdr = hdrs.get("x-requests-used") or hdrs.get("X-Requests-Used")
            limit_hdr = hdrs.get("x-requests-limit") or hdrs.get("X-Requests-Limit")
    except Exception as e:
        print(f"ERROR: request failed: {e}", file=sys.stderr)
        return 2

    # Persist raw odds JSON to project data directory: data/raw/odds
    dt_utc = datetime.now(timezone.utc)
    ts = dt_utc.strftime("%Y%m%dT%H%M%SZ")
    try:
        project_root = Path(__file__).resolve().parents[2]
        odds_dir = project_root / "data" / "raw" / "odds"
        odds_dir.mkdir(parents=True, exist_ok=True)
        out_path_data = odds_dir / f"odds_pull_{ts}.json"
        with out_path_data.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        saved_data_msg = f"Saved raw response to data/raw/odds/{out_path_data.name}"
    except Exception as write_err:
        saved_data_msg = f"WARNING: failed to write JSON to data/raw/odds: {write_err}"

    now = dt_utc.isoformat()
    print(f"Fetched {len(data)} events @ {now} UTC (bookmaker=draftkings, market=totals)")
    print(saved_data_msg)
    # Print credit usage information if present
    remaining_str = remaining_hdr if remaining_hdr is not None else "unknown"
    used_str = used_hdr if used_hdr is not None else "unknown"
    limit_suffix = f", limit: {limit_hdr}" if limit_hdr is not None else ""
    print(f"API credits — remaining: {remaining_str}, used: {used_str}{limit_suffix}")

    # Print a compact summary per event
    for ev in data[:10]:  # preview first 10
        event_id = ev.get("id")
        commence = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        bkms = ev.get("bookmakers", [])
        # Find DraftKings totals market
        line_summaries = []
        for b in bkms:
            if b.get("key") != "draftkings":
                continue
            for mk in b.get("markets", []):
                if mk.get("key") != "totals":
                    continue
                for out in mk.get("outcomes", []):
                    name = out.get("name")  # Over/Under
                    point = out.get("point")
                    price = out.get("price")
                    ip = implied_prob_from_decimal(price)
                    line_summaries.append(f"{name} {point} @ {price} (p={ip:.3f})")
        summary = "; ".join(line_summaries) if line_summaries else "no totals"
        print(f"- {away} @ {home} | {commence} | {event_id} | {summary}")

    # Optionally dump raw JSON (uncomment if desired)
    # print(json.dumps(data, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


