import sys
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx


def main() -> int:
    if len(sys.argv) < 2:
        print(
            "Usage: python fetch_url_to_file.py <FULL_URL_WITH_QUERY>",
            file=sys.stderr,
        )
        return 1

    url = sys.argv[1]

    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()
            hdrs = resp.headers
            remaining_hdr = hdrs.get("x-requests-remaining") or hdrs.get("X-Requests-Remaining")
            used_hdr = hdrs.get("x-requests-used") or hdrs.get("X-Requests-Used")
            limit_hdr = hdrs.get("x-requests-limit") or hdrs.get("X-Requests-Limit")
    except httpx.HTTPStatusError as e:
        print(f"ERROR: request failed: {e}", file=sys.stderr)
        if e.response is not None:
            try:
                print(e.response.text, file=sys.stderr)
            except Exception:
                pass
        return 2
    except Exception as e:
        print(f"ERROR: request failed: {e}", file=sys.stderr)
        return 2

    dt_utc = datetime.now(timezone.utc)
    ts = dt_utc.strftime("%Y%m%dT%H%M%SZ")

    # Save under project data directory
    project_root = Path(__file__).resolve().parents[2]
    out_dir = project_root / "data" / "raw" / "odds"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"event_markets_{ts}.json"

    try:
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved JSON to data/raw/odds/{out_path.name}")
    except Exception as write_err:
        print(f"WARNING: failed to write JSON to disk: {write_err}", file=sys.stderr)

    # Print credit usage if present
    remaining_str = remaining_hdr if remaining_hdr is not None else "unknown"
    used_str = used_hdr if used_hdr is not None else "unknown"
    limit_suffix = f", limit: {limit_hdr}" if limit_hdr is not None else ""
    print(f"API credits — remaining: {remaining_str}, used: {used_str}{limit_suffix}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


