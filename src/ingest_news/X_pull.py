import sys
import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import dotenv_values


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

    # Build the query: from: handles combined with OR, exclude RTs and replies
    base_handles = ["NFLCharean", "AdamSchefter", "LauraRutledge"]
    from_clauses = [f"from:{h}" for h in base_handles]
    query = " OR ".join(from_clauses) + " -is:retweet -is:reply"

    # Load checkpoint: since_id to fetch only tweets newer than last seen
    project_root = Path(__file__).resolve().parents[2]
    ref_dir = project_root / "data" / "ref"
    ref_dir.mkdir(parents=True, exist_ok=True)
    since_path = ref_dir / "x_recent_since_id.json"
    since_id: str | None = None
    try:
        if since_path.exists():
            with since_path.open("r", encoding="utf-8") as f:
                obj = json.load(f)
                since_id = obj.get("since_id")
    except Exception:
        since_id = None

    # Request parameters: up to 100 results, include created_at and author info
    params = {
        "query": query,
        "max_results": "100",
        "tweet.fields": "created_at,author_id",
        "expansions": "author_id",
        "user.fields": "username",
    }
    if since_id:
        params["since_id"] = since_id

    url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {"Authorization": f"Bearer {bearer_token}"}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=20)
        resp.raise_for_status()
    except requests.HTTPError as e:
        # Print HTTP error details and body for debugging
        print(f"ERROR: HTTP error: {e}", file=sys.stderr)
        try:
            print(resp.text, file=sys.stderr)
        except Exception:
            pass
        return 2
    except Exception as e:
        print(f"ERROR: request failed: {e}", file=sys.stderr)
        return 2

    payload = resp.json()

    # Build a mapping from author_id to username from includes
    id_to_username = {}
    includes = payload.get("includes", {})
    for u in includes.get("users", []) or []:
        uid = u.get("id")
        uname = u.get("username")
        if uid and uname:
            id_to_username[uid] = uname

    # Prepare output directory under data/raw/news/YYYY-MM-DD/
    dt_utc = datetime.now(timezone.utc)
    date_dir = dt_utc.strftime("%Y-%m-%d")
    ts = dt_utc.strftime("%Y%m%dT%H%M%SZ")
    out_dir = project_root / "data" / "raw" / "news" / date_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"tweets_recent_{ts}.jsonl"

    # Write minimal fields for each tweet to JSONL
    tweets = payload.get("data", []) or []
    try:
        with out_path.open("w", encoding="utf-8") as f:
            for t in tweets:
                author_id = t.get("author_id")
                record = {
                    "id": t.get("id"),
                    "text": t.get("text"),
                    "author_username": id_to_username.get(author_id),
                    "created_at": t.get("created_at"),
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        print(f"Saved {len(tweets)} tweets to {out_path}")
    except Exception as write_err:
        print(f"WARNING: failed to write output: {write_err}", file=sys.stderr)

    # Update checkpoint with the highest tweet ID seen
    try:
        tweet_ids = [t.get("id") for t in tweets if t.get("id")]
        if tweet_ids:
            max_id = max(tweet_ids, key=lambda s: int(s))
            prev = int(since_id) if since_id and since_id.isdigit() else 0
            if int(max_id) > prev:
                with since_path.open("w", encoding="utf-8") as f:
                    json.dump({"since_id": max_id}, f)
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


