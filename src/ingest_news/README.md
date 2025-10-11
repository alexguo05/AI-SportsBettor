# RSS News Ingestion

This module handles ingestion of NFL news from RSS feeds.

## Features

- **Multiple NFL Sources**: Pulls from 5 major NFL news sources:
  - ESPN NFL
  - CBS Sports NFL
  - Pro Football Talk (NBC Sports)
  - NFL.com Official News
  - Sports Illustrated NFL

- **Time Truth**: Captures and normalizes timestamps to UTC:
  - `t_published_utc`: When the article was originally published
  - `t_updated_utc`: When the article was last updated
  - `t_first_seen_utc`: When our system first saw the article
  - `t_news_utc`: Canonical news time (published → updated → first_seen)

- **Content Deduplication**: Uses SHA256 hash of `source|title|body` to detect duplicates

- **Audit Trail**: Saves raw JSONL files organized by date in `/data/raw/news/YYYY-MM-DD/`

## Usage

### Run the RSS Pull Script

```bash
# From project root
python -m src.ingest_news.rss_pull
```

### Output

The script will:
1. Fetch all configured RSS feeds
2. Parse and normalize entries
3. Save raw data to JSONL files: `/data/raw/news/YYYY-MM-DD/rss_pull_YYYYMMDD_HHMMSS.jsonl`
4. Print summary statistics by source

### Example Output

```
2025-10-11 10:30:15 | INFO     | Starting RSS news ingestion
2025-10-11 10:30:15 | INFO     | Fetching feed: ESPN_NFL from https://www.espn.com/espn/rss/nfl/news
2025-10-11 10:30:16 | INFO     | Found 25 entries in ESPN_NFL
...
2025-10-11 10:30:20 | INFO     | Total entries collected: 120
2025-10-11 10:30:20 | INFO     | Summary by source:
2025-10-11 10:30:20 | INFO     |   CBS_NFL: 20 entries
2025-10-11 10:30:20 | INFO     |   ESPN_NFL: 25 entries
2025-10-11 10:30:20 | INFO     |   NFL_News: 30 entries
2025-10-11 10:30:20 | INFO     |   ProFootballTalk: 28 entries
2025-10-11 10:30:20 | INFO     |   SI_NFL: 17 entries
```

## Data Schema

Each entry in the JSONL output has the following structure:

```json
{
  "source": "ESPN_NFL",
  "guid": "unique-article-id",
  "url": "https://...",
  "headline": "Breaking: Team signs player",
  "body": "Cleaned article text without HTML...",
  "t_published_utc": "2025-10-11T14:30:00+00:00",
  "t_updated_utc": null,
  "t_first_seen_utc": "2025-10-11T14:35:22+00:00",
  "t_news_utc": "2025-10-11T14:30:00+00:00",
  "t_source": "published",
  "content_hash": "sha256_hash_here",
  "raw_entry": {
    "title": "...",
    "link": "...",
    "published": "...",
    "updated": "...",
    "summary": "...",
    "id": "..."
  }
}
```

## Adding New RSS Feeds

Edit `NFL_RSS_FEEDS` in `rss_pull.py`:

```python
NFL_RSS_FEEDS = [
    {
        "name": "YourSource",
        "url": "https://example.com/rss/nfl",
    },
    # ... more feeds
]
```

## Dependencies

- `feedparser`: RSS/Atom feed parsing
- `httpx`: HTTP client for fetching feeds
- `beautifulsoup4`: HTML content cleaning
- `python-dateutil`: Flexible timestamp parsing

## Next Steps

Once you have news data collected, you can:
1. Load it into the `news_events` database table (requires DB setup)
2. Link news to events using the `features.link_pre_snapshot` module
3. Correlate with odds data to find edges

