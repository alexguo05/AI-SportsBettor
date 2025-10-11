# AI-SportsBettor

A disciplined, ToS-safe pipeline that ingests NFL news via RSS, captures licensed sportsbook odds snapshots, and produces latency-aware datasets for modeling line moves and generating manual betting alerts (human-in-the-loop only).

## 🎯 Project Goals

- Ingest NFL news from RSS feeds with proper timestamp handling
- Capture licensed odds snapshots (no scraping)
- Time-align news to pre-news baseline prices
- Generate manual betting alerts (no automated wagering)
- Maintain full audit trail with raw data preservation

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- pip or uv package manager
- (Optional) PostgreSQL 16 for database storage

### Installation

```bash
# Clone the repository
cd AI-SportsBettor

# Install dependencies
pip install -e .

# (Optional) Copy and configure environment
cp .env.example .env
# Edit .env with your configuration
```

### Running RSS News Ingestion

```bash
# Fetch NFL news from RSS feeds
python -m src.ingest_news.rss_pull
```

This will:
- Pull from 5 major NFL news sources (ESPN, CBS, PFT, NFL.com, SI)
- Normalize all timestamps to UTC
- Clean HTML content from articles
- Save raw JSONL data to `data/raw/news/YYYY-MM-DD/`
- Print summary statistics

## 📁 Project Structure

```
AI-SportsBettor/
├── src/
│   ├── ingest_news/      # RSS news ingestion
│   │   ├── rss_pull.py   # Main RSS fetching script
│   │   └── README.md     # News ingestion docs
│   ├── ingest_odds/      # Odds API client (TODO)
│   ├── features/         # Entity resolution & linking (TODO)
│   └── common/           # Shared utilities & config
│       ├── settings.py   # Configuration management
│       └── logging_config.py
├── data/
│   ├── raw/
│   │   ├── news/         # Raw news JSONL files
│   │   └── odds/         # Raw odds JSONL files
│   └── ref/              # Reference data (teams, schedules)
├── infra/                # Docker Compose, migrations (TODO)
├── tests/                # Unit tests (TODO)
└── notebooks/            # Analysis notebooks (TODO)
```

## 🔑 Key Features (Current)

### RSS News Ingestion ✅

- **Multiple Sources**: ESPN, CBS Sports, Pro Football Talk, NFL.com, Sports Illustrated
- **Time Truth**: Captures `published`, `updated`, and `first_seen` timestamps
- **UTC Everywhere**: All timestamps normalized to UTC
- **Content Deduplication**: SHA256 hash-based deduplication
- **Audit Trail**: Raw JSONL files with complete provenance
- **HTML Cleaning**: BeautifulSoup removes tags and normalizes whitespace

## 🚧 Roadmap

### Week 1 (MVP)
- [x] RSS news ingestion
- [ ] PostgreSQL database setup (Docker Compose)
- [ ] Database schema (Alembic migrations)
- [ ] Odds API integration (The Odds API)
- [ ] News-to-event linking
- [ ] APScheduler automation

### Week 2+
- [ ] Team/entity normalization
- [ ] Weather data integration
- [ ] Player props
- [ ] Closing line value (CLV) metrics
- [ ] Basic line movement models
- [ ] Alert generation system

## 🛡️ Non-Negotiables

- **ToS-Safe**: Only licensed APIs, no sportsbook scraping
- **UTC Everywhere**: All timestamps in UTC
- **Audit Trail**: Raw payloads stored on disk
- **Human-in-the-Loop**: No automated wagering
- **Time Truth**: Proper event-time handling with pre-news baselines

## 📊 Data Flow (Planned)

```
RSS Feeds → Raw JSONL → news_events table
                              ↓
                         Entity Resolution
                              ↓
Odds API → Raw JSONL → odds_snapshots table
                              ↓
                    Time-Aligned Linking
                              ↓
                     news_odds_links table
                              ↓
                      Feature Engineering
                              ↓
                          Modeling
                              ↓
                     Manual Alerts 🚨
```

## 📝 Example Usage

### Fetch Latest NFL News

```python
from src.ingest_news.rss_pull import pull_all_feeds, save_to_jsonl
from src.common.settings import settings

# Pull all configured feeds
entries = pull_all_feeds()

# Save to disk
save_to_jsonl(entries, settings.data_raw_news_dir)

print(f"Collected {len(entries)} news entries")
```

## 🤝 Contributing

This is a personal/solo MVP project. Focus areas:
1. Keep it simple and maintainable
2. Typed Python with Pydantic models
3. Small, testable modules
4. UTC timestamps everywhere
5. Preserve raw data for audit

## 📄 License

Private project for educational/research purposes only.

## ⚠️ Disclaimer

This tool is for **educational and research purposes only**. It does not place bets automatically and is not financial advice. Sports betting carries risk. Never bet more than you can afford to lose.