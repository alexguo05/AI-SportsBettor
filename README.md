# AI-SportsBettor

A pipeline for collecting timestamped NFL news and building auditable datasets
for sports-market research.

## 🎯 Project Goals

- Ingest NFL news from selected X accounts with proper timestamp handling
- Preserve raw source payloads in Google Cloud Storage
- Prepare trustworthy, time-aware inputs for future market analysis
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

### Running X News Ingestion

```bash
# Continuously fetch NFL news from configured X accounts
python -m src.ingest_news.X_pull
```

This will:
- Query the accounts configured in `src/config/x_config.json`
- Respect the X API query and polling limits
- Preserve source timestamps and ingestion timestamps
- Upload consolidated JSONL batches to Google Cloud Storage

## 📁 Project Structure

```
AI-SportsBettor/
├── src/
│   ├── ingest_news/
│   │   └── X_pull.py     # X news ingestion
│   ├── config/
│   │   └── x_config.json
│   └── common/           # Shared utilities & config
│       ├── settings.py   # Configuration management
│       └── logging_config.py
├── data/
│   ├── raw/
│   │   └── news/         # Optional local raw news data
│   └── ref/              # Reference data (teams, schedules)
├── infra/                # Docker Compose, migrations (TODO)
├── tests/                # Unit tests (TODO)
└── notebooks/            # Analysis notebooks (TODO)
```

## 🔑 Key Features (Current)

### X News Ingestion ✅

- **Configured Sources**: Collects posts from selected NFL accounts
- **Time Truth**: Preserves post and ingestion timestamps
- **Rate-Aware Polling**: Splits handles into bounded query batches
- **Audit Trail**: Stores consolidated raw JSONL batches in GCS
- **Restart Safety**: Persists the latest processed post ID

## 🚧 Roadmap

### Week 1 (MVP)
- [x] X news ingestion
- [ ] PostgreSQL database setup (Docker Compose)
- [ ] Database schema (Alembic migrations)
- [ ] APScheduler automation

### Week 2+
- [ ] Team/entity normalization
- [ ] Polymarket ingestion
- [ ] News-to-market linking
- [ ] Weather data integration
- [ ] Player props
- [ ] Closing line value (CLV) metrics
- [ ] Basic line movement models
- [ ] Alert generation system

## 🛡️ Non-Negotiables

- **ToS-Safe**: Use authorized APIs and respect provider terms
- **UTC Everywhere**: All timestamps in UTC
- **Audit Trail**: Preserve raw source payloads
- **Human-in-the-Loop**: No automated wagering
- **Time Truth**: Preserve source and ingestion timestamps

## 📊 Data Flow (Planned)

```
X API → Raw JSONL in GCS → news_events table
                               ↓
                         Entity Resolution
                               ↓
                  Future Market Integration
                               ↓
                    Time-Aligned Analysis
```

## 📝 Example Usage

### Start X Collection

```bash
python -m src.ingest_news.X_pull
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