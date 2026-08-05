"""Executable entry point for Kalshi structure and settlement ingestion.

Run with: python -m src.ingest_odds.kalshi_markets_pull
"""

from src.ingest_odds.kalshi_markets_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
