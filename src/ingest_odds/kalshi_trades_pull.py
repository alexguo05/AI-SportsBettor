"""Executable entry point for Kalshi trade-print ingestion.

Run with: python -m src.ingest_odds.kalshi_trades_pull
"""

from src.ingest_odds.kalshi_trades_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
