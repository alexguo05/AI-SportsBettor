"""Executable entry point for Polymarket trade-print ingestion.

Run with: python -m src.ingest_odds.trades_pull
"""

from src.ingest_odds.trades_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
