"""Executable entry point for Polymarket Gamma ingestion.

Run with: python -m src.ingest_odds.polymarket_pull
"""

from src.ingest_odds.polymarket_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
