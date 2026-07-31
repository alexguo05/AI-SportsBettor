"""Executable entry point for Polymarket CLOB price ingestion.

Run with: python -m src.ingest_odds.clob_price_pull
"""

from src.ingest_odds.clob_price_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
