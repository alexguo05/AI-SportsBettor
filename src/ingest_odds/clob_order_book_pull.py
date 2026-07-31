"""Executable entry point for Polymarket CLOB order-book ingestion.

Run with: python -m src.ingest_odds.clob_order_book_pull
"""

from src.ingest_odds.clob_order_book_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
