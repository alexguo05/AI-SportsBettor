"""Executable entry point for Kalshi order-book snapshot ingestion.

Run with: python -m src.ingest_odds.kalshi_order_book_pull
"""

from src.ingest_odds.kalshi_order_book_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
