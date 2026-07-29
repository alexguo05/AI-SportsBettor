"""Executable entry point for X news ingestion.

Run with: python -m src.ingest_news.X_pull
"""

from src.ingest_news.x_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
