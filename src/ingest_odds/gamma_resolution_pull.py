"""Executable entry point for Polymarket Gamma resolution reconciliation.

Run with: python -m src.ingest_odds.gamma_resolution_pull
"""

from src.ingest_odds.gamma_resolution_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
