"""Seed durable jobs for rows that predate transactional enqueueing."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import exists, func, inspect, select

from src.db.engine import create_database_resources
from src.db.models import (
    news_enrichments,
    news_entity_resolution_runs,
    news_events,
    polymarket_events,
    polymarket_markets,
)
from src.enrich_news.config import load_enrichment_settings
from src.enrich_news.prompt import ENTITY_EXTRACTOR_VERSION
from src.entity_bank.prompt import EXTRACTOR_VERSION
from src.jobs.repository import (
    ENRICH_NEWS,
    JOB_CHANNEL,
    RESOLVE_MARKET,
    RESOLVE_NEWS,
    enqueue_job,
)

WRITE_CONFIRMATION = "SEED_JOB_QUEUE"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-live-writes")
    parser.add_argument(
        "--limit",
        type=int,
        help="cap each job type for a controlled smoke test",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.apply and args.confirm_live_writes != WRITE_CONFIRMATION:
        print(
            f"ERROR: --apply requires --confirm-live-writes {WRITE_CONFIRMATION}",
            file=sys.stderr,
        )
        return 2
    if args.limit is not None and args.limit < 1:
        print("ERROR: --limit must be at least 1", file=sys.stderr)
        return 2
    src_dir = Path(__file__).resolve().parents[1]
    settings = load_enrichment_settings(src_dir)
    resources = create_database_resources(src_dir)
    completed_enrichment = exists(
        select(news_enrichments.c.news_id).where(
            news_enrichments.c.news_id == news_events.c.news_id,
            news_enrichments.c.enrichment_version == settings.enrichment_version,
            news_enrichments.c.status.in_(["completed", "completed_with_warnings"]),
        )
    )
    enrichment_statement = (
        select(news_events.c.news_id)
        .where(~completed_enrichment)
        .order_by(news_events.c.published_at)
    )
    completed_resolution = exists(
        select(news_entity_resolution_runs.c.news_id).where(
            news_entity_resolution_runs.c.news_id == news_enrichments.c.news_id,
            news_entity_resolution_runs.c.enrichment_version
            == news_enrichments.c.enrichment_version,
            news_entity_resolution_runs.c.input_fingerprint
            == news_enrichments.c.input_fingerprint,
            news_entity_resolution_runs.c.extractor_version
            == ENTITY_EXTRACTOR_VERSION,
            news_entity_resolution_runs.c.status == "completed",
        )
    )
    resolution_statement = select(
        news_enrichments.c.news_id,
        news_enrichments.c.enrichment_version,
        news_enrichments.c.input_fingerprint,
        news_enrichments.c.entity_extractor_version,
    ).where(
        news_enrichments.c.enrichment_version == settings.enrichment_version,
        news_enrichments.c.entity_extractor_version == ENTITY_EXTRACTOR_VERSION,
        news_enrichments.c.status.in_(["completed", "completed_with_warnings"]),
        ~completed_resolution,
    )
    market_statement = (
        select(polymarket_events.c.event_id)
        .join(
            polymarket_markets,
            polymarket_markets.c.event_id == polymarket_events.c.event_id,
        )
        .where(
            polymarket_events.c.missing_since.is_(None),
            polymarket_markets.c.missing_since.is_(None),
        )
        .group_by(polymarket_events.c.event_id)
        .order_by(polymarket_events.c.event_id)
    )
    if args.limit is not None:
        enrichment_statement = enrichment_statement.limit(args.limit)
        resolution_statement = resolution_statement.limit(args.limit)
        market_statement = market_statement.limit(args.limit)
    try:
        with resources.engine.connect() as connection:
            inspector = inspect(connection)
            present_tables = set(inspector.get_table_names())
            required_tables = {
                "job_outbox",
                "news_entity_resolution_runs",
            }
            missing_tables = sorted(required_tables - present_tables)
            enrichment_columns = {
                column["name"]
                for column in inspector.get_columns("news_enrichments")
            }
            missing_columns = sorted(
                {"entity_extractor_version"} - enrichment_columns
            )
        if missing_tables or missing_columns:
            print(
                json.dumps(
                    {
                        "dry_run": not args.apply,
                        "ready": False,
                        "error": "database migrations required",
                        "required_revision": "20260803_12",
                        "missing_tables": missing_tables,
                        "missing_news_enrichment_columns": missing_columns,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 2
        with resources.engine.connect() as connection:
            news_ids = list(connection.scalars(enrichment_statement))
            resolution_rows = connection.execute(
                resolution_statement
            ).mappings().all()
            event_ids = list(connection.scalars(market_statement))
        summary = {
            "dry_run": not args.apply,
            "enrichment_jobs": len(news_ids),
            "news_resolution_jobs": len(resolution_rows),
            "market_resolution_jobs": len(event_ids),
            "enrichment_version": settings.enrichment_version,
            "news_extractor_version": ENTITY_EXTRACTOR_VERSION,
            "market_extractor_version": EXTRACTOR_VERSION,
            "limit_per_job_type": args.limit,
        }
        if args.apply:
            with resources.engine.begin() as connection:
                for news_id in news_ids:
                    enqueue_job(
                        connection,
                        job_type=ENRICH_NEWS,
                        idempotency_key=(
                            f"{news_id}:{settings.enrichment_version}"
                        ),
                        payload={
                            "news_id": news_id,
                            "enrichment_version": settings.enrichment_version,
                        },
                        priority=10,
                        notify=False,
                    )
                for row in resolution_rows:
                    enqueue_job(
                        connection,
                        job_type=RESOLVE_NEWS,
                        idempotency_key=(
                            f"{row['news_id']}:{row['enrichment_version']}:"
                            f"{row['input_fingerprint']}:"
                            f"{row['entity_extractor_version']}"
                        ),
                        payload={
                            "news_id": row["news_id"],
                            "enrichment_version": row["enrichment_version"],
                            "input_fingerprint": row["input_fingerprint"],
                            "extractor_version": row[
                                "entity_extractor_version"
                            ],
                        },
                        priority=8,
                        notify=False,
                    )
                for event_id in event_ids:
                    enqueue_job(
                        connection,
                        job_type=RESOLVE_MARKET,
                        idempotency_key=(
                            f"{event_id}:seed:{EXTRACTOR_VERSION}"
                        ),
                        payload={
                            "event_id": event_id,
                            "extractor_version": EXTRACTOR_VERSION,
                        },
                        priority=5,
                        notify=False,
                    )
                connection.execute(select(func.pg_notify(JOB_CHANNEL, "seeded")))
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    finally:
        resources.close()


if __name__ == "__main__":
    raise SystemExit(main())
