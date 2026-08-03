"""Read-only 10-news/10-market live smoke harness."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

from src.db.engine import create_database_resources
from src.enrich_news.config import load_enrichment_settings
from src.enrich_news.pipeline import enrich_record
from src.enrich_news.provider import ClaudeProvider
from src.enrich_news.repository import EnrichmentRepository
from src.entity_bank.audit import _candidate_rows
from src.entity_bank.nflverse_pipeline import NflverseClient, fetch_snapshot
from src.entity_bank.provider import ClaudeEntityProvider
from src.entity_bank.resolver import CandidateIndex
from src.entity_bank.worker import Batch, process_market_events, process_news

NEWS_IDS = [
    "x:2082082258081484868",
    "x:1039616414154280960",
    "x:2082176837304905800",
    "x:2082190839766249833",
    "x:2082125282681467322",
    "x:2084348926782525877",
    "x:2084348459415437795",
    "x:2084326593304969567",
    "x:2084343973070483770",
    "x:2084331498291794210",
]
MARKET_IDS = [
    "949831",
    "949835",
    "949851",
    "949853",
    "949866",
    "949871",
    "949881",
    "941759",
    "904739",
    "921962",
]


def _load_markets(resources: Any) -> list[dict[str, Any]]:
    query = text(
        """
        SELECT e.event_id, e.title event_title, e.slug event_slug,
               m.market_id, m.question, m.slug market_slug,
               m.sports_market_type, m.current_content_sha256,
               t.outcome, t.outcome_index
        FROM polymarket_markets m
        JOIN polymarket_events e ON e.event_id = m.event_id
        LEFT JOIN polymarket_tokens t ON t.market_id = m.market_id
        WHERE m.market_id = ANY(:ids)
        ORDER BY e.event_id, m.market_id, t.outcome_index
        """
    )
    with resources.engine.connect() as connection:
        rows = connection.execute(query, {"ids": MARKET_IDS}).mappings().all()
    events: dict[str, dict[str, Any]] = {}
    for row in rows:
        event = events.setdefault(
            row["event_id"],
            {
                "event_id": row["event_id"],
                "title": row["event_title"],
                "slug": row["event_slug"],
                "game_id": None,
                "markets": {},
            },
        )
        market = event["markets"].setdefault(
            row["market_id"],
            {
                "market_id": row["market_id"],
                "question": row["question"],
                "slug": row["market_slug"],
                "group_item_title": None,
                "group_item_threshold": None,
                "sports_market_type": row["sports_market_type"],
                "source_content_sha256": row["current_content_sha256"],
                "prior_entity_input_sha256": None,
                "prior_extractor_version": None,
                "outcomes": [],
            },
        )
        if row["outcome"] is not None:
            market["outcomes"].append(row["outcome"])
    return [
        {**event, "markets": list(event["markets"].values())}
        for event in events.values()
    ]


def main() -> None:
    settings = load_enrichment_settings(Path("src"))
    resources = create_database_resources(Path("src"))
    try:
        repository = EnrichmentRepository(resources)
        records = [repository.load_record(news_id) for news_id in NEWS_IDS]
        if any(record is None for record in records):
            raise RuntimeError("one or more selected news records are missing")
        records = [record for record in records if record is not None]
        enrichment_provider = ClaudeProvider(
            settings.api_key or "",
            model_name=settings.model_name,
            max_tokens=settings.max_output_tokens,
        )
        results = [
            enrich_record(
                record,
                enrichment_provider,
                enrichment_version=settings.enrichment_version,
                allow_network=True,
            )
            for record in records
        ]

        candidates = _candidate_rows(fetch_snapshot(NflverseClient(), season=2026))
        names = {row["entity_id"]: row["canonical_name"] for row in candidates}
        index = CandidateIndex(candidates)
        resolver = ClaudeEntityProvider(
            settings.api_key or "",
            model_name=settings.model_name,
            max_tokens=settings.max_output_tokens,
        )
        news_batch = Batch()
        process_news(
            records=[
                {
                    "news_id": result.news_id,
                    "text": record.text,
                    "input_fingerprint": result.input_fingerprint,
                    "enrichment_version": result.enrichment_version,
                    "entities": (
                        [entity.model_dump(mode="json") for entity in result.output.entities]
                        if result.output
                        else []
                    ),
                    "summary": result.output.summary if result.output else None,
                }
                for record, result in zip(records, results, strict=True)
            ],
            provider=resolver,
            index=index,
            bank_version_id=None,
            batch=news_batch,
            observed_at=datetime.now(UTC),
        )
        market_batch = Batch()
        process_market_events(
            events=_load_markets(resources),
            provider=resolver,
            index=index,
            bank_version_id=None,
            batch=market_batch,
            observed_at=datetime.now(UTC),
        )
        report = {
            "news": [
                {
                    "news_id": result.news_id,
                    "status": result.status,
                    "error": result.error,
                    "summary": result.output.summary if result.output else None,
                    "entities": (
                        [entity.name for entity in result.output.entities]
                        if result.output
                        else []
                    ),
                    "claims": (
                        [claim.statement for claim in result.output.claims]
                        if result.output
                        else []
                    ),
                    "warnings": result.warnings,
                }
                for result in results
            ],
            "news_mentions": [
                {
                    "news_id": row["news_id"],
                    "text": row["mention_text"],
                    "status": row["resolution_status"],
                    "canonical": names.get(row["entity_id"]),
                }
                for row in news_batch.mentions.values()
            ],
            "markets": {
                market_id: {
                    "topic": row["market_topic"],
                    "contract": row["contract_type"],
                }
                for market_id, row in market_batch.classifications.items()
            },
            "market_mentions": [
                {
                    "market_id": row["polymarket_market_id"],
                    "text": row["mention_text"],
                    "status": row["resolution_status"],
                    "canonical": names.get(row["entity_id"]),
                }
                for row in market_batch.mentions.values()
            ],
            "failures": [*news_batch.failures, *market_batch.failures],
            "usage": {
                "input": sum(result.usage.input_tokens for result in results)
                + news_batch.input_tokens
                + market_batch.input_tokens,
                "output": sum(result.usage.output_tokens for result in results)
                + news_batch.output_tokens
                + market_batch.output_tokens,
            },
            "writes": {"database": False, "gcs": False, "local_output": False},
        }
        print(json.dumps(report, indent=2, sort_keys=True))
    finally:
        resources.close()


if __name__ == "__main__":
    main()
