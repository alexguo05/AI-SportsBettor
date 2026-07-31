"""PostgreSQL persistence for versioned enrichment results."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import and_, exists, inspect, select
from sqlalchemy.dialects.postgresql import insert

from src.db.engine import DatabaseResources
from src.db.models import (
    news_enrichment_tags,
    news_enrichments,
    news_events,
    news_media,
)
from src.enrich_news.models import EnrichmentResult, NewsRecord


def enrichment_values(
    result: EnrichmentResult,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    output = result.output
    return {
        "news_id": result.news_id,
        "enrichment_version": result.enrichment_version,
        "provider": result.provider,
        "model_name": result.model_name,
        "status": result.status,
        "input_fingerprint": result.input_fingerprint,
        "input_manifest": result.input_manifest,
        "information_status": output.information_status.value if output else None,
        "usefulness": output.usefulness.value if output else None,
        "summary": output.summary if output else None,
        "classification_reason": output.classification_reason if output else None,
        "entities": (
            [entity.model_dump(mode="json") for entity in output.entities] if output else []
        ),
        "claims": ([claim.model_dump(mode="json") for claim in output.claims] if output else []),
        "usage": result.usage.model_dump(mode="json"),
        "warnings": result.warnings,
        "error": result.error,
        "started_at": result.started_at.astimezone(UTC),
        "completed_at": result.completed_at.astimezone(UTC),
        "updated_at": timestamp,
    }


def tag_values(result: EnrichmentResult) -> list[dict[str, Any]]:
    if not result.output:
        return []
    return [
        {
            "news_id": result.news_id,
            "enrichment_version": result.enrichment_version,
            "tag": assignment.tag.value,
            "certainty": assignment.certainty.value,
            "source_refs": assignment.source_refs,
        }
        for assignment in result.output.tags
    ]


class EnrichmentRepository:
    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    def load_candidates(
        self,
        *,
        enrichment_version: str,
        limit: int,
    ) -> list[NewsRecord]:
        base_query = select(
            news_events.c.news_id,
            news_events.c.text,
            news_events.c.source_url,
            news_events.c.author_username,
            news_events.c.published_at,
            news_events.c.source_entities,
        )
        with self.resources.engine.connect() as connection:
            if inspect(connection).has_table("news_enrichments"):
                completed = exists(
                    select(news_enrichments.c.news_id).where(
                        and_(
                            news_enrichments.c.news_id == news_events.c.news_id,
                            news_enrichments.c.enrichment_version == enrichment_version,
                            news_enrichments.c.status.in_(("completed", "completed_with_warnings")),
                        )
                    )
                )
                base_query = base_query.where(~completed)
            event_query = base_query.order_by(news_events.c.published_at.desc()).limit(limit)
            event_rows = connection.execute(event_query).mappings().all()
            news_ids = [row["news_id"] for row in event_rows]
            media_rows = (
                connection.execute(select(news_media).where(news_media.c.news_id.in_(news_ids)))
                .mappings()
                .all()
                if news_ids
                else []
            )
        media_by_news: dict[str, list[dict[str, Any]]] = {}
        for row in media_rows:
            media_by_news.setdefault(str(row["news_id"]), []).append(dict(row))
        return [
            NewsRecord(
                news_id=str(row["news_id"]),
                text=str(row["text"]),
                source_url=row["source_url"],
                author_username=row["author_username"],
                published_at=row["published_at"].astimezone(UTC).isoformat(),
                source_entities=row["source_entities"] or {},
                media=media_by_news.get(str(row["news_id"]), []),
            )
            for row in event_rows
        ]

    def persist_result(self, result: EnrichmentResult) -> None:
        values = enrichment_values(result)
        with self.resources.engine.begin() as connection:
            enrichment_insert = insert(news_enrichments).values(**values)
            update_values = {
                key: getattr(enrichment_insert.excluded, key)
                for key in values
                if key not in {"news_id", "enrichment_version", "started_at"}
            }
            connection.execute(
                enrichment_insert.on_conflict_do_update(
                    index_elements=[
                        news_enrichments.c.news_id,
                        news_enrichments.c.enrichment_version,
                    ],
                    set_=update_values,
                )
            )
            connection.execute(
                news_enrichment_tags.delete().where(
                    news_enrichment_tags.c.news_id == result.news_id,
                    news_enrichment_tags.c.enrichment_version == result.enrichment_version,
                )
            )
            tags = tag_values(result)
            if tags:
                connection.execute(insert(news_enrichment_tags), tags)
