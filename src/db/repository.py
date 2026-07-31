"""Transactional PostgreSQL writes for normalized X ingestion records."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.dialects.postgresql import insert

from src.db.engine import DatabaseResources, create_database_resources
from src.db.models import (
    ingest_cursors,
    news_event_relationships,
    news_events,
    news_media,
    raw_ingest_objects,
)

X_CURSOR_SOURCE = "x"
X_CURSOR_STREAM = "recent_search"


def _timestamp(value: str | datetime | None, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError(f"{field_name} is required")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def raw_object_values(envelope: dict[str, Any]) -> dict[str, Any]:
    provider = envelope.get("provider") or envelope["source"]
    source = envelope["source"]
    object_type = envelope["object_type"]
    if "provider" not in envelope and source == "x":
        source = "recent-search"
        if object_type == "news_posts":
            object_type = "posts"
    return {
        "ingest_run_id": envelope["ingest_run_id"],
        "provider": provider,
        "source": source,
        "object_type": object_type,
        "schema_name": envelope["schema_name"],
        "schema_version": envelope["schema_version"],
        "storage_uri": envelope["storage_uri"],
        "content_sha256": envelope["content_sha256"],
        "record_count": envelope["record_count"],
        "ingested_at": _timestamp(envelope["ingested_at"], "ingested_at"),
        "request_metadata": {
            **envelope["request"],
            "checkpoint_candidate": envelope.get("checkpoint_candidate"),
        },
    }


def news_event_values(record: dict[str, Any], ingest_run_id: str) -> dict[str, Any]:
    author = record.get("author") or {}
    ingested_at = _timestamp(record.get("ingested_at"), "record.ingested_at")
    return {
        "news_id": record["news_id"],
        "source": record["source"],
        "source_post_id": record["source_post_id"],
        "source_url": record["source_url"],
        "first_raw_ingest_run_id": ingest_run_id,
        "author_source_user_id": author.get("source_user_id"),
        "author_username": author.get("username"),
        "author_display_name": author.get("display_name"),
        "author_verified": author.get("verified"),
        "author_profile_image_url": author.get("profile_image_url"),
        "text": record["text"],
        "language": record.get("language"),
        "conversation_id": record.get("conversation_id"),
        "published_at": _timestamp(record.get("published_at"), "record.published_at"),
        "first_ingested_at": ingested_at,
        "last_ingested_at": ingested_at,
        "possibly_sensitive": record.get("possibly_sensitive"),
        "public_metrics": record.get("public_metrics") or {},
        "source_entities": record.get("source_entities") or {},
        "edit_history_post_ids": record.get("edit_history_post_ids") or [],
    }


def media_values(record: dict[str, Any], media: dict[str, Any]) -> dict[str, Any]:
    return {
        "news_id": record["news_id"],
        "media_key": media["media_key"],
        "media_type": media.get("media_type"),
        "source_url": media.get("source_url"),
        "preview_image_url": media.get("preview_image_url"),
        "selected_source_url": media.get("selected_source_url"),
        "stored_asset_kind": media.get("stored_asset_kind"),
        "gcs_uri": media.get("gcs_uri"),
        "content_type": media.get("content_type"),
        "content_sha256": media.get("content_sha256"),
        "byte_size": media.get("byte_size"),
        "width": media.get("width"),
        "height": media.get("height"),
        "duration_ms": media.get("duration_ms"),
        "alt_text": media.get("alt_text"),
        "upload_status": media.get("upload_status", "pending"),
        "upload_error": media.get("upload_error"),
        "processing_status": media.get("upload_status", "pending"),
    }


def relationship_values(
    record: dict[str, Any],
    relationship: dict[str, Any],
) -> dict[str, Any]:
    return {
        "source_news_id": record["news_id"],
        "target_news_id": relationship["target_news_id"],
        "target_source_post_id": relationship["target_source_post_id"],
        "relationship_type": relationship["relationship_type"],
        "target_available": relationship.get("target_available", False),
    }


class NewsRepository:
    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> NewsRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_checkpoint(self) -> dict[str, Any]:
        with self.resources.engine.connect() as connection:
            row = (
                connection.execute(
                    ingest_cursors.select().where(
                        ingest_cursors.c.source == X_CURSOR_SOURCE,
                        ingest_cursors.c.stream == X_CURSOR_STREAM,
                    )
                )
                .mappings()
                .one_or_none()
            )
        return dict(row) if row else {}

    def persist_records(self, envelope: dict[str, Any]) -> None:
        with self.resources.engine.begin() as connection:
            raw_values = raw_object_values(envelope)
            connection.execute(
                insert(raw_ingest_objects)
                .values(**raw_values)
                .on_conflict_do_nothing(index_elements=[raw_ingest_objects.c.ingest_run_id])
            )
            for record in envelope["records"]:
                event_values = news_event_values(
                    record,
                    envelope["ingest_run_id"],
                )
                event_insert = insert(news_events).values(**event_values)
                connection.execute(
                    event_insert.on_conflict_do_update(
                        index_elements=[news_events.c.news_id],
                        set_={
                            "source_url": event_insert.excluded.source_url,
                            "text": event_insert.excluded.text,
                            "language": event_insert.excluded.language,
                            "last_ingested_at": event_insert.excluded.last_ingested_at,
                            "possibly_sensitive": (event_insert.excluded.possibly_sensitive),
                            "public_metrics": event_insert.excluded.public_metrics,
                            "source_entities": event_insert.excluded.source_entities,
                            "edit_history_post_ids": (event_insert.excluded.edit_history_post_ids),
                        },
                    )
                )
                for relationship in record.get("relationships", []):
                    values = relationship_values(record, relationship)
                    relationship_insert = insert(news_event_relationships).values(**values)
                    connection.execute(
                        relationship_insert.on_conflict_do_update(
                            index_elements=[
                                news_event_relationships.c.source_news_id,
                                news_event_relationships.c.target_news_id,
                                news_event_relationships.c.relationship_type,
                            ],
                            set_={
                                "target_source_post_id": (
                                    relationship_insert.excluded.target_source_post_id
                                ),
                                "target_available": (relationship_insert.excluded.target_available),
                                "updated_at": datetime.now(UTC),
                            },
                        )
                    )
                for media in record.get("media", []):
                    values = media_values(record, media)
                    media_insert = insert(news_media).values(**values)
                    connection.execute(
                        media_insert.on_conflict_do_update(
                            index_elements=[
                                news_media.c.news_id,
                                news_media.c.media_key,
                            ],
                            set_={
                                "media_type": media_insert.excluded.media_type,
                                "source_url": media_insert.excluded.source_url,
                                "preview_image_url": (media_insert.excluded.preview_image_url),
                                "selected_source_url": (media_insert.excluded.selected_source_url),
                                "stored_asset_kind": (media_insert.excluded.stored_asset_kind),
                                "width": media_insert.excluded.width,
                                "height": media_insert.excluded.height,
                                "duration_ms": media_insert.excluded.duration_ms,
                                "alt_text": media_insert.excluded.alt_text,
                                "updated_at": datetime.now(UTC),
                            },
                        )
                    )

    def finalize_cycle(
        self,
        *,
        records: list[dict[str, Any]],
        checkpoint: dict[str, Any],
    ) -> None:
        with self.resources.engine.begin() as connection:
            for record in records:
                for media in record.get("media", []):
                    values = media_values(record, media)
                    connection.execute(
                        news_media.update()
                        .where(
                            news_media.c.news_id == values["news_id"],
                            news_media.c.media_key == values["media_key"],
                        )
                        .values(
                            selected_source_url=values["selected_source_url"],
                            stored_asset_kind=values["stored_asset_kind"],
                            gcs_uri=values["gcs_uri"],
                            content_type=values["content_type"],
                            content_sha256=values["content_sha256"],
                            byte_size=values["byte_size"],
                            upload_status=values["upload_status"],
                            upload_error=values["upload_error"],
                            processing_status=values["processing_status"],
                            updated_at=datetime.now(UTC),
                        )
                    )
            cursor_insert = insert(ingest_cursors).values(
                source=X_CURSOR_SOURCE,
                stream=X_CURSOR_STREAM,
                query_fingerprint=checkpoint["query_fingerprint"],
                since_id=checkpoint.get("since_id"),
                updated_at=_timestamp(checkpoint["updated_at"], "checkpoint.updated_at"),
                last_successful_poll_at=_timestamp(
                    checkpoint["last_successful_poll_at"],
                    "checkpoint.last_successful_poll_at",
                ),
            )
            connection.execute(
                cursor_insert.on_conflict_do_update(
                    index_elements=[
                        ingest_cursors.c.source,
                        ingest_cursors.c.stream,
                    ],
                    set_={
                        "query_fingerprint": cursor_insert.excluded.query_fingerprint,
                        "since_id": cursor_insert.excluded.since_id,
                        "updated_at": cursor_insert.excluded.updated_at,
                        "last_successful_poll_at": (cursor_insert.excluded.last_successful_poll_at),
                    },
                )
            )
