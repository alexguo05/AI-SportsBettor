"""Relational schema for raw ingestion objects and normalized X news."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = MetaData(naming_convention=NAMING_CONVENTION)

raw_ingest_objects = Table(
    "raw_ingest_objects",
    metadata,
    Column("ingest_run_id", String(32), primary_key=True),
    Column("source", String(32), nullable=False),
    Column("object_type", String(64), nullable=False),
    Column("schema_name", String(64), nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("storage_uri", Text, nullable=False, unique=True),
    Column("content_sha256", String(64), nullable=False),
    Column("record_count", Integer, nullable=False),
    Column("ingested_at", DateTime(timezone=True), nullable=False),
    Column("request_metadata", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

news_events = Table(
    "news_events",
    metadata,
    Column("news_id", String(128), primary_key=True),
    Column("source", String(32), nullable=False),
    Column("source_post_id", String(64), nullable=False),
    Column("source_url", Text, nullable=False),
    Column(
        "first_raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
    Column("author_source_user_id", String(64)),
    Column("author_username", String(64)),
    Column("author_display_name", Text),
    Column("author_verified", Boolean),
    Column("author_profile_image_url", Text),
    Column("text", Text, nullable=False),
    Column("language", String(16)),
    Column("conversation_id", String(64)),
    Column("published_at", DateTime(timezone=True), nullable=False),
    Column("first_ingested_at", DateTime(timezone=True), nullable=False),
    Column("last_ingested_at", DateTime(timezone=True), nullable=False),
    Column("possibly_sensitive", Boolean),
    Column("public_metrics", JSONB, nullable=False),
    Column("source_entities", JSONB, nullable=False),
    Column("edit_history_post_ids", JSONB, nullable=False),
    UniqueConstraint("source", "source_post_id"),
)
Index("ix_news_events_published_at", news_events.c.published_at)
Index("ix_news_events_author_username", news_events.c.author_username)

news_event_relationships = Table(
    "news_event_relationships",
    metadata,
    Column(
        "source_news_id",
        String(128),
        ForeignKey("news_events.news_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("target_news_id", String(128), primary_key=True),
    Column("target_source_post_id", String(64), nullable=False),
    Column("relationship_type", String(32), primary_key=True),
    Column("target_available", Boolean, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
Index("ix_news_event_relationships_target_news_id", news_event_relationships.c.target_news_id)

news_media = Table(
    "news_media",
    metadata,
    Column("news_id", String(128), ForeignKey("news_events.news_id"), primary_key=True),
    Column("media_key", String(128), primary_key=True),
    Column("media_type", String(32)),
    Column("source_url", Text),
    Column("preview_image_url", Text),
    Column("selected_source_url", Text),
    Column("stored_asset_kind", String(32)),
    Column("gcs_uri", Text),
    Column("content_type", String(128)),
    Column("content_sha256", String(64)),
    Column("byte_size", BigInteger),
    Column("width", Integer),
    Column("height", Integer),
    Column("duration_ms", BigInteger),
    Column("alt_text", Text),
    Column("upload_status", String(32), nullable=False),
    Column("upload_error", Text),
    Column("processing_status", String(32), nullable=False, server_default="pending"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
Index("ix_news_media_processing_status", news_media.c.processing_status)

ingest_cursors = Table(
    "ingest_cursors",
    metadata,
    Column("source", String(32), primary_key=True),
    Column("stream", String(64), primary_key=True),
    Column("query_fingerprint", String(64), nullable=False),
    Column("since_id", String(64)),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("last_successful_poll_at", DateTime(timezone=True), nullable=False),
)
