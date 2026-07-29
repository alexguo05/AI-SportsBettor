"""Create X ingestion storage and cursor tables.

Revision ID: 20260729_01
Revises:
Create Date: 2026-07-29
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260729_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "raw_ingest_objects",
        sa.Column("ingest_run_id", sa.String(length=32), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("object_type", sa.String(length=64), nullable=False),
        sa.Column("schema_name", sa.String(length=64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("storage_uri", sa.Text(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("request_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("ingest_run_id", name="pk_raw_ingest_objects"),
        sa.UniqueConstraint("storage_uri", name="uq_raw_ingest_objects_storage_uri"),
    )
    op.create_table(
        "ingest_cursors",
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("stream", sa.String(length=64), nullable=False),
        sa.Column("query_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("since_id", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_successful_poll_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("source", "stream", name="pk_ingest_cursors"),
    )
    op.create_table(
        "news_events",
        sa.Column("news_id", sa.String(length=128), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("source_post_id", sa.String(length=64), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("first_raw_ingest_run_id", sa.String(length=32), nullable=False),
        sa.Column("author_source_user_id", sa.String(length=64), nullable=True),
        sa.Column("author_username", sa.String(length=64), nullable=True),
        sa.Column("author_display_name", sa.Text(), nullable=True),
        sa.Column("author_verified", sa.Boolean(), nullable=True),
        sa.Column("author_profile_image_url", sa.Text(), nullable=True),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("language", sa.String(length=16), nullable=True),
        sa.Column("conversation_id", sa.String(length=64), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("possibly_sensitive", sa.Boolean(), nullable=True),
        sa.Column("public_metrics", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_entities", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "edit_history_post_ids",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["first_raw_ingest_run_id"],
            ["raw_ingest_objects.ingest_run_id"],
            name="fk_news_events_first_raw_ingest_run_id_raw_ingest_objects",
        ),
        sa.PrimaryKeyConstraint("news_id", name="pk_news_events"),
        sa.UniqueConstraint(
            "source",
            "source_post_id",
            name="uq_news_events_source",
        ),
    )
    op.create_index(
        "ix_news_events_author_username",
        "news_events",
        ["author_username"],
        unique=False,
    )
    op.create_index(
        "ix_news_events_published_at",
        "news_events",
        ["published_at"],
        unique=False,
    )
    op.create_table(
        "news_media",
        sa.Column("news_id", sa.String(length=128), nullable=False),
        sa.Column("media_key", sa.String(length=128), nullable=False),
        sa.Column("media_type", sa.String(length=32), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("preview_image_url", sa.Text(), nullable=True),
        sa.Column("gcs_uri", sa.Text(), nullable=True),
        sa.Column("content_type", sa.String(length=128), nullable=True),
        sa.Column("content_sha256", sa.String(length=64), nullable=True),
        sa.Column("width", sa.Integer(), nullable=True),
        sa.Column("height", sa.Integer(), nullable=True),
        sa.Column("duration_ms", sa.BigInteger(), nullable=True),
        sa.Column("alt_text", sa.Text(), nullable=True),
        sa.Column("upload_status", sa.String(length=32), nullable=False),
        sa.Column("upload_error", sa.Text(), nullable=True),
        sa.Column(
            "processing_status",
            sa.String(length=32),
            server_default="pending",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["news_id"],
            ["news_events.news_id"],
            name="fk_news_media_news_id_news_events",
        ),
        sa.PrimaryKeyConstraint("news_id", "media_key", name="pk_news_media"),
    )
    op.create_index(
        "ix_news_media_processing_status",
        "news_media",
        ["processing_status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_news_media_processing_status", table_name="news_media")
    op.drop_table("news_media")
    op.drop_index("ix_news_events_published_at", table_name="news_events")
    op.drop_index("ix_news_events_author_username", table_name="news_events")
    op.drop_table("news_events")
    op.drop_table("ingest_cursors")
    op.drop_table("raw_ingest_objects")
