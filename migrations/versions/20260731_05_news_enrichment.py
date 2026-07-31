"""Add versioned semantic enrichment without changing raw source records.

Revision ID: 20260731_05
Revises: 20260730_04
Create Date: 2026-07-31
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260731_05"
down_revision = "20260730_04"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "news_enrichments",
        sa.Column("news_id", sa.String(length=128), nullable=False),
        sa.Column("enrichment_version", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("model_name", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "input_manifest",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("information_status", sa.String(length=32), nullable=True),
        sa.Column("usefulness", sa.String(length=32), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("classification_reason", sa.Text(), nullable=True),
        sa.Column("entities", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("claims", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("usage", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("warnings", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
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
            name="fk_news_enrichments_news_id_news_events",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "news_id",
            "enrichment_version",
            name="pk_news_enrichments",
        ),
    )
    op.create_index(
        "ix_news_enrichments_status",
        "news_enrichments",
        ["status"],
        unique=False,
    )
    op.create_index(
        "ix_news_enrichments_usefulness",
        "news_enrichments",
        ["usefulness"],
        unique=False,
    )
    op.create_table(
        "news_enrichment_tags",
        sa.Column("news_id", sa.String(length=128), nullable=False),
        sa.Column("enrichment_version", sa.String(length=64), nullable=False),
        sa.Column("tag", sa.String(length=64), nullable=False),
        sa.Column("certainty", sa.String(length=32), nullable=False),
        sa.Column(
            "source_refs",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["news_id", "enrichment_version"],
            ["news_enrichments.news_id", "news_enrichments.enrichment_version"],
            name="fk_news_enrichment_tags_enrichment",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "news_id",
            "enrichment_version",
            "tag",
            name="pk_news_enrichment_tags",
        ),
    )
    op.create_index(
        "ix_news_enrichment_tags_tag",
        "news_enrichment_tags",
        ["tag"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_news_enrichment_tags_tag", table_name="news_enrichment_tags")
    op.drop_table("news_enrichment_tags")
    op.drop_index("ix_news_enrichments_usefulness", table_name="news_enrichments")
    op.drop_index("ix_news_enrichments_status", table_name="news_enrichments")
    op.drop_table("news_enrichments")
