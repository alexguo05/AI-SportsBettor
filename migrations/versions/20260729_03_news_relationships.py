"""Add normalized relationships between X posts.

Revision ID: 20260729_03
Revises: 20260729_02
Create Date: 2026-07-29
"""

import sqlalchemy as sa
from alembic import op

revision = "20260729_03"
down_revision = "20260729_02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "news_event_relationships",
        sa.Column("source_news_id", sa.String(length=128), nullable=False),
        sa.Column("target_news_id", sa.String(length=128), nullable=False),
        sa.Column("target_source_post_id", sa.String(length=64), nullable=False),
        sa.Column("relationship_type", sa.String(length=32), nullable=False),
        sa.Column("target_available", sa.Boolean(), nullable=False),
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
            ["source_news_id"],
            ["news_events.news_id"],
            name="fk_news_event_relationships_source_news_id_news_events",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "source_news_id",
            "target_news_id",
            "relationship_type",
            name="pk_news_event_relationships",
        ),
    )
    op.create_index(
        "ix_news_event_relationships_target_news_id",
        "news_event_relationships",
        ["target_news_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_news_event_relationships_target_news_id",
        table_name="news_event_relationships",
    )
    op.drop_table("news_event_relationships")
