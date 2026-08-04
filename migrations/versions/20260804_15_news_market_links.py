"""Add tweet-market links and price-reaction labels.

Revision ID: 20260804_15
Revises: 20260804_14
Create Date: 2026-08-04
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260804_15"
down_revision = "20260804_14"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "news_market_links",
        sa.Column(
            "news_id",
            sa.String(length=128),
            sa.ForeignKey("news_events.news_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "market_id",
            sa.String(length=128),
            sa.ForeignKey("polymarket_markets.market_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "event_id",
            sa.String(length=128),
            sa.ForeignKey("polymarket_events.event_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("shared_entity_ids", postgresql.JSONB(), nullable=False),
        sa.Column("shared_entity_count", sa.Integer(), nullable=False),
        sa.Column("news_mention_roles", postgresql.JSONB(), nullable=False),
        sa.Column("market_mention_roles", postgresql.JSONB(), nullable=False),
        sa.Column("market_topic", sa.String(length=64)),
        sa.Column("contract_type", sa.String(length=32)),
        sa.Column("market_open_at_publish", sa.Boolean(), nullable=False),
        sa.Column("linker_version", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("news_id", "market_id", name="pk_news_market_links"),
    )
    op.create_index(
        "ix_news_market_links_market_id",
        "news_market_links",
        ["market_id"],
    )
    op.create_index(
        "ix_news_market_links_published_at",
        "news_market_links",
        ["published_at"],
    )

    op.create_table(
        "news_market_reactions",
        sa.Column("news_id", sa.String(length=128), nullable=False),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column(
            "token_id",
            sa.String(length=128),
            sa.ForeignKey("polymarket_tokens.token_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("label_version", sa.String(length=64), nullable=False),
        sa.Column("outcome_index", sa.Integer()),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("baseline_midpoint", sa.Numeric()),
        sa.Column("baseline_observed_at", sa.DateTime(timezone=True)),
        sa.Column("midpoint_plus_1m", sa.Numeric()),
        sa.Column("midpoint_plus_5m", sa.Numeric()),
        sa.Column("midpoint_plus_30m", sa.Numeric()),
        sa.Column("midpoint_plus_2h", sa.Numeric()),
        sa.Column("delta_plus_1m", sa.Numeric()),
        sa.Column("delta_plus_5m", sa.Numeric()),
        sa.Column("delta_plus_30m", sa.Numeric()),
        sa.Column("delta_plus_2h", sa.Numeric()),
        sa.Column("trade_count", sa.Integer()),
        sa.Column("trade_notional", sa.Numeric()),
        sa.Column("snapshot_count", sa.Integer(), nullable=False),
        sa.Column("max_gap_seconds", sa.Numeric()),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "news_id",
            "market_id",
            "token_id",
            "label_version",
            name="pk_news_market_reactions",
        ),
        sa.ForeignKeyConstraint(
            ["news_id", "market_id"],
            ["news_market_links.news_id", "news_market_links.market_id"],
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_news_market_reactions_market_id",
        "news_market_reactions",
        ["market_id"],
    )
    op.create_index(
        "ix_news_market_reactions_published_at",
        "news_market_reactions",
        ["published_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_news_market_reactions_published_at",
        table_name="news_market_reactions",
    )
    op.drop_index(
        "ix_news_market_reactions_market_id",
        table_name="news_market_reactions",
    )
    op.drop_table("news_market_reactions")
    op.drop_index("ix_news_market_links_published_at", table_name="news_market_links")
    op.drop_index("ix_news_market_links_market_id", table_name="news_market_links")
    op.drop_table("news_market_links")
