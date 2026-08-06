"""Open entity extraction and linking to Kalshi markets.

Adds entity_mentions.kalshi_market_ticker, a kalshi_market_classifications
table, and platform columns on news_market_links / news_market_reactions.
Drops the FKs that hard-wired links and reactions to Polymarket tables:
market_id/event_id/token_id become platform-scoped identifiers validated by
the loaders instead.

Revision ID: 20260805_18
Revises: 20260805_17
Create Date: 2026-08-05
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20260805_18"
down_revision = "20260805_17"
branch_labels = None
depends_on = None

ONE_SOURCE_WITH_KALSHI = """
    (CASE WHEN news_id IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN polymarket_event_id IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN polymarket_market_id IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN kalshi_market_ticker IS NOT NULL THEN 1 ELSE 0 END) = 1
"""
ONE_SOURCE_POLYMARKET_ONLY = """
    (CASE WHEN news_id IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN polymarket_event_id IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN polymarket_market_id IS NOT NULL THEN 1 ELSE 0 END) = 1
"""
# Migration 20260731_10 passed the fully prefixed name through the ck naming
# convention, so the database holds a double-prefixed name while the metadata
# name is the clean one. This migration normalizes to the metadata name.
ONE_SOURCE_LEGACY = "ck_entity_mentions_ck_entity_mentions_one_source"
ONE_SOURCE_CONSTRAINT = "ck_entity_mentions_one_source"


def upgrade() -> None:
    op.add_column(
        "entity_mentions",
        sa.Column("kalshi_market_ticker", sa.String(128)),
    )
    op.create_foreign_key(
        "fk_entity_mentions_kalshi_market",
        "entity_mentions",
        "kalshi_markets",
        ["kalshi_market_ticker"],
        ["ticker"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_entity_mentions_kalshi_market_ticker",
        "entity_mentions",
        ["kalshi_market_ticker"],
    )
    op.execute(f"ALTER TABLE entity_mentions DROP CONSTRAINT IF EXISTS {ONE_SOURCE_LEGACY}")
    op.execute(f"ALTER TABLE entity_mentions DROP CONSTRAINT IF EXISTS {ONE_SOURCE_CONSTRAINT}")
    op.execute(
        f"ALTER TABLE entity_mentions ADD CONSTRAINT {ONE_SOURCE_CONSTRAINT} "
        f"CHECK ({ONE_SOURCE_WITH_KALSHI})"
    )

    op.create_table(
        "kalshi_market_classifications",
        sa.Column(
            "market_ticker",
            sa.String(128),
            sa.ForeignKey(
                "kalshi_markets.ticker",
                name="fk_kalshi_market_classifications_market_ticker",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        sa.Column("source_content_sha256", sa.String(64), nullable=False),
        sa.Column("entity_input_sha256", sa.String(64), nullable=False),
        sa.Column("market_topic", sa.String(64), nullable=False),
        sa.Column("contract_type", sa.String(32), nullable=False),
        sa.Column("extractor_version", sa.String(64), nullable=False),
        sa.Column("confidence", sa.Numeric(), nullable=False),
        sa.Column("classification_metadata", JSONB, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_kalshi_market_classifications_entity_input",
        "kalshi_market_classifications",
        ["entity_input_sha256"],
    )

    op.add_column(
        "news_market_links",
        sa.Column(
            "platform",
            sa.String(16),
            nullable=False,
            server_default="polymarket",
        ),
    )
    op.drop_constraint(
        "fk_news_market_links_market_id_polymarket_markets",
        "news_market_links",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_news_market_links_event_id_polymarket_events",
        "news_market_links",
        type_="foreignkey",
    )

    op.add_column(
        "news_market_reactions",
        sa.Column(
            "platform",
            sa.String(16),
            nullable=False,
            server_default="polymarket",
        ),
    )
    op.drop_constraint(
        "fk_news_market_reactions_token_id_polymarket_tokens",
        "news_market_reactions",
        type_="foreignkey",
    )


def downgrade() -> None:
    op.execute("DELETE FROM news_market_reactions WHERE platform <> 'polymarket'")
    op.execute("DELETE FROM news_market_links WHERE platform <> 'polymarket'")
    op.create_foreign_key(
        "fk_news_market_reactions_token_id_polymarket_tokens",
        "news_market_reactions",
        "polymarket_tokens",
        ["token_id"],
        ["token_id"],
        ondelete="CASCADE",
    )
    op.drop_column("news_market_reactions", "platform")
    op.create_foreign_key(
        "fk_news_market_links_event_id_polymarket_events",
        "news_market_links",
        "polymarket_events",
        ["event_id"],
        ["event_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_news_market_links_market_id_polymarket_markets",
        "news_market_links",
        "polymarket_markets",
        ["market_id"],
        ["market_id"],
        ondelete="CASCADE",
    )
    op.drop_column("news_market_links", "platform")

    op.drop_index(
        "ix_kalshi_market_classifications_entity_input",
        table_name="kalshi_market_classifications",
    )
    op.drop_table("kalshi_market_classifications")

    op.execute("DELETE FROM entity_mentions WHERE kalshi_market_ticker IS NOT NULL")
    op.execute(f"ALTER TABLE entity_mentions DROP CONSTRAINT IF EXISTS {ONE_SOURCE_CONSTRAINT}")
    op.execute(
        f"ALTER TABLE entity_mentions ADD CONSTRAINT {ONE_SOURCE_CONSTRAINT} "
        f"CHECK ({ONE_SOURCE_POLYMARKET_ONLY})"
    )
    op.drop_index("ix_entity_mentions_kalshi_market_ticker", table_name="entity_mentions")
    op.drop_constraint("fk_entity_mentions_kalshi_market", "entity_mentions", type_="foreignkey")
    op.drop_column("entity_mentions", "kalshi_market_ticker")
