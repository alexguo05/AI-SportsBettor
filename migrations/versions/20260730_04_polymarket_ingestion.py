"""Add Polymarket discovery tables and align raw object metadata.

Revision ID: 20260730_04
Revises: 20260729_03
Create Date: 2026-07-30
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260730_04"
down_revision = "20260729_03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "raw_ingest_objects",
        sa.Column("provider", sa.String(length=32), nullable=True),
    )
    op.execute("UPDATE raw_ingest_objects SET provider = source")
    op.execute(
        """
        UPDATE raw_ingest_objects
        SET source = 'recent-search', object_type = 'posts'
        WHERE provider = 'x' AND source = 'x' AND object_type = 'news_posts'
        """
    )
    op.alter_column(
        "raw_ingest_objects",
        "provider",
        existing_type=sa.String(length=32),
        nullable=False,
    )
    op.add_column(
        "ingest_cursors",
        sa.Column("last_structural_sha256", sa.String(length=64), nullable=True),
    )

    op.create_table(
        "polymarket_events",
        sa.Column("event_id", sa.String(length=128), nullable=False),
        sa.Column("slug", sa.Text(), nullable=True),
        sa.Column("ticker", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category", sa.String(length=64), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("closed", sa.Boolean(), nullable=False),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "tags",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("latest_raw_ingest_run_id", sa.String(length=32), nullable=False),
        sa.Column("current_content_sha256", sa.String(length=64), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("missing_since", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["latest_raw_ingest_run_id"],
            ["raw_ingest_objects.ingest_run_id"],
            name="fk_pm_events_raw_ingest",
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_polymarket_events"),
    )
    op.create_index(
        "ix_polymarket_events_closed",
        "polymarket_events",
        ["closed"],
        unique=False,
    )
    op.create_index(
        "ix_polymarket_events_last_observed_at",
        "polymarket_events",
        ["last_observed_at"],
        unique=False,
    )

    op.create_table(
        "polymarket_event_versions",
        sa.Column("event_id", sa.String(length=128), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_ingest_run_id", sa.String(length=32), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(
            ["event_id"],
            ["polymarket_events.event_id"],
            name="fk_pm_event_versions_event",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["raw_ingest_run_id"],
            ["raw_ingest_objects.ingest_run_id"],
            name="fk_pm_event_versions_raw_ingest",
        ),
        sa.PrimaryKeyConstraint(
            "event_id",
            "observed_at",
            name="pk_polymarket_event_versions",
        ),
    )

    op.create_table(
        "polymarket_markets",
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("event_id", sa.String(length=128), nullable=False),
        sa.Column("condition_id", sa.String(length=128), nullable=True),
        sa.Column("slug", sa.Text(), nullable=True),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("sports_market_type", sa.String(length=64), nullable=True),
        sa.Column("line", sa.Numeric(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("closed", sa.Boolean(), nullable=False),
        sa.Column("accepting_orders", sa.Boolean(), nullable=False),
        sa.Column("enable_order_book", sa.Boolean(), nullable=False),
        sa.Column("latest_raw_ingest_run_id", sa.String(length=32), nullable=False),
        sa.Column("current_content_sha256", sa.String(length=64), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("missing_since", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["event_id"],
            ["polymarket_events.event_id"],
            name="fk_pm_markets_event",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["latest_raw_ingest_run_id"],
            ["raw_ingest_objects.ingest_run_id"],
            name="fk_pm_markets_raw_ingest",
        ),
        sa.PrimaryKeyConstraint("market_id", name="pk_polymarket_markets"),
    )
    op.create_index(
        "ix_polymarket_markets_event_id",
        "polymarket_markets",
        ["event_id"],
        unique=False,
    )
    op.create_index(
        "ix_polymarket_markets_closed",
        "polymarket_markets",
        ["closed"],
        unique=False,
    )
    op.create_index(
        "ix_polymarket_markets_condition_id",
        "polymarket_markets",
        ["condition_id"],
        unique=False,
    )

    op.create_table(
        "polymarket_market_versions",
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_ingest_run_id", sa.String(length=32), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(
            ["market_id"],
            ["polymarket_markets.market_id"],
            name="fk_pm_market_versions_market",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["raw_ingest_run_id"],
            ["raw_ingest_objects.ingest_run_id"],
            name="fk_pm_market_versions_raw_ingest",
        ),
        sa.PrimaryKeyConstraint(
            "market_id",
            "observed_at",
            name="pk_polymarket_market_versions",
        ),
    )

    op.create_table(
        "polymarket_tokens",
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("outcome_index", sa.Integer(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["market_id"],
            ["polymarket_markets.market_id"],
            name="fk_pm_tokens_market",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("token_id", name="pk_polymarket_tokens"),
    )
    op.create_index(
        "ix_polymarket_tokens_market_id",
        "polymarket_tokens",
        ["market_id"],
        unique=False,
    )
    op.create_table(
        "polymarket_price_points",
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("source_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Numeric(), nullable=False),
        sa.Column("fidelity_minutes", sa.Integer(), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("latest_raw_ingest_run_id", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["token_id"],
            ["polymarket_tokens.token_id"],
            name="fk_pm_price_points_token",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["latest_raw_ingest_run_id"],
            ["raw_ingest_objects.ingest_run_id"],
            name="fk_pm_price_points_raw_ingest",
        ),
        sa.PrimaryKeyConstraint(
            "token_id",
            "source_timestamp",
            name="pk_polymarket_price_points",
        ),
    )
    op.create_index(
        "ix_polymarket_price_points_source_timestamp",
        "polymarket_price_points",
        ["source_timestamp"],
        unique=False,
    )
    op.create_table(
        "polymarket_price_point_versions",
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("source_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Numeric(), nullable=False),
        sa.Column("fidelity_minutes", sa.Integer(), nullable=False),
        sa.Column("raw_ingest_run_id", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["token_id", "source_timestamp"],
            [
                "polymarket_price_points.token_id",
                "polymarket_price_points.source_timestamp",
            ],
            name="fk_pm_price_versions_point",
            ondelete="CASCADE",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.ForeignKeyConstraint(
            ["raw_ingest_run_id"],
            ["raw_ingest_objects.ingest_run_id"],
            name="fk_pm_price_versions_raw_ingest",
        ),
        sa.PrimaryKeyConstraint(
            "token_id",
            "source_timestamp",
            "observed_at",
            name="pk_polymarket_price_point_versions",
        ),
    )
    op.create_table(
        "polymarket_price_cursors",
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("query_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("last_end_ts", sa.BigInteger(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["token_id"],
            ["polymarket_tokens.token_id"],
            name="fk_pm_price_cursors_token",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("token_id", name="pk_polymarket_price_cursors"),
    )


def downgrade() -> None:
    op.drop_table("polymarket_price_cursors")
    op.drop_table("polymarket_price_point_versions")
    op.drop_index(
        "ix_polymarket_price_points_source_timestamp",
        table_name="polymarket_price_points",
    )
    op.drop_table("polymarket_price_points")
    op.drop_index("ix_polymarket_tokens_market_id", table_name="polymarket_tokens")
    op.drop_table("polymarket_tokens")
    op.drop_table("polymarket_market_versions")
    op.drop_index("ix_polymarket_markets_condition_id", table_name="polymarket_markets")
    op.drop_index("ix_polymarket_markets_closed", table_name="polymarket_markets")
    op.drop_index("ix_polymarket_markets_event_id", table_name="polymarket_markets")
    op.drop_table("polymarket_markets")
    op.drop_table("polymarket_event_versions")
    op.drop_index(
        "ix_polymarket_events_last_observed_at",
        table_name="polymarket_events",
    )
    op.drop_index("ix_polymarket_events_closed", table_name="polymarket_events")
    op.drop_table("polymarket_events")
    op.execute(
        """
        UPDATE raw_ingest_objects
        SET source = 'x', object_type = 'news_posts'
        WHERE provider = 'x' AND source = 'recent-search' AND object_type = 'posts'
        """
    )
    op.drop_column("ingest_cursors", "last_structural_sha256")
    op.drop_column("raw_ingest_objects", "provider")
