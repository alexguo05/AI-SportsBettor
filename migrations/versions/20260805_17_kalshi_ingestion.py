"""Add Kalshi ingestion tables: series, events, markets, trades, order books.

Revision ID: 20260805_17
Revises: 20260804_16
Create Date: 2026-08-05
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20260805_17"
down_revision = "20260804_16"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kalshi_series",
        sa.Column("series_ticker", sa.String(64), primary_key=True),
        sa.Column("title", sa.Text()),
        sa.Column("category", sa.String(64)),
        sa.Column("frequency", sa.String(64)),
        sa.Column("tags", JSONB, nullable=False),
        sa.Column("fee_type", sa.String(64)),
        sa.Column("fee_multiplier", sa.Numeric()),
        sa.Column("settlement_sources", JSONB, nullable=False),
        sa.Column("contract_url", sa.Text()),
        sa.Column(
            "latest_raw_ingest_run_id",
            sa.String(32),
            sa.ForeignKey(
                "raw_ingest_objects.ingest_run_id",
                name="fk_kalshi_series_latest_raw_ingest_run_id_raw_ingest_objects",
            ),
            nullable=False,
        ),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "kalshi_events",
        sa.Column("event_ticker", sa.String(96), primary_key=True),
        sa.Column("series_ticker", sa.String(64), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("sub_title", sa.Text()),
        sa.Column("category", sa.String(64)),
        sa.Column("mutually_exclusive", sa.Boolean()),
        sa.Column("collateral_return_type", sa.String(32)),
        sa.Column("strike_date", sa.DateTime(timezone=True)),
        sa.Column("strike_period", sa.String(64)),
        sa.Column("settlement_sources", JSONB, nullable=False),
        sa.Column("product_metadata", JSONB),
        sa.Column("available_on_brokers", sa.Boolean()),
        sa.Column(
            "latest_raw_ingest_run_id",
            sa.String(32),
            sa.ForeignKey(
                "raw_ingest_objects.ingest_run_id",
                name="fk_kalshi_events_latest_raw_ingest_run_id_raw_ingest_objects",
            ),
            nullable=False,
        ),
        sa.Column("current_content_sha256", sa.String(64), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("missing_since", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_kalshi_events_series_ticker", "kalshi_events", ["series_ticker"])
    op.create_index("ix_kalshi_events_last_observed_at", "kalshi_events", ["last_observed_at"])

    op.create_table(
        "kalshi_event_versions",
        sa.Column(
            "event_ticker",
            sa.String(96),
            sa.ForeignKey(
                "kalshi_events.event_ticker",
                name="fk_kalshi_event_versions_event_ticker_kalshi_events",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        sa.Column("observed_at", sa.DateTime(timezone=True), primary_key=True),
        sa.Column(
            "raw_ingest_run_id",
            sa.String(32),
            sa.ForeignKey(
                "raw_ingest_objects.ingest_run_id",
                name="fk_kalshi_event_versions_raw_ingest_run_id_raw_ingest_objects",
            ),
            nullable=False,
        ),
        sa.Column("content_sha256", sa.String(64), nullable=False),
    )

    op.create_table(
        "kalshi_markets",
        sa.Column("ticker", sa.String(128), primary_key=True),
        sa.Column("event_ticker", sa.String(96), nullable=False),
        sa.Column("series_ticker", sa.String(64)),
        sa.Column("market_type", sa.String(16), nullable=False),
        sa.Column("title", sa.Text()),
        sa.Column("yes_sub_title", sa.Text()),
        sa.Column("no_sub_title", sa.Text()),
        sa.Column("rules_primary", sa.Text()),
        sa.Column("rules_secondary", sa.Text()),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("result", sa.String(16)),
        sa.Column("settlement_value", sa.Numeric()),
        sa.Column("settlement_ts", sa.DateTime(timezone=True)),
        sa.Column("expiration_value", sa.Text()),
        sa.Column("can_close_early", sa.Boolean()),
        sa.Column("early_close_condition", sa.Text()),
        sa.Column("open_time", sa.DateTime(timezone=True)),
        sa.Column("close_time", sa.DateTime(timezone=True)),
        sa.Column("expected_expiration_time", sa.DateTime(timezone=True)),
        sa.Column("latest_expiration_time", sa.DateTime(timezone=True)),
        sa.Column("occurrence_datetime", sa.DateTime(timezone=True)),
        sa.Column("created_time", sa.DateTime(timezone=True)),
        sa.Column("updated_time", sa.DateTime(timezone=True)),
        sa.Column("settlement_timer_seconds", sa.Integer()),
        sa.Column("strike_type", sa.String(32)),
        sa.Column("floor_strike", sa.Numeric()),
        sa.Column("cap_strike", sa.Numeric()),
        sa.Column("functional_strike", sa.Text()),
        sa.Column("custom_strike", JSONB),
        sa.Column("price_level_structure", sa.Text()),
        sa.Column("price_ranges", JSONB),
        sa.Column("notional_value", sa.Numeric()),
        sa.Column("is_provisional", sa.Boolean()),
        sa.Column("primary_participant_key", sa.Text()),
        sa.Column("mve_collection_ticker", sa.Text()),
        sa.Column("yes_bid", sa.Numeric()),
        sa.Column("yes_ask", sa.Numeric()),
        sa.Column("no_bid", sa.Numeric()),
        sa.Column("no_ask", sa.Numeric()),
        sa.Column("last_price", sa.Numeric()),
        sa.Column("previous_price", sa.Numeric()),
        sa.Column("yes_bid_size", sa.Numeric()),
        sa.Column("yes_ask_size", sa.Numeric()),
        sa.Column("volume", sa.Numeric()),
        sa.Column("volume_24h", sa.Numeric()),
        sa.Column("open_interest", sa.Numeric()),
        sa.Column(
            "latest_raw_ingest_run_id",
            sa.String(32),
            sa.ForeignKey(
                "raw_ingest_objects.ingest_run_id",
                name="fk_kalshi_markets_latest_raw_ingest_run_id_raw_ingest_objects",
            ),
            nullable=False,
        ),
        sa.Column("current_content_sha256", sa.String(64), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("missing_since", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_kalshi_markets_event_ticker", "kalshi_markets", ["event_ticker"])
    op.create_index("ix_kalshi_markets_series_ticker", "kalshi_markets", ["series_ticker"])
    op.create_index("ix_kalshi_markets_status", "kalshi_markets", ["status"])
    op.create_index("ix_kalshi_markets_settlement_ts", "kalshi_markets", ["settlement_ts"])

    op.create_table(
        "kalshi_market_versions",
        sa.Column(
            "ticker",
            sa.String(128),
            sa.ForeignKey(
                "kalshi_markets.ticker",
                name="fk_kalshi_market_versions_ticker_kalshi_markets",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        sa.Column("observed_at", sa.DateTime(timezone=True), primary_key=True),
        sa.Column(
            "raw_ingest_run_id",
            sa.String(32),
            sa.ForeignKey(
                "raw_ingest_objects.ingest_run_id",
                name="fk_kalshi_market_versions_raw_ingest_run_id_raw_ingest_objects",
            ),
            nullable=False,
        ),
        sa.Column("content_sha256", sa.String(64), nullable=False),
    )

    op.create_table(
        "kalshi_trades",
        sa.Column("trade_id", sa.String(64), primary_key=True),
        sa.Column("ticker", sa.String(128), nullable=False),
        sa.Column("count", sa.Numeric(), nullable=False),
        sa.Column("yes_price", sa.Numeric(), nullable=False),
        sa.Column("no_price", sa.Numeric(), nullable=False),
        sa.Column("taker_outcome_side", sa.String(8)),
        sa.Column("taker_book_side", sa.String(8)),
        sa.Column("is_block_trade", sa.Boolean(), nullable=False),
        sa.Column("traded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "raw_ingest_run_id",
            sa.String(32),
            sa.ForeignKey(
                "raw_ingest_objects.ingest_run_id",
                name="fk_kalshi_trades_raw_ingest_run_id_raw_ingest_objects",
            ),
            nullable=False,
        ),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_kalshi_trades_ticker_traded_at", "kalshi_trades", ["ticker", "traded_at"]
    )
    op.create_index("ix_kalshi_trades_traded_at", "kalshi_trades", ["traded_at"])

    op.create_table(
        "kalshi_current_order_books",
        sa.Column(
            "ticker",
            sa.String(128),
            sa.ForeignKey(
                "kalshi_markets.ticker",
                name="fk_kalshi_current_order_books_ticker_kalshi_markets",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("depth_usdc", sa.Numeric(), nullable=False),
        sa.Column("bids", JSONB, nullable=False),
        sa.Column("asks", JSONB, nullable=False),
        sa.Column("best_bid", sa.Numeric()),
        sa.Column("best_ask", sa.Numeric()),
        sa.Column("midpoint", sa.Numeric()),
        sa.Column("spread", sa.Numeric()),
        sa.Column("bid_captured_notional", sa.Numeric(), nullable=False),
        sa.Column("bid_captured_shares", sa.Numeric(), nullable=False),
        sa.Column("bid_total_notional", sa.Numeric(), nullable=False),
        sa.Column("bid_truncated", sa.Boolean(), nullable=False),
        sa.Column("ask_captured_notional", sa.Numeric(), nullable=False),
        sa.Column("ask_captured_shares", sa.Numeric(), nullable=False),
        sa.Column("ask_total_notional", sa.Numeric(), nullable=False),
        sa.Column("ask_truncated", sa.Boolean(), nullable=False),
        sa.Column(
            "raw_ingest_run_id",
            sa.String(32),
            sa.ForeignKey(
                "raw_ingest_objects.ingest_run_id",
                name="fk_kalshi_current_order_books_raw_ingest_run_id_raw_ingest_objects",
            ),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_kalshi_current_order_books_observed_at",
        "kalshi_current_order_books",
        ["observed_at"],
    )


def downgrade() -> None:
    op.drop_table("kalshi_current_order_books")
    op.drop_table("kalshi_trades")
    op.drop_table("kalshi_market_versions")
    op.drop_table("kalshi_markets")
    op.drop_table("kalshi_event_versions")
    op.drop_table("kalshi_events")
    op.drop_table("kalshi_series")
