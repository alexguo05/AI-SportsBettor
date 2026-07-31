"""Keep full depth only for current order books.

Revision ID: 20260731_07
Revises: 20260731_06
Create Date: 2026-07-31
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260731_07"
down_revision = "20260731_06"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_current_order_books",
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("condition_id", sa.String(length=128), nullable=True),
        sa.Column("book_hash", sa.String(length=128), nullable=True),
        sa.Column("depth_usdc", sa.Numeric(), nullable=False),
        sa.Column(
            "bids",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "asks",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("best_bid", sa.Numeric(), nullable=True),
        sa.Column("best_ask", sa.Numeric(), nullable=True),
        sa.Column("midpoint", sa.Numeric(), nullable=True),
        sa.Column("spread", sa.Numeric(), nullable=True),
        sa.Column("bid_captured_notional", sa.Numeric(), nullable=False),
        sa.Column("bid_captured_shares", sa.Numeric(), nullable=False),
        sa.Column("bid_total_notional", sa.Numeric(), nullable=False),
        sa.Column("bid_truncated", sa.Boolean(), nullable=False),
        sa.Column("ask_captured_notional", sa.Numeric(), nullable=False),
        sa.Column("ask_captured_shares", sa.Numeric(), nullable=False),
        sa.Column("ask_total_notional", sa.Numeric(), nullable=False),
        sa.Column("ask_truncated", sa.Boolean(), nullable=False),
        sa.Column("tick_size", sa.Numeric(), nullable=True),
        sa.Column("min_order_size", sa.Numeric(), nullable=True),
        sa.Column("last_trade_price", sa.Numeric(), nullable=True),
        sa.Column("raw_ingest_run_id", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["token_id"],
            ["polymarket_tokens.token_id"],
            name="fk_pm_current_order_books_token",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["raw_ingest_run_id"],
            ["raw_ingest_objects.ingest_run_id"],
            name="fk_pm_current_order_books_raw_ingest",
        ),
        sa.PrimaryKeyConstraint(
            "token_id",
            name="pk_polymarket_current_order_books",
        ),
    )
    op.create_index(
        "ix_polymarket_current_order_books_observed_at",
        "polymarket_current_order_books",
        ["observed_at"],
        unique=False,
    )
    op.execute(
        sa.text(
            """
            INSERT INTO polymarket_current_order_books (
                token_id, observed_at, source_timestamp, condition_id, book_hash,
                depth_usdc, bids, asks, best_bid, best_ask, midpoint, spread,
                bid_captured_notional, bid_captured_shares, bid_total_notional,
                bid_truncated, ask_captured_notional, ask_captured_shares,
                ask_total_notional, ask_truncated, tick_size, min_order_size,
                last_trade_price, raw_ingest_run_id
            )
            SELECT DISTINCT ON (token_id)
                token_id, observed_at, source_timestamp, condition_id, book_hash,
                depth_usdc, bids, asks, best_bid, best_ask, midpoint, spread,
                bid_captured_notional, bid_captured_shares, bid_total_notional,
                bid_truncated, ask_captured_notional, ask_captured_shares,
                ask_total_notional, ask_truncated, tick_size, min_order_size,
                last_trade_price, raw_ingest_run_id
            FROM polymarket_order_book_snapshots
            ORDER BY token_id, observed_at DESC
            """
        )
    )
    op.drop_column("polymarket_order_book_snapshots", "asks")
    op.drop_column("polymarket_order_book_snapshots", "bids")


def downgrade() -> None:
    empty_levels = sa.text("'[]'::jsonb")
    op.add_column(
        "polymarket_order_book_snapshots",
        sa.Column(
            "bids",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=empty_levels,
            nullable=False,
        ),
    )
    op.add_column(
        "polymarket_order_book_snapshots",
        sa.Column(
            "asks",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=empty_levels,
            nullable=False,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE polymarket_order_book_snapshots AS snapshots
            SET bids = current.bids, asks = current.asks
            FROM polymarket_current_order_books AS current
            WHERE snapshots.token_id = current.token_id
              AND snapshots.observed_at = current.observed_at
            """
        )
    )
    op.alter_column(
        "polymarket_order_book_snapshots",
        "bids",
        server_default=None,
    )
    op.alter_column(
        "polymarket_order_book_snapshots",
        "asks",
        server_default=None,
    )
    op.drop_index(
        "ix_polymarket_current_order_books_observed_at",
        table_name="polymarket_current_order_books",
    )
    op.drop_table("polymarket_current_order_books")
