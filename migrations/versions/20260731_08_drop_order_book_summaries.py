"""Drop redundant historical order-book summaries.

Revision ID: 20260731_08
Revises: 20260731_07
Create Date: 2026-07-31
"""

import sqlalchemy as sa
from alembic import op

revision = "20260731_08"
down_revision = "20260731_07"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        "ix_polymarket_order_book_snapshots_observed_at",
        table_name="polymarket_order_book_snapshots",
    )
    op.drop_table("polymarket_order_book_snapshots")


def downgrade() -> None:
    op.create_table(
        "polymarket_order_book_snapshots",
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("condition_id", sa.String(length=128), nullable=True),
        sa.Column("book_hash", sa.String(length=128), nullable=True),
        sa.Column("depth_usdc", sa.Numeric(), nullable=False),
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
            name="fk_pm_order_book_snapshots_token",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["raw_ingest_run_id"],
            ["raw_ingest_objects.ingest_run_id"],
            name="fk_pm_order_book_snapshots_raw_ingest",
        ),
        sa.PrimaryKeyConstraint(
            "token_id",
            "observed_at",
            name="pk_polymarket_order_book_snapshots",
        ),
    )
    op.create_index(
        "ix_polymarket_order_book_snapshots_observed_at",
        "polymarket_order_book_snapshots",
        ["observed_at"],
        unique=False,
    )
