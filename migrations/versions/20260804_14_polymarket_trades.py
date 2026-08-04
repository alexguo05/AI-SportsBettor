"""Add append-only Polymarket trade prints.

Revision ID: 20260804_14
Revises: 20260804_13
Create Date: 2026-08-04
"""

import sqlalchemy as sa
from alembic import op

revision = "20260804_14"
down_revision = "20260804_13"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_trades",
        sa.Column("trade_uid", sa.String(length=64), nullable=False),
        sa.Column("token_id", sa.String(length=128), nullable=False),
        sa.Column("condition_id", sa.String(length=128), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("outcome", sa.Text()),
        sa.Column("outcome_index", sa.Integer()),
        sa.Column("price", sa.Numeric(), nullable=False),
        sa.Column("size", sa.Numeric(), nullable=False),
        sa.Column("traded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("transaction_hash", sa.String(length=80)),
        sa.Column("proxy_wallet", sa.String(length=64)),
        sa.Column(
            "raw_ingest_run_id",
            sa.String(length=32),
            sa.ForeignKey("raw_ingest_objects.ingest_run_id"),
            nullable=False,
        ),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("trade_uid", name="pk_polymarket_trades"),
    )
    op.create_index(
        "ix_polymarket_trades_token_traded_at",
        "polymarket_trades",
        ["token_id", "traded_at"],
    )
    op.create_index(
        "ix_polymarket_trades_condition_id",
        "polymarket_trades",
        ["condition_id"],
    )
    op.create_index(
        "ix_polymarket_trades_traded_at",
        "polymarket_trades",
        ["traded_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_polymarket_trades_traded_at", table_name="polymarket_trades")
    op.drop_index("ix_polymarket_trades_condition_id", table_name="polymarket_trades")
    op.drop_index(
        "ix_polymarket_trades_token_traded_at",
        table_name="polymarket_trades",
    )
    op.drop_table("polymarket_trades")
