"""Add Polymarket market resolution outcome columns.

Revision ID: 20260804_13
Revises: 20260803_12
Create Date: 2026-08-04
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260804_13"
down_revision = "20260803_12"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("polymarket_markets", sa.Column("outcome_prices", postgresql.JSONB()))
    op.add_column(
        "polymarket_markets",
        sa.Column("uma_resolution_status", sa.String(length=64)),
    )
    op.add_column("polymarket_markets", sa.Column("winning_outcome_index", sa.Integer()))
    op.add_column(
        "polymarket_markets",
        sa.Column("closed_time", sa.DateTime(timezone=True)),
    )
    op.add_column(
        "polymarket_markets",
        sa.Column("resolution_observed_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "ix_polymarket_markets_uma_resolution_status",
        "polymarket_markets",
        ["uma_resolution_status"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_polymarket_markets_uma_resolution_status",
        table_name="polymarket_markets",
    )
    op.drop_column("polymarket_markets", "resolution_observed_at")
    op.drop_column("polymarket_markets", "closed_time")
    op.drop_column("polymarket_markets", "winning_outcome_index")
    op.drop_column("polymarket_markets", "uma_resolution_status")
    op.drop_column("polymarket_markets", "outcome_prices")
