"""Add baseline book-quality columns to news_market_reactions.

A midpoint on a near-empty book is not a price: spread and executable depth
at the baseline snapshot let labels exclude rows where the midpoint is
meaningless and bets could not be filled.

Revision ID: 20260804_16
Revises: 20260804_15
Create Date: 2026-08-04
"""

import sqlalchemy as sa
from alembic import op

revision = "20260804_16"
down_revision = "20260804_15"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("news_market_reactions", sa.Column("baseline_spread", sa.Numeric()))
    op.add_column("news_market_reactions", sa.Column("baseline_bid_depth", sa.Numeric()))
    op.add_column("news_market_reactions", sa.Column("baseline_ask_depth", sa.Numeric()))


def downgrade() -> None:
    op.drop_column("news_market_reactions", "baseline_ask_depth")
    op.drop_column("news_market_reactions", "baseline_bid_depth")
    op.drop_column("news_market_reactions", "baseline_spread")
