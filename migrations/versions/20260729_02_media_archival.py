"""Add explicit media archival metadata.

Revision ID: 20260729_02
Revises: 20260729_01
Create Date: 2026-07-29
"""

import sqlalchemy as sa
from alembic import op

revision = "20260729_02"
down_revision = "20260729_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("news_media", sa.Column("selected_source_url", sa.Text(), nullable=True))
    op.add_column(
        "news_media",
        sa.Column("stored_asset_kind", sa.String(length=32), nullable=True),
    )
    op.add_column("news_media", sa.Column("byte_size", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("news_media", "byte_size")
    op.drop_column("news_media", "stored_asset_kind")
    op.drop_column("news_media", "selected_source_url")
