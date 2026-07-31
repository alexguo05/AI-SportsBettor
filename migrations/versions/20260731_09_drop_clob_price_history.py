"""Drop unused CLOB price-history tables.

Revision ID: 20260731_09
Revises: 20260731_08
Create Date: 2026-07-31
"""

import sqlalchemy as sa
from alembic import op

revision = "20260731_09"
down_revision = "20260731_08"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("polymarket_price_cursors")
    op.drop_table("polymarket_price_point_versions")
    op.drop_index(
        "ix_polymarket_price_points_source_timestamp",
        table_name="polymarket_price_points",
    )
    op.drop_table("polymarket_price_points")
    op.execute(
        """
        DELETE FROM ingest_cursors
        WHERE source = 'polymarket' AND stream = 'clob_price_history'
        """
    )


def downgrade() -> None:
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
