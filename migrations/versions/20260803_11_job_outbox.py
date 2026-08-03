"""Add durable concurrent job outbox.

Revision ID: 20260803_12
Revises: 20260803_11
Create Date: 2026-08-03
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260803_12"
down_revision = "20260803_11"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_outbox",
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("job_type", sa.String(length=32), nullable=False),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(length=128)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "status IN ('pending', 'leased', 'completed', 'dead')",
            name="ck_job_outbox_status",
        ),
        sa.PrimaryKeyConstraint("job_id", name="pk_job_outbox"),
        sa.UniqueConstraint(
            "job_type",
            "idempotency_key",
            name="uq_job_outbox_identity",
        ),
    )
    op.create_index(
        "ix_job_outbox_claim",
        "job_outbox",
        ["status", "available_at", "priority"],
    )
    op.create_index(
        "ix_job_outbox_lease_expires_at",
        "job_outbox",
        ["lease_expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_job_outbox_lease_expires_at", table_name="job_outbox")
    op.drop_index("ix_job_outbox_claim", table_name="job_outbox")
    op.drop_table("job_outbox")
