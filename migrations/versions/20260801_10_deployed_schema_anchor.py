"""Anchor the schema revision already deployed to Cloud SQL.

Revision ID: 20260801_10
Revises: 20260731_09
Create Date: 2026-08-01

The production database was stamped with this revision before its migration
file was preserved in the repository. Its schema is compatible with the
pre-entity-bank models, so this migration intentionally contains no DDL.
"""

revision = "20260801_10"
down_revision = "20260731_09"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
