"""Add nflverse-backed entity bank and source resolution.

Revision ID: 20260803_11
Revises: 20260801_10
Create Date: 2026-07-31
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260803_11"
down_revision = "20260801_10"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("polymarket_events", sa.Column("game_id", sa.String(length=128)))
    op.add_column("polymarket_markets", sa.Column("group_item_title", sa.Text()))
    op.add_column("polymarket_markets", sa.Column("group_item_threshold", sa.Text()))
    op.add_column(
        "news_enrichments",
        sa.Column("entity_extractor_version", sa.String(length=64)),
    )

    op.create_table(
        "entity_bank_versions",
        sa.Column("version_id", sa.String(length=32), nullable=False),
        sa.Column("raw_ingest_run_id", sa.String(length=32), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("source_metadata", postgresql.JSONB(), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["raw_ingest_run_id"],
            ["raw_ingest_objects.ingest_run_id"],
            name="fk_entity_bank_versions_raw",
        ),
        sa.PrimaryKeyConstraint("version_id", name="pk_entity_bank_versions"),
        sa.UniqueConstraint(
            "raw_ingest_run_id",
            name="uq_entity_bank_versions_raw_ingest_run_id",
        ),
    )
    op.create_table(
        "entities",
        sa.Column("entity_id", sa.String(length=36), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column("normalized_name", sa.Text(), nullable=False),
        sa.Column("identity_status", sa.String(length=32), nullable=False),
        sa.Column("merged_into_entity_id", sa.String(length=36)),
        sa.Column("latest_bank_version_id", sa.String(length=32)),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
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
        sa.CheckConstraint("entity_type IN ('team', 'person')", name="ck_entities_type"),
        sa.CheckConstraint(
            "identity_status IN ('canonical', 'provisional', 'merged', 'rejected')",
            name="ck_entities_identity_status",
        ),
        sa.ForeignKeyConstraint(
            ["latest_bank_version_id"],
            ["entity_bank_versions.version_id"],
            name="fk_entities_bank_version",
        ),
        sa.ForeignKeyConstraint(
            ["merged_into_entity_id"],
            ["entities.entity_id"],
            name="fk_entities_merged_into",
        ),
        sa.PrimaryKeyConstraint("entity_id", name="pk_entities"),
    )
    op.create_index("ix_entities_normalized_name", "entities", ["normalized_name"])
    op.create_index("ix_entities_identity_status", "entities", ["identity_status"])

    op.create_table(
        "entity_aliases",
        sa.Column("entity_id", sa.String(length=36), nullable=False),
        sa.Column("normalized_alias", sa.Text(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("alias", sa.Text(), nullable=False),
        sa.Column("alias_type", sa.String(length=32), nullable=False),
        sa.Column("confidence", sa.Numeric(), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_metadata", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["entities.entity_id"],
            name="fk_entity_aliases_entity",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "entity_id",
            "normalized_alias",
            "source",
            name="pk_entity_aliases",
        ),
    )
    op.create_index(
        "ix_entity_aliases_normalized_alias",
        "entity_aliases",
        ["normalized_alias"],
    )
    op.create_table(
        "entity_source_mappings",
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("source_entity_type", sa.String(length=32), nullable=False),
        sa.Column("source_entity_id", sa.String(length=128), nullable=False),
        sa.Column("entity_id", sa.String(length=36), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_metadata", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["entities.entity_id"],
            name="fk_entity_mappings_entity",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "provider",
            "source_entity_type",
            "source_entity_id",
            name="pk_entity_source_mappings",
        ),
    )
    op.create_index(
        "ix_entity_source_mappings_entity_id",
        "entity_source_mappings",
        ["entity_id"],
    )
    op.create_table(
        "entity_roles",
        sa.Column("role_id", sa.String(length=36), nullable=False),
        sa.Column("entity_id", sa.String(length=36), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True)),
        sa.Column("valid_to", sa.DateTime(timezone=True)),
        sa.Column("confidence", sa.Numeric(), nullable=False),
        sa.Column("evidence", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["entities.entity_id"],
            name="fk_entity_roles_entity",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("role_id", name="pk_entity_roles"),
        sa.UniqueConstraint(
            "entity_id",
            "role",
            "source",
            "valid_from",
            name="uq_entity_roles_identity",
        ),
    )
    op.create_index("ix_entity_roles_entity_id", "entity_roles", ["entity_id"])
    op.create_table(
        "entity_relationships",
        sa.Column("relationship_id", sa.String(length=36), nullable=False),
        sa.Column("subject_entity_id", sa.String(length=36), nullable=False),
        sa.Column("predicate", sa.String(length=32), nullable=False),
        sa.Column("object_entity_id", sa.String(length=36), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("source_key", sa.String(length=256), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True)),
        sa.Column("valid_to", sa.DateTime(timezone=True)),
        sa.Column("confidence", sa.Numeric(), nullable=False),
        sa.Column("evidence", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["object_entity_id"],
            ["entities.entity_id"],
            name="fk_entity_relationships_object",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["subject_entity_id"],
            ["entities.entity_id"],
            name="fk_entity_relationships_subject",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("relationship_id", name="pk_entity_relationships"),
        sa.UniqueConstraint("source_key", name="uq_entity_relationships_source_key"),
    )
    op.create_index(
        "ix_entity_relationships_subject",
        "entity_relationships",
        ["subject_entity_id"],
    )
    op.create_index(
        "ix_entity_relationships_object",
        "entity_relationships",
        ["object_entity_id"],
    )
    op.create_table(
        "entity_mentions",
        sa.Column("mention_id", sa.String(length=36), nullable=False),
        sa.Column("news_id", sa.String(length=128)),
        sa.Column("polymarket_event_id", sa.String(length=128)),
        sa.Column("polymarket_market_id", sa.String(length=128)),
        sa.Column("entity_id", sa.String(length=36)),
        sa.Column("mention_text", sa.Text(), nullable=False),
        sa.Column("normalized_text", sa.Text(), nullable=False),
        sa.Column("entity_type_hint", sa.String(length=32), nullable=False),
        sa.Column("person_role_hint", sa.String(length=32)),
        sa.Column("mention_role", sa.String(length=32), nullable=False),
        sa.Column("evidence", sa.Text(), nullable=False),
        sa.Column("source_refs", postgresql.JSONB(), nullable=False),
        sa.Column("source_content_sha256", sa.String(length=64), nullable=False),
        sa.Column("extractor_version", sa.String(length=64), nullable=False),
        sa.Column("resolver_version", sa.String(length=64), nullable=False),
        sa.Column("resolution_status", sa.String(length=32), nullable=False),
        sa.Column("match_method", sa.String(length=32)),
        sa.Column("confidence", sa.Numeric(), nullable=False),
        sa.Column("last_bank_version_id", sa.String(length=32)),
        sa.Column("candidate_entity_ids", postgresql.JSONB(), nullable=False),
        sa.Column("resolution_metadata", postgresql.JSONB(), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
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
        sa.CheckConstraint(
            """
            (CASE WHEN news_id IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN polymarket_event_id IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN polymarket_market_id IS NOT NULL THEN 1 ELSE 0 END) = 1
            """,
            name="ck_entity_mentions_one_source",
        ),
        sa.CheckConstraint(
            "resolution_status <> 'resolved' OR entity_id IS NOT NULL",
            name="ck_entity_mentions_resolved_entity",
        ),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["entities.entity_id"],
            name="fk_entity_mentions_entity",
        ),
        sa.ForeignKeyConstraint(
            ["last_bank_version_id"],
            ["entity_bank_versions.version_id"],
            name="fk_entity_mentions_bank_version",
        ),
        sa.ForeignKeyConstraint(
            ["news_id"],
            ["news_events.news_id"],
            name="fk_entity_mentions_news",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["polymarket_event_id"],
            ["polymarket_events.event_id"],
            name="fk_entity_mentions_pm_event",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["polymarket_market_id"],
            ["polymarket_markets.market_id"],
            name="fk_entity_mentions_pm_market",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("mention_id", name="pk_entity_mentions"),
    )
    op.create_index("ix_entity_mentions_entity_id", "entity_mentions", ["entity_id"])
    op.create_index(
        "ix_entity_mentions_resolution_status",
        "entity_mentions",
        ["resolution_status"],
    )
    op.create_table(
        "polymarket_market_classifications",
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("source_content_sha256", sa.String(length=64), nullable=False),
        sa.Column("entity_input_sha256", sa.String(length=64), nullable=False),
        sa.Column("market_topic", sa.String(length=64), nullable=False),
        sa.Column("contract_type", sa.String(length=32), nullable=False),
        sa.Column("extractor_version", sa.String(length=64), nullable=False),
        sa.Column("confidence", sa.Numeric(), nullable=False),
        sa.Column("classification_metadata", postgresql.JSONB(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["market_id"],
            ["polymarket_markets.market_id"],
            name="fk_pm_market_classifications_market",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("market_id", name="pk_pm_market_classifications"),
    )
    op.create_index(
        "ix_pm_market_classifications_entity_input",
        "polymarket_market_classifications",
        ["entity_input_sha256"],
    )
    op.create_table(
        "entity_resolution_attempts",
        sa.Column("attempt_id", sa.String(length=36), nullable=False),
        sa.Column("mention_id", sa.String(length=36), nullable=False),
        sa.Column("bank_version_id", sa.String(length=32)),
        sa.Column("resolver_version", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("model_name", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("candidate_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("decision", postgresql.JSONB(), nullable=False),
        sa.Column("usage", postgresql.JSONB(), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["bank_version_id"],
            ["entity_bank_versions.version_id"],
            name="fk_entity_resolution_attempts_bank",
        ),
        sa.ForeignKeyConstraint(
            ["mention_id"],
            ["entity_mentions.mention_id"],
            name="fk_entity_resolution_attempts_mention",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("attempt_id", name="pk_entity_resolution_attempts"),
    )
    op.create_index(
        "ix_entity_resolution_attempts_mention_id",
        "entity_resolution_attempts",
        ["mention_id"],
    )
    op.create_table(
        "news_entity_resolution_runs",
        sa.Column("news_id", sa.String(length=128), nullable=False),
        sa.Column("enrichment_version", sa.String(length=64), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("extractor_version", sa.String(length=64), nullable=False),
        sa.Column("bank_version_id", sa.String(length=32)),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("mention_count", sa.Integer(), nullable=False),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["bank_version_id"],
            ["entity_bank_versions.version_id"],
            name="fk_news_resolution_runs_bank",
        ),
        sa.ForeignKeyConstraint(
            ["news_id", "enrichment_version"],
            ["news_enrichments.news_id", "news_enrichments.enrichment_version"],
            name="fk_news_resolution_runs_enrichment",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "news_id",
            "enrichment_version",
            "input_fingerprint",
            "extractor_version",
            name="pk_news_entity_resolution_runs",
        ),
    )
    op.create_index(
        "ix_news_entity_resolution_runs_status",
        "news_entity_resolution_runs",
        ["status"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_news_entity_resolution_runs_status",
        table_name="news_entity_resolution_runs",
    )
    op.drop_table("news_entity_resolution_runs")
    op.drop_index(
        "ix_entity_resolution_attempts_mention_id",
        table_name="entity_resolution_attempts",
    )
    op.drop_table("entity_resolution_attempts")
    op.drop_index(
        "ix_pm_market_classifications_entity_input",
        table_name="polymarket_market_classifications",
    )
    op.drop_table("polymarket_market_classifications")
    op.drop_index("ix_entity_mentions_resolution_status", table_name="entity_mentions")
    op.drop_index("ix_entity_mentions_entity_id", table_name="entity_mentions")
    op.drop_table("entity_mentions")
    op.drop_index("ix_entity_relationships_object", table_name="entity_relationships")
    op.drop_index("ix_entity_relationships_subject", table_name="entity_relationships")
    op.drop_table("entity_relationships")
    op.drop_index("ix_entity_roles_entity_id", table_name="entity_roles")
    op.drop_table("entity_roles")
    op.drop_index(
        "ix_entity_source_mappings_entity_id",
        table_name="entity_source_mappings",
    )
    op.drop_table("entity_source_mappings")
    op.drop_index(
        "ix_entity_aliases_normalized_alias",
        table_name="entity_aliases",
    )
    op.drop_table("entity_aliases")
    op.drop_index("ix_entities_identity_status", table_name="entities")
    op.drop_index("ix_entities_normalized_name", table_name="entities")
    op.drop_table("entities")
    op.drop_table("entity_bank_versions")
    op.drop_column("polymarket_markets", "group_item_threshold")
    op.drop_column("polymarket_markets", "group_item_title")
    op.drop_column("polymarket_events", "game_id")
    op.drop_column("news_enrichments", "entity_extractor_version")
