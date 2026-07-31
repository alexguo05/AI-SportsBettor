"""Relational schema for raw ingestion objects and normalized X news."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = MetaData(naming_convention=NAMING_CONVENTION)

raw_ingest_objects = Table(
    "raw_ingest_objects",
    metadata,
    Column("ingest_run_id", String(32), primary_key=True),
    Column("provider", String(32), nullable=False),
    Column("source", String(32), nullable=False),
    Column("object_type", String(64), nullable=False),
    Column("schema_name", String(64), nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("storage_uri", Text, nullable=False, unique=True),
    Column("content_sha256", String(64), nullable=False),
    Column("record_count", Integer, nullable=False),
    Column("ingested_at", DateTime(timezone=True), nullable=False),
    Column("request_metadata", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

news_events = Table(
    "news_events",
    metadata,
    Column("news_id", String(128), primary_key=True),
    Column("source", String(32), nullable=False),
    Column("source_post_id", String(64), nullable=False),
    Column("source_url", Text, nullable=False),
    Column(
        "first_raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
    Column("author_source_user_id", String(64)),
    Column("author_username", String(64)),
    Column("author_display_name", Text),
    Column("author_verified", Boolean),
    Column("author_profile_image_url", Text),
    Column("text", Text, nullable=False),
    Column("language", String(16)),
    Column("conversation_id", String(64)),
    Column("published_at", DateTime(timezone=True), nullable=False),
    Column("first_ingested_at", DateTime(timezone=True), nullable=False),
    Column("last_ingested_at", DateTime(timezone=True), nullable=False),
    Column("possibly_sensitive", Boolean),
    Column("public_metrics", JSONB, nullable=False),
    Column("source_entities", JSONB, nullable=False),
    Column("edit_history_post_ids", JSONB, nullable=False),
    UniqueConstraint("source", "source_post_id"),
)
Index("ix_news_events_published_at", news_events.c.published_at)
Index("ix_news_events_author_username", news_events.c.author_username)

news_event_relationships = Table(
    "news_event_relationships",
    metadata,
    Column(
        "source_news_id",
        String(128),
        ForeignKey("news_events.news_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("target_news_id", String(128), primary_key=True),
    Column("target_source_post_id", String(64), nullable=False),
    Column("relationship_type", String(32), primary_key=True),
    Column("target_available", Boolean, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
Index("ix_news_event_relationships_target_news_id", news_event_relationships.c.target_news_id)

news_media = Table(
    "news_media",
    metadata,
    Column("news_id", String(128), ForeignKey("news_events.news_id"), primary_key=True),
    Column("media_key", String(128), primary_key=True),
    Column("media_type", String(32)),
    Column("source_url", Text),
    Column("preview_image_url", Text),
    Column("selected_source_url", Text),
    Column("stored_asset_kind", String(32)),
    Column("gcs_uri", Text),
    Column("content_type", String(128)),
    Column("content_sha256", String(64)),
    Column("byte_size", BigInteger),
    Column("width", Integer),
    Column("height", Integer),
    Column("duration_ms", BigInteger),
    Column("alt_text", Text),
    Column("upload_status", String(32), nullable=False),
    Column("upload_error", Text),
    Column("processing_status", String(32), nullable=False, server_default="pending"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
Index("ix_news_media_processing_status", news_media.c.processing_status)

polymarket_events = Table(
    "polymarket_events",
    metadata,
    Column("event_id", String(128), primary_key=True),
    Column("slug", Text),
    Column("ticker", Text),
    Column("title", Text, nullable=False),
    Column("description", Text),
    Column("category", String(64)),
    Column("active", Boolean, nullable=False),
    Column("closed", Boolean, nullable=False),
    Column("start_at", DateTime(timezone=True)),
    Column("end_at", DateTime(timezone=True)),
    Column("tags", JSONB, nullable=False),
    Column(
        "latest_raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
    Column("current_content_sha256", String(64), nullable=False),
    Column("first_observed_at", DateTime(timezone=True), nullable=False),
    Column("last_observed_at", DateTime(timezone=True), nullable=False),
    Column("missing_since", DateTime(timezone=True)),
)
Index("ix_polymarket_events_closed", polymarket_events.c.closed)
Index("ix_polymarket_events_last_observed_at", polymarket_events.c.last_observed_at)

polymarket_event_versions = Table(
    "polymarket_event_versions",
    metadata,
    Column(
        "event_id",
        String(128),
        ForeignKey("polymarket_events.event_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("observed_at", DateTime(timezone=True), primary_key=True),
    Column(
        "raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
    Column("content_sha256", String(64), nullable=False),
)

polymarket_markets = Table(
    "polymarket_markets",
    metadata,
    Column("market_id", String(128), primary_key=True),
    Column(
        "event_id",
        String(128),
        ForeignKey("polymarket_events.event_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("condition_id", String(128)),
    Column("slug", Text),
    Column("question", Text, nullable=False),
    Column("sports_market_type", String(64)),
    Column("line", Numeric),
    Column("active", Boolean, nullable=False),
    Column("closed", Boolean, nullable=False),
    Column("accepting_orders", Boolean, nullable=False),
    Column("enable_order_book", Boolean, nullable=False),
    Column(
        "latest_raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
    Column("current_content_sha256", String(64), nullable=False),
    Column("first_observed_at", DateTime(timezone=True), nullable=False),
    Column("last_observed_at", DateTime(timezone=True), nullable=False),
    Column("missing_since", DateTime(timezone=True)),
)
Index("ix_polymarket_markets_event_id", polymarket_markets.c.event_id)
Index("ix_polymarket_markets_closed", polymarket_markets.c.closed)
Index("ix_polymarket_markets_condition_id", polymarket_markets.c.condition_id)

polymarket_market_versions = Table(
    "polymarket_market_versions",
    metadata,
    Column(
        "market_id",
        String(128),
        ForeignKey("polymarket_markets.market_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("observed_at", DateTime(timezone=True), primary_key=True),
    Column(
        "raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
    Column("content_sha256", String(64), nullable=False),
)

polymarket_tokens = Table(
    "polymarket_tokens",
    metadata,
    Column("token_id", String(128), primary_key=True),
    Column(
        "market_id",
        String(128),
        ForeignKey("polymarket_markets.market_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("outcome_index", Integer, nullable=False),
    Column("outcome", Text, nullable=False),
    Column("first_observed_at", DateTime(timezone=True), nullable=False),
    Column("last_observed_at", DateTime(timezone=True), nullable=False),
)
Index("ix_polymarket_tokens_market_id", polymarket_tokens.c.market_id)

polymarket_price_points = Table(
    "polymarket_price_points",
    metadata,
    Column(
        "token_id",
        String(128),
        ForeignKey("polymarket_tokens.token_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("source_timestamp", DateTime(timezone=True), primary_key=True),
    Column("price", Numeric, nullable=False),
    Column("fidelity_minutes", Integer, nullable=False),
    Column("first_observed_at", DateTime(timezone=True), nullable=False),
    Column("last_observed_at", DateTime(timezone=True), nullable=False),
    Column(
        "latest_raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
)
Index(
    "ix_polymarket_price_points_source_timestamp",
    polymarket_price_points.c.source_timestamp,
)

polymarket_price_point_versions = Table(
    "polymarket_price_point_versions",
    metadata,
    Column("token_id", String(128), primary_key=True),
    Column("source_timestamp", DateTime(timezone=True), primary_key=True),
    Column("observed_at", DateTime(timezone=True), primary_key=True),
    Column("price", Numeric, nullable=False),
    Column("fidelity_minutes", Integer, nullable=False),
    Column(
        "raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
    ForeignKeyConstraint(
        ["token_id", "source_timestamp"],
        [
            "polymarket_price_points.token_id",
            "polymarket_price_points.source_timestamp",
        ],
        ondelete="CASCADE",
        deferrable=True,
        initially="DEFERRED",
    ),
)

polymarket_price_cursors = Table(
    "polymarket_price_cursors",
    metadata,
    Column(
        "token_id",
        String(128),
        ForeignKey("polymarket_tokens.token_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("query_fingerprint", String(64), nullable=False),
    Column("last_end_ts", BigInteger, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

polymarket_order_book_snapshots = Table(
    "polymarket_order_book_snapshots",
    metadata,
    Column(
        "token_id",
        String(128),
        ForeignKey("polymarket_tokens.token_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("observed_at", DateTime(timezone=True), primary_key=True),
    Column("source_timestamp", DateTime(timezone=True), nullable=False),
    Column("condition_id", String(128)),
    Column("book_hash", String(128)),
    Column("depth_usdc", Numeric, nullable=False),
    Column("best_bid", Numeric),
    Column("best_ask", Numeric),
    Column("midpoint", Numeric),
    Column("spread", Numeric),
    Column("bid_captured_notional", Numeric, nullable=False),
    Column("bid_captured_shares", Numeric, nullable=False),
    Column("bid_total_notional", Numeric, nullable=False),
    Column("bid_truncated", Boolean, nullable=False),
    Column("ask_captured_notional", Numeric, nullable=False),
    Column("ask_captured_shares", Numeric, nullable=False),
    Column("ask_total_notional", Numeric, nullable=False),
    Column("ask_truncated", Boolean, nullable=False),
    Column("tick_size", Numeric),
    Column("min_order_size", Numeric),
    Column("last_trade_price", Numeric),
    Column(
        "raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
)
Index(
    "ix_polymarket_order_book_snapshots_observed_at",
    polymarket_order_book_snapshots.c.observed_at,
)

polymarket_current_order_books = Table(
    "polymarket_current_order_books",
    metadata,
    Column(
        "token_id",
        String(128),
        ForeignKey("polymarket_tokens.token_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("observed_at", DateTime(timezone=True), nullable=False),
    Column("source_timestamp", DateTime(timezone=True), nullable=False),
    Column("condition_id", String(128)),
    Column("book_hash", String(128)),
    Column("depth_usdc", Numeric, nullable=False),
    Column("bids", JSONB, nullable=False),
    Column("asks", JSONB, nullable=False),
    Column("best_bid", Numeric),
    Column("best_ask", Numeric),
    Column("midpoint", Numeric),
    Column("spread", Numeric),
    Column("bid_captured_notional", Numeric, nullable=False),
    Column("bid_captured_shares", Numeric, nullable=False),
    Column("bid_total_notional", Numeric, nullable=False),
    Column("bid_truncated", Boolean, nullable=False),
    Column("ask_captured_notional", Numeric, nullable=False),
    Column("ask_captured_shares", Numeric, nullable=False),
    Column("ask_total_notional", Numeric, nullable=False),
    Column("ask_truncated", Boolean, nullable=False),
    Column("tick_size", Numeric),
    Column("min_order_size", Numeric),
    Column("last_trade_price", Numeric),
    Column(
        "raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
)
Index(
    "ix_polymarket_current_order_books_observed_at",
    polymarket_current_order_books.c.observed_at,
)

news_enrichments = Table(
    "news_enrichments",
    metadata,
    Column(
        "news_id",
        String(128),
        ForeignKey("news_events.news_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("enrichment_version", String(64), primary_key=True),
    Column("provider", String(32), nullable=False),
    Column("model_name", String(128), nullable=False),
    Column("status", String(32), nullable=False),
    Column("input_fingerprint", String(64), nullable=False),
    Column("input_manifest", JSONB, nullable=False),
    Column("information_status", String(32)),
    Column("usefulness", String(32)),
    Column("summary", Text),
    Column("classification_reason", Text),
    Column("entities", JSONB, nullable=False),
    Column("claims", JSONB, nullable=False),
    Column("usage", JSONB, nullable=False),
    Column("warnings", JSONB, nullable=False),
    Column("error", Text),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
Index("ix_news_enrichments_status", news_enrichments.c.status)
Index("ix_news_enrichments_usefulness", news_enrichments.c.usefulness)

news_enrichment_tags = Table(
    "news_enrichment_tags",
    metadata,
    Column("news_id", String(128), primary_key=True),
    Column("enrichment_version", String(64), primary_key=True),
    Column("tag", String(64), primary_key=True),
    Column("certainty", String(32), nullable=False),
    Column("source_refs", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    ForeignKeyConstraint(
        ["news_id", "enrichment_version"],
        ["news_enrichments.news_id", "news_enrichments.enrichment_version"],
        ondelete="CASCADE",
    ),
)
Index("ix_news_enrichment_tags_tag", news_enrichment_tags.c.tag)

ingest_cursors = Table(
    "ingest_cursors",
    metadata,
    Column("source", String(32), primary_key=True),
    Column("stream", String(64), primary_key=True),
    Column("query_fingerprint", String(64), nullable=False),
    Column("last_structural_sha256", String(64)),
    Column("since_id", String(64)),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("last_successful_poll_at", DateTime(timezone=True), nullable=False),
)
