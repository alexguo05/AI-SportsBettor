"""Relational schema for raw ingestion objects and normalized X news."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
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
    Column("game_id", String(128)),
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
    Column("group_item_title", Text),
    Column("group_item_threshold", Text),
    Column("sports_market_type", String(64)),
    Column("line", Numeric),
    Column("active", Boolean, nullable=False),
    Column("closed", Boolean, nullable=False),
    Column("accepting_orders", Boolean, nullable=False),
    Column("enable_order_book", Boolean, nullable=False),
    Column("outcome_prices", JSONB),
    Column("uma_resolution_status", String(64)),
    Column("winning_outcome_index", Integer),
    Column("closed_time", DateTime(timezone=True)),
    Column("resolution_observed_at", DateTime(timezone=True)),
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
Index(
    "ix_polymarket_markets_uma_resolution_status",
    polymarket_markets.c.uma_resolution_status,
)

# Trades reference tokens the collector already knows, but no foreign key is
# enforced: a provider-side token mismatch must not reject an entire archived
# trade batch.
polymarket_trades = Table(
    "polymarket_trades",
    metadata,
    Column("trade_uid", String(64), primary_key=True),
    Column("token_id", String(128), nullable=False),
    Column("condition_id", String(128), nullable=False),
    Column("side", String(8), nullable=False),
    Column("outcome", Text),
    Column("outcome_index", Integer),
    Column("price", Numeric, nullable=False),
    Column("size", Numeric, nullable=False),
    Column("traded_at", DateTime(timezone=True), nullable=False),
    Column("transaction_hash", String(80)),
    Column("proxy_wallet", String(64)),
    Column(
        "raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
    Column("observed_at", DateTime(timezone=True), nullable=False),
)
Index(
    "ix_polymarket_trades_token_traded_at",
    polymarket_trades.c.token_id,
    polymarket_trades.c.traded_at,
)
Index("ix_polymarket_trades_condition_id", polymarket_trades.c.condition_id)
Index("ix_polymarket_trades_traded_at", polymarket_trades.c.traded_at)

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

kalshi_series = Table(
    "kalshi_series",
    metadata,
    Column("series_ticker", String(64), primary_key=True),
    Column("title", Text),
    Column("category", String(64)),
    Column("frequency", String(64)),
    Column("tags", JSONB, nullable=False),
    Column("fee_type", String(64)),
    Column("fee_multiplier", Numeric),
    Column("settlement_sources", JSONB, nullable=False),
    Column("contract_url", Text),
    Column(
        "latest_raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
    Column("first_observed_at", DateTime(timezone=True), nullable=False),
    Column("last_observed_at", DateTime(timezone=True), nullable=False),
)

kalshi_events = Table(
    "kalshi_events",
    metadata,
    Column("event_ticker", String(96), primary_key=True),
    Column("series_ticker", String(64), nullable=False),
    Column("title", Text, nullable=False),
    Column("sub_title", Text),
    Column("category", String(64)),
    Column("mutually_exclusive", Boolean),
    Column("collateral_return_type", String(32)),
    Column("strike_date", DateTime(timezone=True)),
    Column("strike_period", String(64)),
    Column("settlement_sources", JSONB, nullable=False),
    Column("product_metadata", JSONB),
    Column("available_on_brokers", Boolean),
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
Index("ix_kalshi_events_series_ticker", kalshi_events.c.series_ticker)
Index("ix_kalshi_events_last_observed_at", kalshi_events.c.last_observed_at)

kalshi_event_versions = Table(
    "kalshi_event_versions",
    metadata,
    Column(
        "event_ticker",
        String(96),
        ForeignKey("kalshi_events.event_ticker", ondelete="CASCADE"),
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

# No foreign key to kalshi_events: the settled-market sweep can capture a
# market whose parent event never appeared in the open feed, and that must not
# reject an archived batch.
kalshi_markets = Table(
    "kalshi_markets",
    metadata,
    Column("ticker", String(128), primary_key=True),
    Column("event_ticker", String(96), nullable=False),
    Column("series_ticker", String(64)),
    Column("market_type", String(16), nullable=False),
    Column("title", Text),
    Column("yes_sub_title", Text),
    Column("no_sub_title", Text),
    Column("rules_primary", Text),
    Column("rules_secondary", Text),
    Column("status", String(32), nullable=False),
    Column("result", String(16)),
    Column("settlement_value", Numeric),
    Column("settlement_ts", DateTime(timezone=True)),
    Column("expiration_value", Text),
    Column("can_close_early", Boolean),
    Column("early_close_condition", Text),
    Column("open_time", DateTime(timezone=True)),
    Column("close_time", DateTime(timezone=True)),
    Column("expected_expiration_time", DateTime(timezone=True)),
    Column("latest_expiration_time", DateTime(timezone=True)),
    Column("occurrence_datetime", DateTime(timezone=True)),
    Column("created_time", DateTime(timezone=True)),
    Column("updated_time", DateTime(timezone=True)),
    Column("settlement_timer_seconds", Integer),
    Column("strike_type", String(32)),
    Column("floor_strike", Numeric),
    Column("cap_strike", Numeric),
    Column("functional_strike", Text),
    Column("custom_strike", JSONB),
    Column("price_level_structure", Text),
    Column("price_ranges", JSONB),
    Column("notional_value", Numeric),
    Column("is_provisional", Boolean),
    Column("primary_participant_key", Text),
    Column("mve_collection_ticker", Text),
    Column("yes_bid", Numeric),
    Column("yes_ask", Numeric),
    Column("no_bid", Numeric),
    Column("no_ask", Numeric),
    Column("last_price", Numeric),
    Column("previous_price", Numeric),
    Column("yes_bid_size", Numeric),
    Column("yes_ask_size", Numeric),
    Column("volume", Numeric),
    Column("volume_24h", Numeric),
    Column("open_interest", Numeric),
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
Index("ix_kalshi_markets_event_ticker", kalshi_markets.c.event_ticker)
Index("ix_kalshi_markets_series_ticker", kalshi_markets.c.series_ticker)
Index("ix_kalshi_markets_status", kalshi_markets.c.status)
Index("ix_kalshi_markets_settlement_ts", kalshi_markets.c.settlement_ts)

kalshi_market_versions = Table(
    "kalshi_market_versions",
    metadata,
    Column(
        "ticker",
        String(128),
        ForeignKey("kalshi_markets.ticker", ondelete="CASCADE"),
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

# Trades reference markets the collector already knows, but no foreign key is
# enforced: a provider-side ticker mismatch must not reject an archived batch.
kalshi_trades = Table(
    "kalshi_trades",
    metadata,
    Column("trade_id", String(64), primary_key=True),
    Column("ticker", String(128), nullable=False),
    Column("count", Numeric, nullable=False),
    Column("yes_price", Numeric, nullable=False),
    Column("no_price", Numeric, nullable=False),
    Column("taker_outcome_side", String(8)),
    Column("taker_book_side", String(8)),
    Column("is_block_trade", Boolean, nullable=False),
    Column("traded_at", DateTime(timezone=True), nullable=False),
    Column(
        "raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
    Column("observed_at", DateTime(timezone=True), nullable=False),
)
Index(
    "ix_kalshi_trades_ticker_traded_at",
    kalshi_trades.c.ticker,
    kalshi_trades.c.traded_at,
)
Index("ix_kalshi_trades_traded_at", kalshi_trades.c.traded_at)

kalshi_current_order_books = Table(
    "kalshi_current_order_books",
    metadata,
    Column(
        "ticker",
        String(128),
        ForeignKey("kalshi_markets.ticker", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("observed_at", DateTime(timezone=True), nullable=False),
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
    Column(
        "raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
    ),
)
Index(
    "ix_kalshi_current_order_books_observed_at",
    kalshi_current_order_books.c.observed_at,
)

entity_bank_versions = Table(
    "entity_bank_versions",
    metadata,
    Column("version_id", String(32), primary_key=True),
    Column(
        "raw_ingest_run_id",
        String(32),
        ForeignKey("raw_ingest_objects.ingest_run_id"),
        nullable=False,
        unique=True,
    ),
    Column("source", String(32), nullable=False),
    Column("season", Integer, nullable=False),
    Column("content_sha256", String(64), nullable=False),
    Column("status", String(32), nullable=False),
    Column("source_metadata", JSONB, nullable=False),
    Column("ingested_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

entities = Table(
    "entities",
    metadata,
    Column("entity_id", String(36), primary_key=True),
    Column("entity_type", String(32), nullable=False),
    Column("canonical_name", Text, nullable=False),
    Column("normalized_name", Text, nullable=False),
    Column("identity_status", String(32), nullable=False),
    Column(
        "merged_into_entity_id",
        String(36),
        ForeignKey("entities.entity_id"),
    ),
    Column(
        "latest_bank_version_id",
        String(32),
        ForeignKey("entity_bank_versions.version_id"),
    ),
    Column("first_observed_at", DateTime(timezone=True), nullable=False),
    Column("last_observed_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("entity_type IN ('team', 'person')", name="type"),
    CheckConstraint(
        "identity_status IN ('canonical', 'provisional', 'merged', 'rejected')",
        name="identity_status",
    ),
)
Index("ix_entities_normalized_name", entities.c.normalized_name)
Index("ix_entities_identity_status", entities.c.identity_status)

entity_aliases = Table(
    "entity_aliases",
    metadata,
    Column(
        "entity_id",
        String(36),
        ForeignKey("entities.entity_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("normalized_alias", Text, primary_key=True),
    Column("source", String(32), primary_key=True),
    Column("alias", Text, nullable=False),
    Column("alias_type", String(32), nullable=False),
    Column("confidence", Numeric, nullable=False),
    Column("first_observed_at", DateTime(timezone=True), nullable=False),
    Column("last_observed_at", DateTime(timezone=True), nullable=False),
    Column("source_metadata", JSONB, nullable=False),
)
Index("ix_entity_aliases_normalized_alias", entity_aliases.c.normalized_alias)

entity_source_mappings = Table(
    "entity_source_mappings",
    metadata,
    Column("provider", String(32), primary_key=True),
    Column("source_entity_type", String(32), primary_key=True),
    Column("source_entity_id", String(128), primary_key=True),
    Column(
        "entity_id",
        String(36),
        ForeignKey("entities.entity_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("first_observed_at", DateTime(timezone=True), nullable=False),
    Column("last_observed_at", DateTime(timezone=True), nullable=False),
    Column("source_metadata", JSONB, nullable=False),
)
Index("ix_entity_source_mappings_entity_id", entity_source_mappings.c.entity_id)

entity_roles = Table(
    "entity_roles",
    metadata,
    Column("role_id", String(36), primary_key=True),
    Column(
        "entity_id",
        String(36),
        ForeignKey("entities.entity_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("role", String(32), nullable=False),
    Column("source", String(32), nullable=False),
    Column("valid_from", DateTime(timezone=True)),
    Column("valid_to", DateTime(timezone=True)),
    Column("confidence", Numeric, nullable=False),
    Column("evidence", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint(
        "entity_id",
        "role",
        "source",
        "valid_from",
        name="uq_entity_roles_identity",
    ),
)
Index("ix_entity_roles_entity_id", entity_roles.c.entity_id)

entity_relationships = Table(
    "entity_relationships",
    metadata,
    Column("relationship_id", String(36), primary_key=True),
    Column(
        "subject_entity_id",
        String(36),
        ForeignKey("entities.entity_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("predicate", String(32), nullable=False),
    Column(
        "object_entity_id",
        String(36),
        ForeignKey("entities.entity_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("source", String(32), nullable=False),
    Column("source_key", String(256), nullable=False, unique=True),
    Column("valid_from", DateTime(timezone=True)),
    Column("valid_to", DateTime(timezone=True)),
    Column("confidence", Numeric, nullable=False),
    Column("evidence", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
Index("ix_entity_relationships_subject", entity_relationships.c.subject_entity_id)
Index("ix_entity_relationships_object", entity_relationships.c.object_entity_id)

entity_mentions = Table(
    "entity_mentions",
    metadata,
    Column("mention_id", String(36), primary_key=True),
    Column("news_id", String(128), ForeignKey("news_events.news_id", ondelete="CASCADE")),
    Column(
        "polymarket_event_id",
        String(128),
        ForeignKey("polymarket_events.event_id", ondelete="CASCADE"),
    ),
    Column(
        "polymarket_market_id",
        String(128),
        ForeignKey("polymarket_markets.market_id", ondelete="CASCADE"),
    ),
    Column("entity_id", String(36), ForeignKey("entities.entity_id")),
    Column("mention_text", Text, nullable=False),
    Column("normalized_text", Text, nullable=False),
    Column("entity_type_hint", String(32), nullable=False),
    Column("person_role_hint", String(32)),
    Column("mention_role", String(32), nullable=False),
    Column("evidence", Text, nullable=False),
    Column("source_refs", JSONB, nullable=False),
    Column("source_content_sha256", String(64), nullable=False),
    Column("extractor_version", String(64), nullable=False),
    Column("resolver_version", String(64), nullable=False),
    Column("resolution_status", String(32), nullable=False),
    Column("match_method", String(32)),
    Column("confidence", Numeric, nullable=False),
    Column("last_bank_version_id", String(32), ForeignKey("entity_bank_versions.version_id")),
    Column("candidate_entity_ids", JSONB, nullable=False),
    Column("resolution_metadata", JSONB, nullable=False),
    Column("first_observed_at", DateTime(timezone=True), nullable=False),
    Column("last_observed_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        """
        (CASE WHEN news_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN polymarket_event_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN polymarket_market_id IS NOT NULL THEN 1 ELSE 0 END) = 1
        """,
        name="one_source",
    ),
    CheckConstraint(
        "resolution_status <> 'resolved' OR entity_id IS NOT NULL",
        name="resolved_entity",
    ),
)
Index("ix_entity_mentions_entity_id", entity_mentions.c.entity_id)
Index("ix_entity_mentions_resolution_status", entity_mentions.c.resolution_status)

polymarket_market_classifications = Table(
    "polymarket_market_classifications",
    metadata,
    Column(
        "market_id",
        String(128),
        ForeignKey("polymarket_markets.market_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("source_content_sha256", String(64), nullable=False),
    Column("entity_input_sha256", String(64), nullable=False),
    Column("market_topic", String(64), nullable=False),
    Column("contract_type", String(32), nullable=False),
    Column("extractor_version", String(64), nullable=False),
    Column("confidence", Numeric, nullable=False),
    Column("classification_metadata", JSONB, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
Index(
    "ix_polymarket_market_classifications_entity_input",
    polymarket_market_classifications.c.entity_input_sha256,
)

entity_resolution_attempts = Table(
    "entity_resolution_attempts",
    metadata,
    Column("attempt_id", String(36), primary_key=True),
    Column(
        "mention_id",
        String(36),
        ForeignKey("entity_mentions.mention_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("bank_version_id", String(32), ForeignKey("entity_bank_versions.version_id")),
    Column("resolver_version", String(64), nullable=False),
    Column("provider", String(32), nullable=False),
    Column("model_name", String(128), nullable=False),
    Column("status", String(32), nullable=False),
    Column("candidate_snapshot", JSONB, nullable=False),
    Column("decision", JSONB, nullable=False),
    Column("usage", JSONB, nullable=False),
    Column("error", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
Index("ix_entity_resolution_attempts_mention_id", entity_resolution_attempts.c.mention_id)

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
    Column("entity_extractor_version", String(64)),
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

news_entity_resolution_runs = Table(
    "news_entity_resolution_runs",
    metadata,
    Column("news_id", String(128), primary_key=True),
    Column("enrichment_version", String(64), primary_key=True),
    Column("input_fingerprint", String(64), primary_key=True),
    Column("extractor_version", String(64), primary_key=True),
    Column(
        "bank_version_id",
        String(32),
        ForeignKey("entity_bank_versions.version_id"),
    ),
    Column("status", String(32), nullable=False),
    Column("mention_count", Integer, nullable=False),
    Column("failure_count", Integer, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    ForeignKeyConstraint(
        ["news_id", "enrichment_version"],
        ["news_enrichments.news_id", "news_enrichments.enrichment_version"],
        ondelete="CASCADE",
    ),
)
Index(
    "ix_news_entity_resolution_runs_status",
    news_entity_resolution_runs.c.status,
)

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

news_market_links = Table(
    "news_market_links",
    metadata,
    Column(
        "news_id",
        String(128),
        ForeignKey("news_events.news_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "market_id",
        String(128),
        ForeignKey("polymarket_markets.market_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "event_id",
        String(128),
        ForeignKey("polymarket_events.event_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("published_at", DateTime(timezone=True), nullable=False),
    Column("shared_entity_ids", JSONB, nullable=False),
    Column("shared_entity_count", Integer, nullable=False),
    Column("news_mention_roles", JSONB, nullable=False),
    Column("market_mention_roles", JSONB, nullable=False),
    Column("market_topic", String(64)),
    Column("contract_type", String(32)),
    Column("market_open_at_publish", Boolean, nullable=False),
    Column("linker_version", String(64), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
Index("ix_news_market_links_market_id", news_market_links.c.market_id)
Index("ix_news_market_links_published_at", news_market_links.c.published_at)

news_market_reactions = Table(
    "news_market_reactions",
    metadata,
    Column("news_id", String(128), primary_key=True),
    Column("market_id", String(128), primary_key=True),
    Column(
        "token_id",
        String(128),
        ForeignKey("polymarket_tokens.token_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("label_version", String(64), primary_key=True),
    Column("outcome_index", Integer),
    Column("published_at", DateTime(timezone=True), nullable=False),
    Column("baseline_midpoint", Numeric),
    Column("baseline_observed_at", DateTime(timezone=True)),
    Column("baseline_spread", Numeric),
    Column("baseline_bid_depth", Numeric),
    Column("baseline_ask_depth", Numeric),
    Column("midpoint_plus_1m", Numeric),
    Column("midpoint_plus_5m", Numeric),
    Column("midpoint_plus_30m", Numeric),
    Column("midpoint_plus_2h", Numeric),
    Column("delta_plus_1m", Numeric),
    Column("delta_plus_5m", Numeric),
    Column("delta_plus_30m", Numeric),
    Column("delta_plus_2h", Numeric),
    Column("trade_count", Integer),
    Column("trade_notional", Numeric),
    Column("snapshot_count", Integer, nullable=False),
    Column("max_gap_seconds", Numeric),
    Column("computed_at", DateTime(timezone=True), nullable=False),
    ForeignKeyConstraint(
        ["news_id", "market_id"],
        ["news_market_links.news_id", "news_market_links.market_id"],
        ondelete="CASCADE",
    ),
)
Index("ix_news_market_reactions_market_id", news_market_reactions.c.market_id)
Index("ix_news_market_reactions_published_at", news_market_reactions.c.published_at)

job_outbox = Table(
    "job_outbox",
    metadata,
    Column("job_id", String(36), primary_key=True),
    Column("job_type", String(32), nullable=False),
    Column("idempotency_key", String(256), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("status", String(16), nullable=False),
    Column("priority", Integer, nullable=False),
    Column("attempts", Integer, nullable=False),
    Column("max_attempts", Integer, nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("lease_owner", String(128)),
    Column("lease_expires_at", DateTime(timezone=True)),
    Column("last_error", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("completed_at", DateTime(timezone=True)),
    CheckConstraint(
        "status IN ('pending', 'leased', 'completed', 'dead')",
        name="status",
    ),
    UniqueConstraint("job_type", "idempotency_key", name="uq_job_outbox_identity"),
)
Index(
    "ix_job_outbox_claim",
    job_outbox.c.status,
    job_outbox.c.available_at,
    job_outbox.c.priority,
)
Index("ix_job_outbox_lease_expires_at", job_outbox.c.lease_expires_at)

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
