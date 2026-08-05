"""Transactional PostgreSQL writes for Kalshi ingestion."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import case, func, or_, select
from sqlalchemy.dialects.postgresql import insert

from src.db.engine import DatabaseResources, create_database_resources
from src.db.models import (
    ingest_cursors,
    kalshi_current_order_books,
    kalshi_event_versions,
    kalshi_events,
    kalshi_market_versions,
    kalshi_markets,
    kalshi_series,
    kalshi_trades,
    raw_ingest_objects,
)
from src.db.repository import raw_object_values

KALSHI_CURSOR_SOURCE = "kalshi"
STRUCTURE_CURSOR_STREAM = "structure"
ORDER_BOOKS_CURSOR_STREAM = "order_books"
TRADES_CURSOR_STREAM = "trades"

EVENT_STATE_COLUMNS = (
    "series_ticker",
    "title",
    "sub_title",
    "category",
    "mutually_exclusive",
    "collateral_return_type",
    "strike_date",
    "strike_period",
    "settlement_sources",
    "product_metadata",
    "available_on_brokers",
    "latest_raw_ingest_run_id",
    "current_content_sha256",
    "last_observed_at",
)
MARKET_STATE_COLUMNS = (
    "event_ticker",
    "series_ticker",
    "market_type",
    "title",
    "yes_sub_title",
    "no_sub_title",
    "rules_primary",
    "rules_secondary",
    "status",
    "result",
    "settlement_value",
    "settlement_ts",
    "expiration_value",
    "can_close_early",
    "early_close_condition",
    "open_time",
    "close_time",
    "expected_expiration_time",
    "latest_expiration_time",
    "occurrence_datetime",
    "created_time",
    "updated_time",
    "settlement_timer_seconds",
    "strike_type",
    "floor_strike",
    "cap_strike",
    "functional_strike",
    "custom_strike",
    "price_level_structure",
    "price_ranges",
    "notional_value",
    "is_provisional",
    "primary_participant_key",
    "mve_collection_ticker",
    "yes_bid",
    "yes_ask",
    "no_bid",
    "no_ask",
    "last_price",
    "previous_price",
    "yes_bid_size",
    "yes_ask_size",
    "volume",
    "volume_24h",
    "open_interest",
    "latest_raw_ingest_run_id",
    "current_content_sha256",
    "last_observed_at",
)
MARKET_DECIMAL_FIELDS = (
    "settlement_value",
    "floor_strike",
    "cap_strike",
    "notional_value",
    "yes_bid",
    "yes_ask",
    "no_bid",
    "no_ask",
    "last_price",
    "previous_price",
    "yes_bid_size",
    "yes_ask_size",
    "volume",
    "volume_24h",
    "open_interest",
)
MARKET_TIMESTAMP_FIELDS = (
    "settlement_ts",
    "open_time",
    "close_time",
    "expected_expiration_time",
    "latest_expiration_time",
    "occurrence_datetime",
    "created_time",
    "updated_time",
)
BOOK_DECIMAL_FIELDS = (
    "depth_usdc",
    "best_bid",
    "best_ask",
    "midpoint",
    "spread",
    "bid_captured_notional",
    "bid_captured_shares",
    "bid_total_notional",
    "ask_captured_notional",
    "ask_captured_shares",
    "ask_total_notional",
)


def should_append_version(prior_hash: str | None, new_hash: str) -> bool:
    """Compare with only the immediately prior state so reversions are retained."""
    return prior_hash != new_hash


def _timestamp(value: str | datetime | None, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    else:
        raise ValueError(f"{field_name} is required")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_timestamp(value: Any) -> datetime | None:
    return _timestamp(value, "timestamp") if value else None


def _optional_decimal(value: Any) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def series_values(record: dict[str, Any], ingest_run_id: str) -> dict[str, Any]:
    observed_at = _timestamp(record.get("observed_at"), "series.observed_at")
    return {
        "series_ticker": record["series_ticker"],
        "title": record.get("title"),
        "category": record.get("category"),
        "frequency": record.get("frequency"),
        "tags": record.get("tags") or [],
        "fee_type": record.get("fee_type"),
        "fee_multiplier": _optional_decimal(record.get("fee_multiplier")),
        "settlement_sources": record.get("settlement_sources") or [],
        "contract_url": record.get("contract_url"),
        "latest_raw_ingest_run_id": ingest_run_id,
        "first_observed_at": observed_at,
        "last_observed_at": observed_at,
    }


def event_values(record: dict[str, Any], ingest_run_id: str) -> dict[str, Any]:
    observed_at = _timestamp(record.get("observed_at"), "event.observed_at")
    return {
        "event_ticker": record["event_ticker"],
        "series_ticker": record["series_ticker"],
        "title": record["title"],
        "sub_title": record.get("sub_title"),
        "category": record.get("category"),
        "mutually_exclusive": record.get("mutually_exclusive"),
        "collateral_return_type": record.get("collateral_return_type"),
        "strike_date": _optional_timestamp(record.get("strike_date")),
        "strike_period": record.get("strike_period"),
        "settlement_sources": record.get("settlement_sources") or [],
        "product_metadata": record.get("product_metadata"),
        "available_on_brokers": record.get("available_on_brokers"),
        "latest_raw_ingest_run_id": ingest_run_id,
        "current_content_sha256": record["content_sha256"],
        "first_observed_at": observed_at,
        "last_observed_at": observed_at,
    }


def market_values(record: dict[str, Any], ingest_run_id: str) -> dict[str, Any]:
    observed_at = _timestamp(record.get("observed_at"), "market.observed_at")
    values: dict[str, Any] = {
        "ticker": record["ticker"],
        "latest_raw_ingest_run_id": ingest_run_id,
        "current_content_sha256": record["content_sha256"],
        "first_observed_at": observed_at,
        "last_observed_at": observed_at,
    }
    for name in MARKET_STATE_COLUMNS:
        if name in (
            "latest_raw_ingest_run_id",
            "current_content_sha256",
            "last_observed_at",
        ):
            continue
        values[name] = record.get(name)
    for name in MARKET_DECIMAL_FIELDS:
        values[name] = _optional_decimal(values[name])
    for name in MARKET_TIMESTAMP_FIELDS:
        values[name] = _optional_timestamp(values[name])
    return values


class KalshiStructureRepository:
    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> KalshiStructureRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_checkpoint(self) -> dict[str, Any]:
        with self.resources.engine.connect() as connection:
            row = (
                connection.execute(
                    ingest_cursors.select().where(
                        ingest_cursors.c.source == KALSHI_CURSOR_SOURCE,
                        ingest_cursors.c.stream == STRUCTURE_CURSOR_STREAM,
                    )
                )
                .mappings()
                .one_or_none()
            )
        return dict(row) if row else {}

    def _upsert_market(
        self,
        connection: Any,
        market_row: dict[str, Any],
        ingest_run_id: str,
    ) -> None:
        prior_hash = connection.scalar(
            select(kalshi_market_versions.c.content_sha256)
            .where(
                kalshi_market_versions.c.ticker == market_row["ticker"],
                kalshi_market_versions.c.observed_at < market_row["last_observed_at"],
            )
            .order_by(kalshi_market_versions.c.observed_at.desc())
            .limit(1)
        )
        market_insert = insert(kalshi_markets).values(**market_row)
        market_is_newer = (
            market_insert.excluded.last_observed_at >= kalshi_markets.c.last_observed_at
        )
        connection.execute(
            market_insert.on_conflict_do_update(
                index_elements=[kalshi_markets.c.ticker],
                set_={
                    **{
                        name: case(
                            (market_is_newer, getattr(market_insert.excluded, name)),
                            else_=getattr(kalshi_markets.c, name),
                        )
                        for name in MARKET_STATE_COLUMNS
                    },
                    "first_observed_at": func.least(
                        kalshi_markets.c.first_observed_at,
                        market_insert.excluded.first_observed_at,
                    ),
                },
            )
        )
        if should_append_version(prior_hash, market_row["current_content_sha256"]):
            connection.execute(
                insert(kalshi_market_versions)
                .values(
                    ticker=market_row["ticker"],
                    observed_at=market_row["last_observed_at"],
                    raw_ingest_run_id=ingest_run_id,
                    content_sha256=market_row["current_content_sha256"],
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        kalshi_market_versions.c.ticker,
                        kalshi_market_versions.c.observed_at,
                    ]
                )
            )

    def persist_records(self, envelope: dict[str, Any]) -> None:
        ingest_run_id = envelope["ingest_run_id"]
        cycle_observed_at = _timestamp(envelope["ingested_at"], "envelope.ingested_at")
        records = envelope["records"]
        seen_event_tickers: set[str] = set()
        seen_market_tickers: set[str] = set()
        with self.resources.engine.begin() as connection:
            connection.execute(
                insert(raw_ingest_objects)
                .values(**raw_object_values(envelope))
                .on_conflict_do_nothing(index_elements=[raw_ingest_objects.c.ingest_run_id])
            )

            for series in records.get("series", []):
                values = series_values(series, ingest_run_id)
                series_insert = insert(kalshi_series).values(**values)
                series_is_newer = (
                    series_insert.excluded.last_observed_at
                    >= kalshi_series.c.last_observed_at
                )
                connection.execute(
                    series_insert.on_conflict_do_update(
                        index_elements=[kalshi_series.c.series_ticker],
                        set_={
                            **{
                                column.name: case(
                                    (
                                        series_is_newer,
                                        getattr(series_insert.excluded, column.name),
                                    ),
                                    else_=getattr(kalshi_series.c, column.name),
                                )
                                for column in kalshi_series.c
                                if column.name not in ("series_ticker", "first_observed_at")
                            },
                            "first_observed_at": func.least(
                                kalshi_series.c.first_observed_at,
                                series_insert.excluded.first_observed_at,
                            ),
                        },
                    )
                )

            for event in records.get("events", []):
                values = event_values(event, ingest_run_id)
                seen_event_tickers.add(values["event_ticker"])
                prior_hash = connection.scalar(
                    select(kalshi_event_versions.c.content_sha256)
                    .where(
                        kalshi_event_versions.c.event_ticker == values["event_ticker"],
                        kalshi_event_versions.c.observed_at < values["last_observed_at"],
                    )
                    .order_by(kalshi_event_versions.c.observed_at.desc())
                    .limit(1)
                )
                event_insert = insert(kalshi_events).values(**values)
                event_is_newer = (
                    event_insert.excluded.last_observed_at
                    >= kalshi_events.c.last_observed_at
                )
                connection.execute(
                    event_insert.on_conflict_do_update(
                        index_elements=[kalshi_events.c.event_ticker],
                        set_={
                            **{
                                name: case(
                                    (event_is_newer, getattr(event_insert.excluded, name)),
                                    else_=getattr(kalshi_events.c, name),
                                )
                                for name in EVENT_STATE_COLUMNS
                            },
                            "first_observed_at": func.least(
                                kalshi_events.c.first_observed_at,
                                event_insert.excluded.first_observed_at,
                            ),
                        },
                    )
                )
                if should_append_version(prior_hash, values["current_content_sha256"]):
                    connection.execute(
                        insert(kalshi_event_versions)
                        .values(
                            event_ticker=values["event_ticker"],
                            observed_at=values["last_observed_at"],
                            raw_ingest_run_id=ingest_run_id,
                            content_sha256=values["current_content_sha256"],
                        )
                        .on_conflict_do_nothing(
                            index_elements=[
                                kalshi_event_versions.c.event_ticker,
                                kalshi_event_versions.c.observed_at,
                            ]
                        )
                    )

                for market in event["markets"]:
                    market_row = market_values(market, ingest_run_id)
                    seen_market_tickers.add(market_row["ticker"])
                    self._upsert_market(connection, market_row, ingest_run_id)

            for market in records.get("settled_markets", []):
                market_row = market_values(market, ingest_run_id)
                seen_market_tickers.add(market_row["ticker"])
                self._upsert_market(connection, market_row, ingest_run_id)

            if seen_event_tickers:
                connection.execute(
                    kalshi_events.update()
                    .where(
                        kalshi_events.c.event_ticker.in_(sorted(seen_event_tickers)),
                        kalshi_events.c.missing_since.is_not(None),
                        kalshi_events.c.missing_since <= cycle_observed_at,
                    )
                    .values(missing_since=None)
                )
            if seen_market_tickers:
                connection.execute(
                    kalshi_markets.update()
                    .where(
                        kalshi_markets.c.ticker.in_(sorted(seen_market_tickers)),
                        kalshi_markets.c.missing_since.is_not(None),
                        kalshi_markets.c.missing_since <= cycle_observed_at,
                    )
                    .values(missing_since=None)
                )

            missing_events = kalshi_events.update().where(
                kalshi_events.c.missing_since.is_(None),
                kalshi_events.c.last_observed_at <= cycle_observed_at,
            )
            if seen_event_tickers:
                missing_events = missing_events.where(
                    kalshi_events.c.event_ticker.not_in(sorted(seen_event_tickers))
                )
            connection.execute(missing_events.values(missing_since=cycle_observed_at))

            missing_markets = kalshi_markets.update().where(
                kalshi_markets.c.missing_since.is_(None),
                kalshi_markets.c.last_observed_at <= cycle_observed_at,
            )
            if seen_market_tickers:
                missing_markets = missing_markets.where(
                    kalshi_markets.c.ticker.not_in(sorted(seen_market_tickers))
                )
            connection.execute(missing_markets.values(missing_since=cycle_observed_at))

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        _finalize_cursor(
            self.resources,
            stream=STRUCTURE_CURSOR_STREAM,
            checkpoint=checkpoint,
            last_structural_sha256=checkpoint.get("last_structural_sha256"),
            since_id=checkpoint.get("since_id"),
        )


class KalshiOrderBookRepository:
    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> KalshiOrderBookRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_open_market_tickers(self) -> list[str]:
        with self.resources.engine.connect() as connection:
            rows = connection.execute(
                select(kalshi_markets.c.ticker)
                .where(
                    kalshi_markets.c.status == "active",
                    kalshi_markets.c.missing_since.is_(None),
                )
                .order_by(kalshi_markets.c.ticker)
            )
        return [str(row.ticker) for row in rows]

    def persist_records(self, envelope: dict[str, Any]) -> None:
        rows: list[dict[str, Any]] = []
        for record in envelope["records"]:
            row = {
                **record,
                "observed_at": _timestamp(record["observed_at"], "order_book.observed_at"),
                "raw_ingest_run_id": envelope["ingest_run_id"],
            }
            for field_name in BOOK_DECIMAL_FIELDS:
                value = row[field_name]
                row[field_name] = Decimal(str(value)) if value is not None else None
            rows.append(row)

        with self.resources.engine.begin() as connection:
            connection.execute(
                insert(raw_ingest_objects)
                .values(**raw_object_values(envelope))
                .on_conflict_do_nothing(index_elements=[raw_ingest_objects.c.ingest_run_id])
            )
            for offset in range(0, len(rows), 500):
                current_rows = rows[offset : offset + 500]
                current_insert = insert(kalshi_current_order_books).values(current_rows)
                connection.execute(
                    current_insert.on_conflict_do_update(
                        index_elements=[kalshi_current_order_books.c.ticker],
                        set_={
                            column.name: getattr(current_insert.excluded, column.name)
                            for column in kalshi_current_order_books.c
                            if column.name != "ticker"
                        },
                        where=(
                            current_insert.excluded.observed_at
                            >= kalshi_current_order_books.c.observed_at
                        ),
                    )
                )

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        _finalize_cursor(
            self.resources,
            stream=ORDER_BOOKS_CURSOR_STREAM,
            checkpoint=checkpoint,
            last_structural_sha256=None,
            since_id=checkpoint.get("since_id"),
        )


class KalshiTradesRepository:
    """Append-only persistence for executed Kalshi trades."""

    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> KalshiTradesRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_checkpoint(self) -> dict[str, Any]:
        with self.resources.engine.connect() as connection:
            row = (
                connection.execute(
                    ingest_cursors.select().where(
                        ingest_cursors.c.source == KALSHI_CURSOR_SOURCE,
                        ingest_cursors.c.stream == TRADES_CURSOR_STREAM,
                    )
                )
                .mappings()
                .one_or_none()
            )
        return dict(row) if row else {}

    def load_tracked_tickers(self, *, missing_cutoff: datetime) -> list[str]:
        """Known markets, plus a grace window after they leave the open feed so
        the final trades before close are still collected."""
        with self.resources.engine.connect() as connection:
            rows = connection.execute(
                select(kalshi_markets.c.ticker)
                .where(
                    or_(
                        kalshi_markets.c.missing_since.is_(None),
                        kalshi_markets.c.missing_since > missing_cutoff,
                    ),
                )
                .order_by(kalshi_markets.c.ticker)
            )
            return [str(row.ticker) for row in rows]

    def persist_records(self, envelope: dict[str, Any]) -> None:
        rows: list[dict[str, Any]] = []
        for record in envelope["records"]:
            rows.append(
                {
                    "trade_id": record["trade_id"],
                    "ticker": record["ticker"],
                    "count": Decimal(record["count"]),
                    "yes_price": Decimal(record["yes_price"]),
                    "no_price": Decimal(record["no_price"]),
                    "taker_outcome_side": record["taker_outcome_side"],
                    "taker_book_side": record["taker_book_side"],
                    "is_block_trade": record["is_block_trade"],
                    "traded_at": _timestamp(record["traded_at"], "trade.traded_at"),
                    "raw_ingest_run_id": envelope["ingest_run_id"],
                    "observed_at": _timestamp(record["observed_at"], "trade.observed_at"),
                }
            )
        with self.resources.engine.begin() as connection:
            connection.execute(
                insert(raw_ingest_objects)
                .values(**raw_object_values(envelope))
                .on_conflict_do_nothing(index_elements=[raw_ingest_objects.c.ingest_run_id])
            )
            for offset in range(0, len(rows), 500):
                connection.execute(
                    insert(kalshi_trades)
                    .values(rows[offset : offset + 500])
                    .on_conflict_do_nothing(index_elements=[kalshi_trades.c.trade_id])
                )

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        _finalize_cursor(
            self.resources,
            stream=TRADES_CURSOR_STREAM,
            checkpoint=checkpoint,
            last_structural_sha256=None,
            since_id=checkpoint.get("since_id"),
        )


def _finalize_cursor(
    resources: DatabaseResources,
    *,
    stream: str,
    checkpoint: dict[str, Any],
    last_structural_sha256: str | None,
    since_id: str | None,
) -> None:
    with resources.engine.begin() as connection:
        cursor_insert = insert(ingest_cursors).values(
            source=KALSHI_CURSOR_SOURCE,
            stream=stream,
            query_fingerprint=checkpoint["query_fingerprint"],
            last_structural_sha256=last_structural_sha256,
            since_id=since_id,
            updated_at=_timestamp(checkpoint["updated_at"], "checkpoint.updated_at"),
            last_successful_poll_at=_timestamp(
                checkpoint["last_successful_poll_at"],
                "checkpoint.last_successful_poll_at",
            ),
        )
        connection.execute(
            cursor_insert.on_conflict_do_update(
                index_elements=[
                    ingest_cursors.c.source,
                    ingest_cursors.c.stream,
                ],
                set_={
                    "query_fingerprint": cursor_insert.excluded.query_fingerprint,
                    "last_structural_sha256": (
                        cursor_insert.excluded.last_structural_sha256
                    ),
                    "since_id": cursor_insert.excluded.since_id,
                    "updated_at": cursor_insert.excluded.updated_at,
                    "last_successful_poll_at": (
                        cursor_insert.excluded.last_successful_poll_at
                    ),
                },
                where=(
                    cursor_insert.excluded.last_successful_poll_at
                    >= ingest_cursors.c.last_successful_poll_at
                ),
            )
        )
