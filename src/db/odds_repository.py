"""Transactional PostgreSQL writes for Polymarket discovery."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import case, func, or_, select
from sqlalchemy.dialects.postgresql import insert

from src.db.engine import DatabaseResources, create_database_resources
from src.db.models import (
    ingest_cursors,
    polymarket_current_order_books,
    polymarket_event_versions,
    polymarket_events,
    polymarket_market_versions,
    polymarket_markets,
    polymarket_tokens,
    polymarket_trades,
    raw_ingest_objects,
)
from src.db.repository import raw_object_values
from src.entity_bank.prompt import EXTRACTOR_VERSION
from src.jobs.repository import RESOLVE_MARKET, enqueue_job

POLYMARKET_CURSOR_SOURCE = "polymarket"
POLYMARKET_CURSOR_STREAM = "gamma_nfl_events"
RESOLUTION_CURSOR_STREAM = "gamma_event_resolutions"
TRADES_CURSOR_STREAM = "data_api_trades"


def should_append_version(prior_hash: str | None, new_hash: str) -> bool:
    """Compare with only the immediately prior state so reversions are retained."""
    return prior_hash != new_hash


def _timestamp(value: str | datetime | None, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError(f"{field_name} is required")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def event_values(record: dict[str, Any], ingest_run_id: str) -> dict[str, Any]:
    observed_at = _timestamp(record.get("observed_at"), "event.observed_at")
    return {
        "event_id": record["event_id"],
        "slug": record.get("slug"),
        "ticker": record.get("ticker"),
        "game_id": record.get("game_id"),
        "title": record["title"],
        "description": record.get("description"),
        "category": record.get("category"),
        "active": record["active"],
        "closed": record["closed"],
        "start_at": (
            _timestamp(record["start_at"], "event.start_at") if record.get("start_at") else None
        ),
        "end_at": (
            _timestamp(record["end_at"], "event.end_at") if record.get("end_at") else None
        ),
        "tags": record.get("tags") or [],
        "latest_raw_ingest_run_id": ingest_run_id,
        "current_content_sha256": record["content_sha256"],
        "first_observed_at": observed_at,
        "last_observed_at": observed_at,
    }


def market_values(record: dict[str, Any], ingest_run_id: str) -> dict[str, Any]:
    observed_at = _timestamp(record.get("observed_at"), "market.observed_at")
    return {
        "market_id": record["market_id"],
        "event_id": record["event_id"],
        "condition_id": record.get("condition_id"),
        "slug": record.get("slug"),
        "question": record["question"],
        "group_item_title": record.get("group_item_title"),
        "group_item_threshold": record.get("group_item_threshold"),
        "sports_market_type": record.get("sports_market_type"),
        "line": record.get("line"),
        "active": record["active"],
        "closed": record["closed"],
        "accepting_orders": record["accepting_orders"],
        "enable_order_book": record["enable_order_book"],
        "latest_raw_ingest_run_id": ingest_run_id,
        "current_content_sha256": record["content_sha256"],
        "first_observed_at": observed_at,
        "last_observed_at": observed_at,
    }


def market_resolution_values(record: dict[str, Any], ingest_run_id: str) -> dict[str, Any]:
    values = market_values(record, ingest_run_id)
    observed_at = values["last_observed_at"]
    values.update(
        {
            "outcome_prices": record.get("outcome_prices") or [],
            "uma_resolution_status": record.get("uma_resolution_status"),
            "winning_outcome_index": record.get("winning_outcome_index"),
            "closed_time": (
                _timestamp(record["closed_time"], "market.closed_time")
                if record.get("closed_time")
                else None
            ),
            "resolution_observed_at": observed_at,
        }
    )
    return values


class OddsRepository:
    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> OddsRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_checkpoint(self) -> dict[str, Any]:
        with self.resources.engine.connect() as connection:
            row = (
                connection.execute(
                    ingest_cursors.select().where(
                        ingest_cursors.c.source == POLYMARKET_CURSOR_SOURCE,
                        ingest_cursors.c.stream == POLYMARKET_CURSOR_STREAM,
                    )
                )
                .mappings()
                .one_or_none()
            )
        return dict(row) if row else {}

    def persist_records(self, envelope: dict[str, Any]) -> None:
        ingest_run_id = envelope["ingest_run_id"]
        cycle_observed_at = _timestamp(envelope["ingested_at"], "envelope.ingested_at")
        seen_event_ids: set[str] = set()
        seen_market_ids: set[str] = set()
        with self.resources.engine.begin() as connection:
            connection.execute(
                insert(raw_ingest_objects)
                .values(**raw_object_values(envelope))
                .on_conflict_do_nothing(index_elements=[raw_ingest_objects.c.ingest_run_id])
            )
            for event in envelope["records"]:
                values = event_values(event, ingest_run_id)
                event_market_hashes: list[str] = []
                seen_event_ids.add(values["event_id"])
                prior_hash = connection.scalar(
                    select(polymarket_event_versions.c.content_sha256)
                    .where(
                        polymarket_event_versions.c.event_id == values["event_id"],
                        polymarket_event_versions.c.observed_at
                        < values["last_observed_at"],
                    )
                    .order_by(polymarket_event_versions.c.observed_at.desc())
                    .limit(1)
                )
                event_insert = insert(polymarket_events).values(**values)
                event_is_newer = (
                    event_insert.excluded.last_observed_at
                    >= polymarket_events.c.last_observed_at
                )
                connection.execute(
                    event_insert.on_conflict_do_update(
                        index_elements=[polymarket_events.c.event_id],
                        set_={
                            **{
                                column.name: case(
                                    (
                                        event_is_newer,
                                        getattr(event_insert.excluded, column.name),
                                    ),
                                    else_=getattr(polymarket_events.c, column.name),
                                )
                                for column in (
                                    polymarket_events.c.slug,
                                    polymarket_events.c.ticker,
                                    polymarket_events.c.game_id,
                                    polymarket_events.c.title,
                                    polymarket_events.c.description,
                                    polymarket_events.c.category,
                                    polymarket_events.c.active,
                                    polymarket_events.c.closed,
                                    polymarket_events.c.start_at,
                                    polymarket_events.c.end_at,
                                    polymarket_events.c.tags,
                                    polymarket_events.c.latest_raw_ingest_run_id,
                                    polymarket_events.c.current_content_sha256,
                                    polymarket_events.c.last_observed_at,
                                )
                            },
                            "first_observed_at": func.least(
                                polymarket_events.c.first_observed_at,
                                event_insert.excluded.first_observed_at,
                            ),
                        },
                    )
                )
                event_needs_resolution = should_append_version(
                    prior_hash,
                    values["current_content_sha256"],
                )
                if event_needs_resolution:
                    connection.execute(
                        insert(polymarket_event_versions)
                        .values(
                            event_id=values["event_id"],
                            observed_at=values["last_observed_at"],
                            raw_ingest_run_id=ingest_run_id,
                            content_sha256=values["current_content_sha256"],
                        )
                        .on_conflict_do_nothing(
                            index_elements=[
                                polymarket_event_versions.c.event_id,
                                polymarket_event_versions.c.observed_at,
                            ]
                        )
                    )

                for market in event["markets"]:
                    market_row = market_values(market, ingest_run_id)
                    event_market_hashes.append(market_row["current_content_sha256"])
                    seen_market_ids.add(market_row["market_id"])
                    prior_market_hash = connection.scalar(
                        select(polymarket_market_versions.c.content_sha256)
                        .where(
                            polymarket_market_versions.c.market_id
                            == market_row["market_id"],
                            polymarket_market_versions.c.observed_at
                            < market_row["last_observed_at"],
                        )
                        .order_by(polymarket_market_versions.c.observed_at.desc())
                        .limit(1)
                    )
                    market_insert = insert(polymarket_markets).values(**market_row)
                    market_is_newer = (
                        market_insert.excluded.last_observed_at
                        >= polymarket_markets.c.last_observed_at
                    )
                    connection.execute(
                        market_insert.on_conflict_do_update(
                            index_elements=[polymarket_markets.c.market_id],
                            set_={
                                **{
                                    column.name: case(
                                        (
                                            market_is_newer,
                                            getattr(
                                                market_insert.excluded,
                                                column.name,
                                            ),
                                        ),
                                        else_=getattr(
                                            polymarket_markets.c,
                                            column.name,
                                        ),
                                    )
                                    for column in (
                                        polymarket_markets.c.event_id,
                                        polymarket_markets.c.condition_id,
                                        polymarket_markets.c.slug,
                                        polymarket_markets.c.question,
                                        polymarket_markets.c.group_item_title,
                                        polymarket_markets.c.group_item_threshold,
                                        polymarket_markets.c.sports_market_type,
                                        polymarket_markets.c.line,
                                        polymarket_markets.c.active,
                                        polymarket_markets.c.closed,
                                        polymarket_markets.c.accepting_orders,
                                        polymarket_markets.c.enable_order_book,
                                        polymarket_markets.c.latest_raw_ingest_run_id,
                                        polymarket_markets.c.current_content_sha256,
                                        polymarket_markets.c.last_observed_at,
                                    )
                                },
                                "first_observed_at": func.least(
                                    polymarket_markets.c.first_observed_at,
                                    market_insert.excluded.first_observed_at,
                                ),
                            },
                        )
                    )
                    market_changed = should_append_version(
                        prior_market_hash,
                        market_row["current_content_sha256"],
                    )
                    event_needs_resolution = event_needs_resolution or market_changed
                    if market_changed:
                        connection.execute(
                            insert(polymarket_market_versions)
                            .values(
                                market_id=market_row["market_id"],
                                observed_at=market_row["last_observed_at"],
                                raw_ingest_run_id=ingest_run_id,
                                content_sha256=market_row["current_content_sha256"],
                            )
                            .on_conflict_do_nothing(
                                index_elements=[
                                    polymarket_market_versions.c.market_id,
                                    polymarket_market_versions.c.observed_at,
                                ]
                            )
                        )

                    for token in market["tokens"]:
                        token_insert = insert(polymarket_tokens).values(
                            token_id=token["token_id"],
                            market_id=market_row["market_id"],
                            outcome_index=token["outcome_index"],
                            outcome=token["outcome"],
                            first_observed_at=market_row["first_observed_at"],
                            last_observed_at=market_row["last_observed_at"],
                        )
                        token_is_newer = (
                            token_insert.excluded.last_observed_at
                            >= polymarket_tokens.c.last_observed_at
                        )
                        connection.execute(
                            token_insert.on_conflict_do_update(
                                index_elements=[polymarket_tokens.c.token_id],
                                set_={
                                    **{
                                        column.name: case(
                                            (
                                                token_is_newer,
                                                getattr(
                                                    token_insert.excluded,
                                                    column.name,
                                                ),
                                            ),
                                            else_=getattr(
                                                polymarket_tokens.c,
                                                column.name,
                                            ),
                                        )
                                        for column in (
                                            polymarket_tokens.c.market_id,
                                            polymarket_tokens.c.outcome_index,
                                            polymarket_tokens.c.outcome,
                                            polymarket_tokens.c.last_observed_at,
                                        )
                                    },
                                    "first_observed_at": func.least(
                                        polymarket_tokens.c.first_observed_at,
                                        token_insert.excluded.first_observed_at,
                                    ),
                                },
                            )
                        )

                if event_needs_resolution:
                    resolution_digest = hashlib.sha256(
                        "|".join(
                            [
                                values["current_content_sha256"],
                                *sorted(event_market_hashes),
                                EXTRACTOR_VERSION,
                            ]
                        ).encode("utf-8")
                    ).hexdigest()
                    enqueue_job(
                        connection,
                        job_type=RESOLVE_MARKET,
                        idempotency_key=(
                            f"{values['event_id']}:{resolution_digest}"
                        ),
                        payload={
                            "event_id": values["event_id"],
                            "extractor_version": EXTRACTOR_VERSION,
                        },
                        priority=5,
                    )

            if seen_event_ids:
                connection.execute(
                    polymarket_events.update()
                    .where(
                        polymarket_events.c.event_id.in_(sorted(seen_event_ids)),
                        polymarket_events.c.missing_since.is_not(None),
                        polymarket_events.c.missing_since <= cycle_observed_at,
                    )
                    .values(missing_since=None)
                )
            if seen_market_ids:
                connection.execute(
                    polymarket_markets.update()
                    .where(
                        polymarket_markets.c.market_id.in_(sorted(seen_market_ids)),
                        polymarket_markets.c.missing_since.is_not(None),
                        polymarket_markets.c.missing_since <= cycle_observed_at,
                    )
                    .values(missing_since=None)
                )

            missing_events = polymarket_events.update().where(
                polymarket_events.c.missing_since.is_(None),
                polymarket_events.c.last_observed_at <= cycle_observed_at,
            )
            if seen_event_ids:
                missing_events = missing_events.where(
                    polymarket_events.c.event_id.not_in(sorted(seen_event_ids))
                )
            connection.execute(missing_events.values(missing_since=cycle_observed_at))

            missing_markets = polymarket_markets.update().where(
                polymarket_markets.c.missing_since.is_(None),
                polymarket_markets.c.last_observed_at <= cycle_observed_at,
            )
            if seen_market_ids:
                missing_markets = missing_markets.where(
                    polymarket_markets.c.market_id.not_in(sorted(seen_market_ids))
                )
            connection.execute(missing_markets.values(missing_since=cycle_observed_at))

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        with self.resources.engine.begin() as connection:
            cursor_insert = insert(ingest_cursors).values(
                source=POLYMARKET_CURSOR_SOURCE,
                stream=POLYMARKET_CURSOR_STREAM,
                query_fingerprint=checkpoint["query_fingerprint"],
                last_structural_sha256=checkpoint["last_structural_sha256"],
                since_id=None,
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
                        "since_id": None,
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


class OrderBookRepository:
    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> OrderBookRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_open_token_ids(self) -> list[str]:
        with self.resources.engine.connect() as connection:
            rows = connection.execute(
                select(polymarket_tokens.c.token_id)
                .join(
                    polymarket_markets,
                    polymarket_markets.c.market_id == polymarket_tokens.c.market_id,
                )
                .where(
                    polymarket_markets.c.active.is_(True),
                    polymarket_markets.c.closed.is_(False),
                    polymarket_markets.c.accepting_orders.is_(True),
                    polymarket_markets.c.enable_order_book.is_(True),
                    polymarket_markets.c.missing_since.is_(None),
                )
                .order_by(polymarket_tokens.c.token_id)
            )
        return [str(row.token_id) for row in rows]

    def persist_records(self, envelope: dict[str, Any]) -> None:
        decimal_fields = (
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
            "tick_size",
            "min_order_size",
            "last_trade_price",
        )
        rows: list[dict[str, Any]] = []
        for record in envelope["records"]:
            row = {
                **record,
                "source_timestamp": _timestamp(
                    record["source_timestamp"],
                    "order_book.source_timestamp",
                ),
                "observed_at": _timestamp(
                    record["observed_at"],
                    "order_book.observed_at",
                ),
                "raw_ingest_run_id": envelope["ingest_run_id"],
            }
            for field_name in decimal_fields:
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
                current_insert = insert(polymarket_current_order_books).values(current_rows)
                connection.execute(
                    current_insert.on_conflict_do_update(
                        index_elements=[polymarket_current_order_books.c.token_id],
                        set_={
                            column.name: getattr(current_insert.excluded, column.name)
                            for column in polymarket_current_order_books.c
                            if column.name != "token_id"
                        },
                        where=(
                            current_insert.excluded.observed_at
                            >= polymarket_current_order_books.c.observed_at
                        ),
                    )
                )

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        with self.resources.engine.begin() as connection:
            cursor_insert = insert(ingest_cursors).values(
                source=POLYMARKET_CURSOR_SOURCE,
                stream="clob_order_books",
                query_fingerprint=checkpoint["query_fingerprint"],
                last_structural_sha256=None,
                since_id=checkpoint["since_id"],
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


class TradesRepository:
    """Append-only persistence for executed Polymarket trades."""

    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> TradesRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_checkpoint(self) -> dict[str, Any]:
        with self.resources.engine.connect() as connection:
            row = (
                connection.execute(
                    ingest_cursors.select().where(
                        ingest_cursors.c.source == POLYMARKET_CURSOR_SOURCE,
                        ingest_cursors.c.stream == TRADES_CURSOR_STREAM,
                    )
                )
                .mappings()
                .one_or_none()
            )
        return dict(row) if row else {}

    def load_open_condition_ids(self, *, missing_cutoff: datetime) -> list[str]:
        """Order-book markets in the open feed, plus a grace window after they
        disappear so the final trades before close are still collected."""
        with self.resources.engine.connect() as connection:
            rows = connection.execute(
                select(polymarket_markets.c.condition_id)
                .where(
                    polymarket_markets.c.condition_id.is_not(None),
                    polymarket_markets.c.enable_order_book.is_(True),
                    or_(
                        polymarket_markets.c.missing_since.is_(None),
                        polymarket_markets.c.missing_since > missing_cutoff,
                    ),
                )
                .distinct()
                .order_by(polymarket_markets.c.condition_id)
            )
            return [str(row.condition_id) for row in rows]

    def persist_records(self, envelope: dict[str, Any]) -> None:
        rows: list[dict[str, Any]] = []
        for record in envelope["records"]:
            rows.append(
                {
                    "trade_uid": record["trade_uid"],
                    "token_id": record["token_id"],
                    "condition_id": record["condition_id"],
                    "side": record["side"],
                    "outcome": record["outcome"],
                    "outcome_index": record["outcome_index"],
                    "price": Decimal(record["price"]),
                    "size": Decimal(record["size"]),
                    "traded_at": _timestamp(record["traded_at"], "trade.traded_at"),
                    "transaction_hash": record["transaction_hash"],
                    "proxy_wallet": record["proxy_wallet"],
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
                    insert(polymarket_trades)
                    .values(rows[offset : offset + 500])
                    .on_conflict_do_nothing(
                        index_elements=[polymarket_trades.c.trade_uid]
                    )
                )

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        with self.resources.engine.begin() as connection:
            cursor_insert = insert(ingest_cursors).values(
                source=POLYMARKET_CURSOR_SOURCE,
                stream=TRADES_CURSOR_STREAM,
                query_fingerprint=checkpoint["query_fingerprint"],
                last_structural_sha256=None,
                since_id=checkpoint["since_id"],
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


EVENT_STATE_COLUMNS = (
    "slug",
    "ticker",
    "game_id",
    "title",
    "description",
    "category",
    "active",
    "closed",
    "start_at",
    "end_at",
    "tags",
    "latest_raw_ingest_run_id",
    "current_content_sha256",
    "last_observed_at",
)
MARKET_RESOLUTION_STATE_COLUMNS = (
    "event_id",
    "condition_id",
    "slug",
    "question",
    "group_item_title",
    "group_item_threshold",
    "sports_market_type",
    "line",
    "active",
    "closed",
    "accepting_orders",
    "enable_order_book",
    "outcome_prices",
    "uma_resolution_status",
    "winning_outcome_index",
    "closed_time",
    "resolution_observed_at",
    "latest_raw_ingest_run_id",
    "current_content_sha256",
    "last_observed_at",
)


class ResolutionRepository:
    """Persist final resolution state for markets absent from the open feed."""

    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> ResolutionRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_checkpoint(self) -> dict[str, Any]:
        with self.resources.engine.connect() as connection:
            row = (
                connection.execute(
                    ingest_cursors.select().where(
                        ingest_cursors.c.source == POLYMARKET_CURSOR_SOURCE,
                        ingest_cursors.c.stream == RESOLUTION_CURSOR_STREAM,
                    )
                )
                .mappings()
                .one_or_none()
            )
        return dict(row) if row else {}

    def load_pending_event_ids(self, *, cutoff: datetime | None) -> list[str]:
        """Events with markets whose UMA resolution has not been observed yet.

        A market becomes pending once it is either observed closed or its event
        disappears from the open discovery feed. Events missing for longer than
        the cutoff are abandoned so provider deletions cannot grow the set
        forever.
        """
        statement = (
            select(polymarket_events.c.event_id)
            .join(
                polymarket_markets,
                polymarket_markets.c.event_id == polymarket_events.c.event_id,
            )
            .where(
                polymarket_markets.c.uma_resolution_status.is_distinct_from("resolved"),
                or_(
                    polymarket_markets.c.closed.is_(True),
                    polymarket_events.c.missing_since.is_not(None),
                ),
            )
            .distinct()
            .order_by(polymarket_events.c.event_id)
        )
        if cutoff is not None:
            statement = statement.where(
                or_(
                    polymarket_events.c.missing_since.is_(None),
                    polymarket_events.c.missing_since > cutoff,
                )
            )
        with self.resources.engine.connect() as connection:
            rows = connection.execute(statement)
            return [str(row.event_id) for row in rows]

    def persist_records(self, envelope: dict[str, Any]) -> None:
        ingest_run_id = envelope["ingest_run_id"]
        with self.resources.engine.begin() as connection:
            connection.execute(
                insert(raw_ingest_objects)
                .values(**raw_object_values(envelope))
                .on_conflict_do_nothing(index_elements=[raw_ingest_objects.c.ingest_run_id])
            )
            for event in envelope["records"]:
                values = event_values(event, ingest_run_id)
                prior_hash = connection.scalar(
                    select(polymarket_event_versions.c.content_sha256)
                    .where(
                        polymarket_event_versions.c.event_id == values["event_id"],
                        polymarket_event_versions.c.observed_at
                        < values["last_observed_at"],
                    )
                    .order_by(polymarket_event_versions.c.observed_at.desc())
                    .limit(1)
                )
                event_insert = insert(polymarket_events).values(**values)
                event_is_newer = (
                    event_insert.excluded.last_observed_at
                    >= polymarket_events.c.last_observed_at
                )
                connection.execute(
                    event_insert.on_conflict_do_update(
                        index_elements=[polymarket_events.c.event_id],
                        set_={
                            **{
                                name: case(
                                    (
                                        event_is_newer,
                                        getattr(event_insert.excluded, name),
                                    ),
                                    else_=getattr(polymarket_events.c, name),
                                )
                                for name in EVENT_STATE_COLUMNS
                            },
                            "first_observed_at": func.least(
                                polymarket_events.c.first_observed_at,
                                event_insert.excluded.first_observed_at,
                            ),
                        },
                    )
                )
                if should_append_version(prior_hash, values["current_content_sha256"]):
                    connection.execute(
                        insert(polymarket_event_versions)
                        .values(
                            event_id=values["event_id"],
                            observed_at=values["last_observed_at"],
                            raw_ingest_run_id=ingest_run_id,
                            content_sha256=values["current_content_sha256"],
                        )
                        .on_conflict_do_nothing(
                            index_elements=[
                                polymarket_event_versions.c.event_id,
                                polymarket_event_versions.c.observed_at,
                            ]
                        )
                    )

                for market in event["markets"]:
                    market_row = market_resolution_values(market, ingest_run_id)
                    prior_market_hash = connection.scalar(
                        select(polymarket_market_versions.c.content_sha256)
                        .where(
                            polymarket_market_versions.c.market_id
                            == market_row["market_id"],
                            polymarket_market_versions.c.observed_at
                            < market_row["last_observed_at"],
                        )
                        .order_by(polymarket_market_versions.c.observed_at.desc())
                        .limit(1)
                    )
                    market_insert = insert(polymarket_markets).values(**market_row)
                    market_is_newer = (
                        market_insert.excluded.last_observed_at
                        >= polymarket_markets.c.last_observed_at
                    )
                    connection.execute(
                        market_insert.on_conflict_do_update(
                            index_elements=[polymarket_markets.c.market_id],
                            set_={
                                **{
                                    name: case(
                                        (
                                            market_is_newer,
                                            getattr(market_insert.excluded, name),
                                        ),
                                        else_=getattr(polymarket_markets.c, name),
                                    )
                                    for name in MARKET_RESOLUTION_STATE_COLUMNS
                                },
                                "first_observed_at": func.least(
                                    polymarket_markets.c.first_observed_at,
                                    market_insert.excluded.first_observed_at,
                                ),
                            },
                        )
                    )
                    if should_append_version(
                        prior_market_hash,
                        market_row["current_content_sha256"],
                    ):
                        connection.execute(
                            insert(polymarket_market_versions)
                            .values(
                                market_id=market_row["market_id"],
                                observed_at=market_row["last_observed_at"],
                                raw_ingest_run_id=ingest_run_id,
                                content_sha256=market_row["current_content_sha256"],
                            )
                            .on_conflict_do_nothing(
                                index_elements=[
                                    polymarket_market_versions.c.market_id,
                                    polymarket_market_versions.c.observed_at,
                                ]
                            )
                        )

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        with self.resources.engine.begin() as connection:
            cursor_insert = insert(ingest_cursors).values(
                source=POLYMARKET_CURSOR_SOURCE,
                stream=RESOLUTION_CURSOR_STREAM,
                query_fingerprint=checkpoint["query_fingerprint"],
                last_structural_sha256=checkpoint["last_structural_sha256"],
                since_id=None,
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
                        "since_id": None,
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
