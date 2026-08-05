"""Raw-first Kalshi series/event/market structure and settlement discovery.

One poller covers both discovery and resolution: the open feed returns every
event and nested market for the configured series patterns (with prices,
volume, and open interest), and an exchange-wide settled sweep with a
``min_settled_ts`` watermark captures each market's final ``result``,
``settlement_value``, and ``settlement_ts`` even after it leaves the open
feed. Kalshi therefore needs no separate resolutions collector.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

from src.common.gcs import canonical_json_bytes, create_gcs_client
from src.ingest_odds.kalshi_client import KalshiClient, load_kalshi_credentials

SCHEMA_NAME = "kalshi_structure"
SCHEMA_VERSION = 1
STORAGE_PROVIDER = "kalshi"
STORAGE_SOURCE = "trade-api"
STORAGE_OBJECT = "structure"
CURSOR_STREAM = "structure"

SERIES_PATH = "/trade-api/v2/series"
EVENTS_PATH = "/trade-api/v2/events"
MARKETS_PATH = "/trade-api/v2/markets"

EVENT_STRUCTURAL_FIELDS = (
    "event_ticker",
    "series_ticker",
    "title",
    "sub_title",
    "category",
    "mutually_exclusive",
    "collateral_return_type",
    "strike_date",
    "strike_period",
    "available_on_brokers",
    "settlement_sources",
    "product_metadata",
)
# Volatile trading fields (prices, sizes, volume, open interest, updated_time)
# are stored on the market row but excluded here so version rows only appear
# on lifecycle changes: listing edits, status transitions, and settlement.
MARKET_STRUCTURAL_FIELDS = (
    "ticker",
    "event_ticker",
    "market_type",
    "title",
    "yes_sub_title",
    "no_sub_title",
    "status",
    "result",
    "can_close_early",
    "early_close_condition",
    "expiration_value",
    "settlement_value_dollars",
    "settlement_ts",
    "occurrence_datetime",
    "expected_expiration_time",
    "latest_expiration_time",
    "open_time",
    "close_time",
    "created_time",
    "settlement_timer_seconds",
    "strike_type",
    "floor_strike",
    "cap_strike",
    "functional_strike",
    "custom_strike",
    "rules_primary",
    "rules_secondary",
    "price_level_structure",
    "price_ranges",
    "notional_value_dollars",
    "is_provisional",
    "primary_participant_key",
    "mve_collection_ticker",
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def content_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def parse_timestamp(value: Any) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def decimal_text(value: Any) -> str | None:
    """Normalize Kalshi fixed-point strings; absent/blank values become None."""
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"non-finite decimal value: {value!r}")
    return format(parsed, "f")


@dataclass(frozen=True)
class KalshiMarketsConfig:
    bucket_name: str = "ai-sports-bettor"
    base_url: str = "https://api.elections.kalshi.com"
    series_category: str = "Sports"
    series_patterns: tuple[str, ...] = ("^KXNFL", "^KXSB")
    poll_interval_seconds: int = 900
    page_size: int = 200
    max_pages_per_series: int = 25
    settled_page_limit: int = 1000
    settled_max_pages: int = 30
    settled_overlap_seconds: int = 3600
    settled_initial_lookback_hours: int = 168
    timeout_seconds: float = 30
    max_attempts: int = 5
    min_request_interval_seconds: float = 0.07

    def __post_init__(self) -> None:
        if not self.series_patterns:
            raise ValueError("kalshi_series_patterns cannot be empty")
        if not 1 <= self.page_size <= 200:
            raise ValueError("kalshi_page_size must be between 1 and 200")
        if not 1 <= self.settled_page_limit <= 1000:
            raise ValueError("kalshi_settled_page_limit must be between 1 and 1000")
        if self.poll_interval_seconds < 1:
            raise ValueError("kalshi_markets_poll_interval_seconds must be positive")
        if self.max_pages_per_series < 1 or self.settled_max_pages < 1:
            raise ValueError("kalshi page safety limits must be positive")
        if self.settled_overlap_seconds < 0:
            raise ValueError("kalshi_settled_overlap_seconds cannot be negative")
        if self.settled_initial_lookback_hours < 1:
            raise ValueError("kalshi_settled_initial_lookback_hours must be positive")

    @property
    def compiled_patterns(self) -> tuple[re.Pattern[str], ...]:
        return tuple(re.compile(pattern) for pattern in self.series_patterns)


@dataclass
class KalshiStructureResult:
    series: list[dict[str, Any]] = field(default_factory=list)
    raw_pages: list[dict[str, Any]] = field(default_factory=list)
    open_events: list[dict[str, Any]] = field(default_factory=list)
    settled_markets: list[dict[str, Any]] = field(default_factory=list)
    request_log: list[dict[str, Any]] = field(default_factory=list)


class KalshiStructureRepositoryProtocol(Protocol):
    def load_checkpoint(self) -> dict[str, Any]: ...

    def persist_records(self, envelope: dict[str, Any]) -> None: ...

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None: ...


def load_markets_config(path: Path) -> KalshiMarketsConfig:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    return KalshiMarketsConfig(
        bucket_name=str(payload.get("gcs_bucket", "ai-sports-bettor")),
        base_url=str(payload.get("kalshi_base_url", "https://api.elections.kalshi.com")),
        series_category=str(payload.get("kalshi_series_category", "Sports")),
        series_patterns=tuple(payload.get("kalshi_series_patterns", ["^KXNFL", "^KXSB"])),
        poll_interval_seconds=int(payload.get("kalshi_markets_poll_interval_seconds", 900)),
        page_size=int(payload.get("kalshi_page_size", 200)),
        max_pages_per_series=int(payload.get("kalshi_max_pages_per_series", 25)),
        settled_page_limit=int(payload.get("kalshi_settled_page_limit", 1000)),
        settled_max_pages=int(payload.get("kalshi_settled_max_pages", 30)),
        settled_overlap_seconds=int(payload.get("kalshi_settled_overlap_seconds", 3600)),
        settled_initial_lookback_hours=int(
            payload.get("kalshi_settled_initial_lookback_hours", 168)
        ),
        timeout_seconds=float(payload.get("kalshi_timeout_seconds", 30)),
        max_attempts=int(payload.get("kalshi_max_attempts", 5)),
        min_request_interval_seconds=float(
            payload.get("kalshi_min_request_interval_seconds", 0.07)
        ),
    )


def query_fingerprint(config: KalshiMarketsConfig) -> str:
    return content_sha256(
        {
            "endpoint": EVENTS_PATH,
            "series_category": config.series_category,
            "series_patterns": list(config.series_patterns),
            "schema_name": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
        }
    )


def series_matches(ticker: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    return any(pattern.search(ticker) for pattern in patterns)


def resolve_series_ticker(market_ticker: str, series_tickers: set[str]) -> str | None:
    """Longest known series ticker that prefixes the market ticker."""
    best: str | None = None
    for series_ticker in series_tickers:
        if market_ticker == series_ticker or market_ticker.startswith(series_ticker + "-"):
            if best is None or len(series_ticker) > len(best):
                best = series_ticker
    return best


def _structural_projection(payload: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {name: payload[name] for name in fields if name in payload}


def normalize_series(payload: dict[str, Any], observed_at: datetime) -> dict[str, Any] | None:
    ticker = payload.get("ticker")
    if not ticker:
        return None
    fee_multiplier = payload.get("fee_multiplier")
    return {
        "series_ticker": str(ticker),
        "title": payload.get("title"),
        "category": payload.get("category"),
        "frequency": payload.get("frequency"),
        "tags": payload.get("tags") or [],
        "fee_type": payload.get("fee_type"),
        "fee_multiplier": decimal_text(fee_multiplier) if fee_multiplier is not None else None,
        "settlement_sources": payload.get("settlement_sources") or [],
        "contract_url": payload.get("contract_url"),
        "observed_at": observed_at.isoformat(),
    }


def normalize_kalshi_market(
    payload: dict[str, Any],
    observed_at: datetime,
    series_tickers: set[str],
) -> dict[str, Any] | None:
    ticker = payload.get("ticker")
    event_ticker = payload.get("event_ticker")
    if not ticker or not event_ticker:
        return None
    structural_state = _structural_projection(payload, MARKET_STRUCTURAL_FIELDS)
    result = payload.get("result")
    return {
        "ticker": str(ticker),
        "event_ticker": str(event_ticker),
        "series_ticker": resolve_series_ticker(str(ticker), series_tickers),
        "market_type": str(payload.get("market_type") or "binary"),
        "title": payload.get("title") or None,
        "yes_sub_title": payload.get("yes_sub_title") or None,
        "no_sub_title": payload.get("no_sub_title") or None,
        "rules_primary": payload.get("rules_primary") or None,
        "rules_secondary": payload.get("rules_secondary") or None,
        "status": str(payload.get("status") or "unknown"),
        "result": str(result) if result else None,
        "settlement_value": decimal_text(payload.get("settlement_value_dollars")),
        "settlement_ts": parse_timestamp(payload.get("settlement_ts")),
        "expiration_value": payload.get("expiration_value") or None,
        "can_close_early": (
            bool(payload["can_close_early"]) if "can_close_early" in payload else None
        ),
        "early_close_condition": payload.get("early_close_condition") or None,
        "open_time": parse_timestamp(payload.get("open_time")),
        "close_time": parse_timestamp(payload.get("close_time")),
        "expected_expiration_time": parse_timestamp(payload.get("expected_expiration_time")),
        "latest_expiration_time": parse_timestamp(payload.get("latest_expiration_time")),
        "occurrence_datetime": parse_timestamp(payload.get("occurrence_datetime")),
        "created_time": parse_timestamp(payload.get("created_time")),
        "updated_time": parse_timestamp(payload.get("updated_time")),
        "settlement_timer_seconds": (
            int(payload["settlement_timer_seconds"])
            if payload.get("settlement_timer_seconds") is not None
            else None
        ),
        "strike_type": payload.get("strike_type") or None,
        "floor_strike": decimal_text(payload.get("floor_strike")),
        "cap_strike": decimal_text(payload.get("cap_strike")),
        "functional_strike": payload.get("functional_strike") or None,
        "custom_strike": payload.get("custom_strike"),
        "price_level_structure": payload.get("price_level_structure") or None,
        "price_ranges": payload.get("price_ranges"),
        "notional_value": decimal_text(payload.get("notional_value_dollars")),
        "is_provisional": (
            bool(payload["is_provisional"]) if "is_provisional" in payload else None
        ),
        "primary_participant_key": payload.get("primary_participant_key") or None,
        "mve_collection_ticker": payload.get("mve_collection_ticker") or None,
        "yes_bid": decimal_text(payload.get("yes_bid_dollars")),
        "yes_ask": decimal_text(payload.get("yes_ask_dollars")),
        "no_bid": decimal_text(payload.get("no_bid_dollars")),
        "no_ask": decimal_text(payload.get("no_ask_dollars")),
        "last_price": decimal_text(payload.get("last_price_dollars")),
        "previous_price": decimal_text(payload.get("previous_price_dollars")),
        "yes_bid_size": decimal_text(payload.get("yes_bid_size_fp")),
        "yes_ask_size": decimal_text(payload.get("yes_ask_size_fp")),
        "volume": decimal_text(payload.get("volume_fp")),
        "volume_24h": decimal_text(payload.get("volume_24h_fp")),
        "open_interest": decimal_text(payload.get("open_interest_fp")),
        "observed_at": observed_at.isoformat(),
        "content_sha256": content_sha256(structural_state),
    }


def normalize_kalshi_event(
    payload: dict[str, Any],
    observed_at: datetime,
    series_tickers: set[str],
) -> dict[str, Any] | None:
    event_ticker = payload.get("event_ticker")
    title = payload.get("title")
    if not event_ticker or not title:
        return None
    markets = [
        market
        for market_payload in payload.get("markets") or []
        if isinstance(market_payload, dict)
        and (market := normalize_kalshi_market(market_payload, observed_at, series_tickers))
        is not None
    ]
    structural_state = _structural_projection(payload, EVENT_STRUCTURAL_FIELDS)
    return {
        "event_ticker": str(event_ticker),
        "series_ticker": str(payload.get("series_ticker") or ""),
        "title": str(title),
        "sub_title": payload.get("sub_title") or None,
        "category": payload.get("category") or None,
        "mutually_exclusive": (
            bool(payload["mutually_exclusive"]) if "mutually_exclusive" in payload else None
        ),
        "collateral_return_type": payload.get("collateral_return_type") or None,
        "strike_date": parse_timestamp(payload.get("strike_date")),
        "strike_period": payload.get("strike_period") or None,
        "settlement_sources": payload.get("settlement_sources") or [],
        "product_metadata": payload.get("product_metadata"),
        "available_on_brokers": (
            bool(payload["available_on_brokers"])
            if "available_on_brokers" in payload
            else None
        ),
        "observed_at": observed_at.isoformat(),
        "content_sha256": content_sha256(structural_state),
        "markets": markets,
    }


def fetch_structure(
    *,
    config: KalshiMarketsConfig,
    client: KalshiClient,
    settled_floor: datetime,
) -> KalshiStructureResult:
    result = KalshiStructureResult()
    patterns = config.compiled_patterns

    params: dict[str, str] = {}
    if config.series_category:
        params["category"] = config.series_category
    payload, attempts = client.get_json(SERIES_PATH, params, description="series list")
    result.raw_pages.append({"endpoint": SERIES_PATH, "payload": payload})
    all_series = payload.get("series") or []
    result.series = [
        series
        for series in all_series
        if isinstance(series, dict) and series_matches(str(series.get("ticker") or ""), patterns)
    ]
    result.request_log.append(
        {
            "endpoint": SERIES_PATH,
            "series_total": len(all_series),
            "series_matched": len(result.series),
            "attempts": attempts,
        }
    )
    matched_tickers = sorted(str(series["ticker"]) for series in result.series)
    print(
        f"Matched {len(matched_tickers)} of {len(all_series)} Kalshi series "
        f"against {list(config.series_patterns)}"
    )

    for series_ticker in matched_tickers:
        cursor: str | None = None
        for page_number in range(1, config.max_pages_per_series + 1):
            event_params = {
                "series_ticker": series_ticker,
                "status": "open",
                "with_nested_markets": "true",
                "limit": str(config.page_size),
            }
            if cursor:
                event_params["cursor"] = cursor
            payload, attempts = client.get_json(
                EVENTS_PATH, event_params, description=f"events for {series_ticker}"
            )
            events = payload.get("events") or []
            if events:
                result.raw_pages.append({"endpoint": EVENTS_PATH, "payload": payload})
                result.open_events.extend(
                    event for event in events if isinstance(event, dict)
                )
            result.request_log.append(
                {
                    "endpoint": EVENTS_PATH,
                    "series_ticker": series_ticker,
                    "page_number": page_number,
                    "event_count": len(events),
                    "attempts": attempts,
                }
            )
            cursor = payload.get("cursor") or None
            if not cursor or not events:
                break
        else:
            raise RuntimeError(
                f"Kalshi events for {series_ticker} exceeded "
                f"kalshi_max_pages_per_series={config.max_pages_per_series}"
            )

    # The settled sweep runs per series: the exchange-wide feed settles tens of
    # thousands of markets per hour (mostly crypto strike ladders), so a global
    # sweep would page through megabytes of irrelevant records every cycle.
    floor_epoch = int(settled_floor.timestamp())
    for series_ticker in matched_tickers:
        cursor = None
        for page_number in range(1, config.settled_max_pages + 1):
            settled_params = {
                "series_ticker": series_ticker,
                "status": "settled",
                "min_settled_ts": str(floor_epoch),
                "limit": str(config.settled_page_limit),
            }
            if cursor:
                settled_params["cursor"] = cursor
            payload, attempts = client.get_json(
                MARKETS_PATH,
                settled_params,
                description=f"settled markets for {series_ticker}",
            )
            markets = [
                market for market in payload.get("markets") or [] if isinstance(market, dict)
            ]
            if markets:
                result.raw_pages.append({"endpoint": MARKETS_PATH, "payload": payload})
                result.settled_markets.extend(markets)
            result.request_log.append(
                {
                    "endpoint": MARKETS_PATH,
                    "series_ticker": series_ticker,
                    "page_number": page_number,
                    "market_count": len(markets),
                    "attempts": attempts,
                }
            )
            cursor = payload.get("cursor") or None
            if not cursor or not markets:
                break
        else:
            raise RuntimeError(
                f"Kalshi settled sweep for {series_ticker} exceeded "
                f"kalshi_settled_max_pages={config.settled_max_pages}; "
                "checkpoint was not advanced"
            )
    return result


def structural_fingerprint(
    events: list[dict[str, Any]],
    settled_markets: list[dict[str, Any]],
) -> str:
    graph = {
        "events": [
            {
                "event_ticker": event["event_ticker"],
                "content_sha256": event["content_sha256"],
                "markets": [
                    {
                        "ticker": market["ticker"],
                        "content_sha256": market["content_sha256"],
                    }
                    for market in sorted(event["markets"], key=lambda item: item["ticker"])
                ],
            }
            for event in sorted(events, key=lambda item: item["event_ticker"])
        ],
        "settled_markets": [
            {"ticker": market["ticker"], "content_sha256": market["content_sha256"]}
            for market in sorted(settled_markets, key=lambda item: item["ticker"])
        ],
    }
    return content_sha256(graph)


def build_object_path(ingested_at: datetime, ingest_run_id: str) -> str:
    utc = ingested_at.astimezone(UTC)
    return (
        f"raw/provider={STORAGE_PROVIDER}/source={STORAGE_SOURCE}/"
        f"object={STORAGE_OBJECT}/schema=v{SCHEMA_VERSION}/"
        f"date={utc:%Y-%m-%d}/hour={utc:%H}/"
        f"kalshi_structure_{ingest_run_id}.json.gz"
    )


def build_envelope(
    *,
    config: KalshiMarketsConfig,
    result: KalshiStructureResult,
    records: dict[str, Any],
    settled_floor: datetime,
    ingest_run_id: str,
    ingested_at: datetime,
    storage_uri: str,
) -> dict[str, Any]:
    record_count = (
        len(records["series"]) + len(records["events"]) + len(records["settled_markets"])
    )
    return {
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "provider": STORAGE_PROVIDER,
        "source": STORAGE_SOURCE,
        "object_type": STORAGE_OBJECT,
        "ingest_run_id": ingest_run_id,
        "ingested_at": ingested_at.astimezone(UTC).isoformat(),
        "storage_uri": storage_uri,
        "content_sha256": content_sha256(result.raw_pages),
        "structural_sha256": structural_fingerprint(
            records["events"], records["settled_markets"]
        ),
        "record_count": record_count,
        "request": {
            "series_category": config.series_category,
            "series_patterns": list(config.series_patterns),
            "settled_floor": settled_floor.astimezone(UTC).isoformat(),
            "query_fingerprint": query_fingerprint(config),
            "requests": result.request_log,
        },
        "records": records,
        "raw_api_responses": result.raw_pages,
    }


def archive_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    """Remove transient normalized records; raw pages remain replayable."""
    return {key: value for key, value in envelope.items() if key != "records"}


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return gzip.compress(canonical_json_bytes(envelope), compresslevel=6, mtime=0)


def load_settled_floor(
    checkpoint: dict[str, Any],
    config: KalshiMarketsConfig,
    now: datetime,
) -> datetime:
    since_id = checkpoint.get("since_id")
    if since_id and checkpoint.get("query_fingerprint") == query_fingerprint(config):
        parsed = datetime.fromisoformat(str(since_id))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC) - timedelta(seconds=config.settled_overlap_seconds)
    return now - timedelta(hours=config.settled_initial_lookback_hours)


def prepare_records(
    result: KalshiStructureResult,
    observed_at: datetime,
) -> dict[str, Any]:
    series_tickers = {
        str(series["ticker"]) for series in result.series if series.get("ticker")
    }
    series_records = [
        record
        for series in result.series
        if (record := normalize_series(series, observed_at)) is not None
    ]
    event_records: dict[str, dict[str, Any]] = {}
    for event in result.open_events:
        record = normalize_kalshi_event(event, observed_at, series_tickers)
        if record is not None:
            event_records[record["event_ticker"]] = record
    open_market_tickers = {
        market["ticker"]
        for event in event_records.values()
        for market in event["markets"]
    }
    settled_records: dict[str, dict[str, Any]] = {}
    for market in result.settled_markets:
        record = normalize_kalshi_market(market, observed_at, series_tickers)
        if record is not None and record["ticker"] not in open_market_tickers:
            settled_records[record["ticker"]] = record
    return {
        "series": series_records,
        "events": list(event_records.values()),
        "settled_markets": list(settled_records.values()),
    }


def run_cycle(
    *,
    config: KalshiMarketsConfig,
    client: KalshiClient,
    bucket: Any,
    repository: KalshiStructureRepositoryProtocol,
    now: datetime | None = None,
) -> dict[str, Any]:
    cycle_started_at = (now or utc_now()).astimezone(UTC)
    settled_floor = load_settled_floor(
        repository.load_checkpoint(), config, cycle_started_at
    )
    print(
        f"Starting Kalshi structure cycle; settled sweep floor "
        f"{settled_floor.isoformat()}"
    )
    result = fetch_structure(config=config, client=client, settled_floor=settled_floor)
    records = prepare_records(result, cycle_started_at)
    market_count = sum(len(event["markets"]) for event in records["events"])
    print(
        f"Normalized {len(records['series'])} series, {len(records['events'])} open "
        f"events, {market_count} open markets, and "
        f"{len(records['settled_markets'])} settled markets"
    )
    ingest_run_id = uuid.uuid4().hex
    object_path = build_object_path(cycle_started_at, ingest_run_id)
    envelope = build_envelope(
        config=config,
        result=result,
        records=records,
        settled_floor=settled_floor,
        ingest_run_id=ingest_run_id,
        ingested_at=cycle_started_at,
        storage_uri=f"gs://{config.bucket_name}/{object_path}",
    )
    blob = bucket.blob(object_path)
    blob.metadata = {
        "schema_name": SCHEMA_NAME,
        "schema_version": str(SCHEMA_VERSION),
        "content_sha256": envelope["content_sha256"],
        "structural_sha256": envelope["structural_sha256"],
        "record_count": str(envelope["record_count"]),
    }
    blob.content_encoding = "gzip"
    blob.upload_from_string(
        encode_envelope(archive_envelope(envelope)),
        content_type="application/json",
    )
    print(f"Uploaded Kalshi structure envelope to {envelope['storage_uri']}")
    repository.persist_records(envelope)
    checkpoint = {
        "query_fingerprint": query_fingerprint(config),
        "last_structural_sha256": envelope["structural_sha256"],
        "since_id": cycle_started_at.isoformat(),
        "updated_at": cycle_started_at.isoformat(),
        "last_successful_poll_at": cycle_started_at.isoformat(),
    }
    repository.finalize_cycle(checkpoint)
    print("Committed Kalshi structure graph and advanced the checkpoint")
    return checkpoint


def run_dry_run(
    *,
    config: KalshiMarketsConfig,
    client: KalshiClient,
    now: datetime | None = None,
) -> dict[str, Any]:
    observed_at = (now or utc_now()).astimezone(UTC)
    settled_floor = observed_at - timedelta(hours=config.settled_initial_lookback_hours)
    result = fetch_structure(config=config, client=client, settled_floor=settled_floor)
    records = prepare_records(result, observed_at)
    market_count = sum(len(event["markets"]) for event in records["events"])
    print("DRY RUN: no GCS, PostgreSQL, or checkpoint writes will occur")
    print(
        f"Fetched {len(records['series'])} series, {len(records['events'])} open "
        f"events, {market_count} open markets, "
        f"{len(records['settled_markets'])} settled markets"
    )
    for event in records["events"][:5]:
        print(f"  Event {event['event_ticker']}: {event['title']} "
              f"({len(event['markets'])} markets)")
    if len(records["events"]) > 5:
        print(f"  ... {len(records['events']) - 5} additional events")
    for market in records["settled_markets"][:5]:
        print(
            f"  Settled {market['ticker']}: result={market['result']} "
            f"value={market['settlement_value']} at {market['settlement_ts']}"
        )
    return records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one live collection cycle instead of polling continuously",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    from src.db.kalshi_repository import KalshiStructureRepository

    args = build_parser().parse_args(argv)
    src_dir = Path(__file__).resolve().parents[1]
    config_path = src_dir / "config" / "kalshi_config.json"
    try:
        config = load_markets_config(config_path)
        client = KalshiClient(
            credentials=load_kalshi_credentials(src_dir),
            base_url=config.base_url,
            timeout_seconds=config.timeout_seconds,
            max_attempts=config.max_attempts,
            min_request_interval_seconds=config.min_request_interval_seconds,
        )
    except Exception as exc:
        print(f"ERROR: failed to initialize Kalshi structure ingestion: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        try:
            run_dry_run(config=config, client=client)
        except Exception as exc:
            print(f"ERROR: Kalshi structure dry run failed: {exc}", file=sys.stderr)
            return 1
        return 0

    repository: KalshiStructureRepository | None = None
    try:
        gcs_client = create_gcs_client(src_dir)
        bucket = gcs_client.bucket(config.bucket_name)
        repository = KalshiStructureRepository.from_environment(src_dir)
    except Exception as exc:
        if repository:
            repository.close()
        print(f"ERROR: failed to initialize Kalshi structure storage: {exc}", file=sys.stderr)
        return 1

    print(
        "Starting Kalshi structure poller. "
        f"Interval: {config.poll_interval_seconds}s. Bucket: {config.bucket_name}"
    )
    try:
        while True:
            try:
                run_cycle(
                    config=config,
                    client=client,
                    bucket=bucket,
                    repository=repository,
                )
            except Exception as exc:
                print(f"ERROR: Kalshi structure cycle failed: {exc}", file=sys.stderr)
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(config.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0
    finally:
        repository.close()
