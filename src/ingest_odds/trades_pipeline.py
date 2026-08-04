"""Raw-first collection of executed Polymarket trades for open NFL markets.

Order-book snapshots capture standing offers, not transactions. This poller
captures the trade prints between snapshots from the public data API so price
moves can be distinguished from thin-book quote drift. Each cycle re-scans a
bounded overlap window behind the stored watermark; the deterministic trade
identity makes replays and overlap re-reads idempotent.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

import requests

from src.common.gcs import canonical_json_bytes, create_gcs_client

DATA_API_TRADES_URL = "https://data-api.polymarket.com/trades"
SCHEMA_NAME = "polymarket_data_api_trades"
SCHEMA_VERSION = 1
STORAGE_PROVIDER = "polymarket"
STORAGE_SOURCE = "data-api"
STORAGE_OBJECT = "trades"
CURSOR_STREAM = "data_api_trades"

TRADE_IDENTITY_FIELDS = (
    "transaction_hash",
    "token_id",
    "condition_id",
    "proxy_wallet",
    "side",
    "price",
    "size",
    "traded_at",
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def content_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def decimal_text(value: Any, field_name: str) -> str:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return format(parsed, "f")


@dataclass(frozen=True)
class TradesConfig:
    bucket_name: str = "ai-sports-bettor"
    poll_interval_seconds: int = 60
    market_batch_size: int = 20
    page_limit: int = 500
    max_pages_per_batch: int = 10
    overlap_seconds: int = 120
    initial_lookback_hours: int = 24
    timeout_seconds: float = 30
    max_attempts: int = 5

    def __post_init__(self) -> None:
        if self.poll_interval_seconds < 1:
            raise ValueError("trades_poll_interval_seconds must be positive")
        if not 1 <= self.market_batch_size <= 50:
            raise ValueError("trades_market_batch_size must be between 1 and 50")
        if not 1 <= self.page_limit <= 1000:
            raise ValueError("trades_page_limit must be between 1 and 1000")
        if self.max_pages_per_batch < 1:
            raise ValueError("trades_max_pages_per_batch must be positive")
        if self.overlap_seconds < 0:
            raise ValueError("trades_overlap_seconds cannot be negative")
        if self.initial_lookback_hours < 1:
            raise ValueError("trades_initial_lookback_hours must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("trades_timeout_seconds must be positive")
        if self.max_attempts < 1:
            raise ValueError("trades_max_attempts must be positive")


@dataclass
class TradesResult:
    trades: list[dict[str, Any]] = field(default_factory=list)
    request_batches: list[dict[str, Any]] = field(default_factory=list)


class TradesRepositoryProtocol(Protocol):
    def load_checkpoint(self) -> dict[str, Any]: ...

    def load_open_condition_ids(self, *, missing_cutoff: datetime) -> list[str]: ...

    def persist_records(self, envelope: dict[str, Any]) -> None: ...

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None: ...


def load_trades_config(path: Path) -> TradesConfig:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    return TradesConfig(
        bucket_name=str(payload.get("gcs_bucket", "ai-sports-bettor")),
        poll_interval_seconds=int(payload.get("trades_poll_interval_seconds", 60)),
        market_batch_size=int(payload.get("trades_market_batch_size", 20)),
        page_limit=int(payload.get("trades_page_limit", 500)),
        max_pages_per_batch=int(payload.get("trades_max_pages_per_batch", 10)),
        overlap_seconds=int(payload.get("trades_overlap_seconds", 120)),
        initial_lookback_hours=int(payload.get("trades_initial_lookback_hours", 24)),
        timeout_seconds=float(payload.get("polymarket_timeout_seconds", 30)),
        max_attempts=int(payload.get("polymarket_max_attempts", 5)),
    )


def query_fingerprint(config: TradesConfig) -> str:
    return content_sha256(
        {
            "endpoint": DATA_API_TRADES_URL,
            "schema_name": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
        }
    )


def trade_uid(identity: dict[str, Any]) -> str:
    return content_sha256({name: identity[name] for name in TRADE_IDENTITY_FIELDS})


def normalize_trade(payload: dict[str, Any], observed_at: datetime) -> dict[str, Any]:
    token_id = payload.get("asset")
    condition_id = payload.get("conditionId")
    side = payload.get("side")
    timestamp = payload.get("timestamp")
    if not token_id or not condition_id:
        raise ValueError("trade is missing asset or conditionId")
    if side not in ("BUY", "SELL"):
        raise ValueError(f"trade has an unsupported side: {side!r}")
    try:
        traded_at = datetime.fromtimestamp(int(timestamp), tz=UTC)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError("trade timestamp is invalid") from exc
    outcome_index = payload.get("outcomeIndex")
    record = {
        "token_id": str(token_id),
        "condition_id": str(condition_id),
        "side": str(side),
        "outcome": str(payload["outcome"]) if payload.get("outcome") is not None else None,
        "outcome_index": int(outcome_index) if outcome_index is not None else None,
        "price": decimal_text(payload.get("price"), "trade.price"),
        "size": decimal_text(payload.get("size"), "trade.size"),
        "traded_at": traded_at.isoformat(),
        "transaction_hash": (
            str(payload["transactionHash"]) if payload.get("transactionHash") else None
        ),
        "proxy_wallet": str(payload["proxyWallet"]) if payload.get("proxyWallet") else None,
        "observed_at": observed_at.isoformat(),
    }
    record["trade_uid"] = trade_uid(record)
    return record


class TradesClient:
    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout_seconds: float = 30,
        max_attempts: int = 5,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[float, float], float] | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.session = session or requests.Session()
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.sleep = sleep
        self.jitter = jitter or random.uniform
        self.headers = {"User-Agent": "ai-sports-bettor-polymarket-ingest/1.0"}

    def fetch_page(
        self,
        *,
        condition_ids: list[str],
        limit: int,
        offset: int,
        batch_number: int,
    ) -> tuple[list[dict[str, Any]], int]:
        params = {
            "market": ",".join(condition_ids),
            "limit": str(limit),
            "offset": str(offset),
        }
        retryable_statuses = {429, 500, 502, 503, 504}
        for attempt in range(1, self.max_attempts + 1):
            response: requests.Response | None = None
            try:
                response = self.session.get(
                    DATA_API_TRADES_URL,
                    params=params,
                    headers=self.headers,
                    timeout=self.timeout_seconds,
                )
                if response.status_code not in retryable_statuses:
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, list) or not all(
                        isinstance(trade, dict) for trade in payload
                    ):
                        raise ValueError("trades response must be a list of objects")
                    return payload, attempt
                if attempt == self.max_attempts:
                    response.raise_for_status()
            except (requests.ConnectionError, requests.Timeout):
                if attempt == self.max_attempts:
                    raise
            retry_after = 0.0
            if response is not None:
                try:
                    retry_after = float(response.headers.get("Retry-After", "0"))
                except ValueError:
                    pass
            delay = max(retry_after, min(30.0, 2.0**attempt) + self.jitter(0.0, 1.0))
            status = response.status_code if response is not None else "network error"
            print(
                f"WARNING: trades batch {batch_number} offset {offset} returned "
                f"{status}; retrying attempt {attempt + 1}/{self.max_attempts} "
                f"in {delay:.1f}s",
                file=sys.stderr,
            )
            self.sleep(delay)
        raise RuntimeError("unreachable trades retry state")


def fetch_new_trades(
    *,
    config: TradesConfig,
    client: TradesClient,
    condition_ids: list[str],
    floor: datetime,
    observed_at: datetime,
) -> TradesResult:
    """Page newest-first per batch until trades fall behind the overlap floor."""
    result = TradesResult()
    floor_epoch = int(floor.timestamp())
    seen_uids: set[str] = set()
    batches = [
        condition_ids[offset : offset + config.market_batch_size]
        for offset in range(0, len(condition_ids), config.market_batch_size)
    ]
    for batch_number, id_batch in enumerate(batches, start=1):
        pages = 0
        offset = 0
        while True:
            payload, attempts = client.fetch_page(
                condition_ids=id_batch,
                limit=config.page_limit,
                offset=offset,
                batch_number=batch_number,
            )
            pages += 1
            new_in_page = 0
            for raw_trade in payload:
                timestamp = raw_trade.get("timestamp")
                if not isinstance(timestamp, int | float) or timestamp < floor_epoch:
                    continue
                record = normalize_trade(raw_trade, observed_at)
                if record["trade_uid"] in seen_uids:
                    continue
                seen_uids.add(record["trade_uid"])
                result.trades.append(record)
                new_in_page += 1
            result.request_batches.append(
                {
                    "batch_number": batch_number,
                    "condition_id_count": len(id_batch),
                    "offset": offset,
                    "returned_trade_count": len(payload),
                    "new_trade_count": new_in_page,
                    "attempts": attempts,
                }
            )
            if not payload or len(payload) < config.page_limit:
                break
            oldest = min(trade.get("timestamp", 0) for trade in payload)
            if oldest < floor_epoch:
                break
            if pages >= config.max_pages_per_batch:
                print(
                    f"WARNING: trades batch {batch_number} hit the "
                    f"{config.max_pages_per_batch}-page safety limit before "
                    "reaching the watermark; older trades will be picked up by "
                    "the overlap window or remain in the provider history",
                    file=sys.stderr,
                )
                break
            offset += config.page_limit
    result.trades.sort(key=lambda trade: trade["traded_at"])
    return result


def build_object_path(ingested_at: datetime, ingest_run_id: str) -> str:
    utc = ingested_at.astimezone(UTC)
    return (
        f"raw/provider={STORAGE_PROVIDER}/source={STORAGE_SOURCE}/"
        f"object={STORAGE_OBJECT}/schema=v{SCHEMA_VERSION}/"
        f"date={utc:%Y-%m-%d}/hour={utc:%H}/"
        f"polymarket_trades_{ingest_run_id}.json.gz"
    )


def build_envelope(
    *,
    config: TradesConfig,
    result: TradesResult,
    watermark: datetime,
    floor: datetime,
    ingest_run_id: str,
    ingested_at: datetime,
    storage_uri: str,
) -> dict[str, Any]:
    return {
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "provider": STORAGE_PROVIDER,
        "source": STORAGE_SOURCE,
        "object_type": STORAGE_OBJECT,
        "ingest_run_id": ingest_run_id,
        "ingested_at": ingested_at.astimezone(UTC).isoformat(),
        "storage_uri": storage_uri,
        "content_sha256": content_sha256(result.trades),
        "record_count": len(result.trades),
        "request": {
            "endpoint": DATA_API_TRADES_URL,
            "page_limit": config.page_limit,
            "watermark": watermark.astimezone(UTC).isoformat(),
            "overlap_floor": floor.astimezone(UTC).isoformat(),
            "batch_count": len(result.request_batches),
            "query_fingerprint": query_fingerprint(config),
            "batches": result.request_batches,
        },
        "records": result.trades,
    }


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return gzip.compress(canonical_json_bytes(envelope), compresslevel=6, mtime=0)


def load_watermark(checkpoint: dict[str, Any], config: TradesConfig, now: datetime) -> datetime:
    since_id = checkpoint.get("since_id")
    if (
        since_id
        and checkpoint.get("query_fingerprint") == query_fingerprint(config)
    ):
        parsed = datetime.fromisoformat(str(since_id))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return now - timedelta(hours=config.initial_lookback_hours)


def run_cycle(
    *,
    config: TradesConfig,
    client: TradesClient,
    bucket: Any,
    repository: TradesRepositoryProtocol,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    cycle_started_at = (now or utc_now()).astimezone(UTC)
    condition_ids = repository.load_open_condition_ids(
        missing_cutoff=cycle_started_at - timedelta(days=1),
    )
    if not condition_ids:
        print("No open Polymarket condition IDs are available; skipping trades cycle")
        return None
    watermark = load_watermark(repository.load_checkpoint(), config, cycle_started_at)
    floor = watermark - timedelta(seconds=config.overlap_seconds)
    print(
        f"Starting trades cycle for {len(condition_ids)} markets "
        f"since {floor.isoformat()}"
    )
    result = fetch_new_trades(
        config=config,
        client=client,
        condition_ids=condition_ids,
        floor=floor,
        observed_at=cycle_started_at,
    )
    checkpoint = {
        "query_fingerprint": query_fingerprint(config),
        "since_id": cycle_started_at.isoformat(),
        "updated_at": cycle_started_at.isoformat(),
        "last_successful_poll_at": cycle_started_at.isoformat(),
    }
    if not result.trades:
        repository.finalize_cycle(checkpoint)
        print("No new trades since the watermark; advanced the checkpoint only")
        return checkpoint

    ingest_run_id = uuid.uuid4().hex
    object_path = build_object_path(cycle_started_at, ingest_run_id)
    envelope = build_envelope(
        config=config,
        result=result,
        watermark=watermark,
        floor=floor,
        ingest_run_id=ingest_run_id,
        ingested_at=cycle_started_at,
        storage_uri=f"gs://{config.bucket_name}/{object_path}",
    )
    blob = bucket.blob(object_path)
    blob.metadata = {
        "schema_name": SCHEMA_NAME,
        "schema_version": str(SCHEMA_VERSION),
        "content_sha256": envelope["content_sha256"],
        "record_count": str(envelope["record_count"]),
    }
    blob.content_encoding = "gzip"
    blob.upload_from_string(
        encode_envelope(envelope),
        content_type="application/json",
    )
    print(
        f"Uploaded {envelope['record_count']} trades to {envelope['storage_uri']}"
    )
    repository.persist_records(envelope)
    repository.finalize_cycle(checkpoint)
    print("Committed Polymarket trades and advanced the checkpoint")
    return checkpoint


def run_dry_run(
    *,
    config: TradesConfig,
    client: TradesClient,
    condition_ids: list[str],
    now: datetime | None = None,
) -> TradesResult:
    observed_at = (now or utc_now()).astimezone(UTC)
    floor = observed_at - timedelta(hours=config.initial_lookback_hours)
    result = fetch_new_trades(
        config=config,
        client=client,
        condition_ids=condition_ids,
        floor=floor,
        observed_at=observed_at,
    )
    print("DRY RUN: no GCS, PostgreSQL, or checkpoint writes will occur")
    print(
        f"Fetched {len(result.trades)} trades across "
        f"{len(result.request_batches)} request pages "
        f"since {floor.isoformat()}"
    )
    for trade in result.trades[:5]:
        print(
            f"  {trade['traded_at']} {trade['side']} {trade['size']} @ "
            f"{trade['price']} ({trade['outcome']}) token={trade['token_id'][:16]}..."
        )
    if len(result.trades) > 5:
        print(f"  ... {len(result.trades) - 5} additional trades")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--condition-id",
        action="append",
        default=[],
        help="Optional condition ID for dry-run mode; otherwise open markets are "
        "read from PostgreSQL",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of PostgreSQL condition IDs used by a dry run without "
        "--condition-id (default: 10)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one live collection cycle instead of polling continuously",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    from src.db.odds_repository import TradesRepository

    args = build_parser().parse_args(argv)
    src_dir = Path(__file__).resolve().parents[1]
    config_path = src_dir / "config" / "polymarket_config.json"
    try:
        config = load_trades_config(config_path)
        client = TradesClient(
            timeout_seconds=config.timeout_seconds,
            max_attempts=config.max_attempts,
        )
        if args.dry_run:
            if args.limit < 1:
                raise ValueError("--limit must be positive")
            condition_ids = list(dict.fromkeys(args.condition_id))
            if not condition_ids:
                repository = TradesRepository.from_environment(src_dir)
                try:
                    condition_ids = repository.load_open_condition_ids(
                        missing_cutoff=utc_now() - timedelta(days=1),
                    )[: args.limit]
                finally:
                    repository.close()
                if not condition_ids:
                    raise ValueError("PostgreSQL contains no open condition IDs")
                print(
                    f"DRY RUN: loaded {len(condition_ids)} open condition IDs "
                    "from PostgreSQL",
                    file=sys.stderr,
                )
            run_dry_run(config=config, client=client, condition_ids=condition_ids)
            return 0
    except Exception as exc:
        print(f"ERROR: failed to initialize trades ingestion: {exc}", file=sys.stderr)
        return 1

    repository: TradesRepository | None = None
    try:
        gcs_client = create_gcs_client(src_dir)
        bucket = gcs_client.bucket(config.bucket_name)
        repository = TradesRepository.from_environment(src_dir)
    except Exception as exc:
        if repository:
            repository.close()
        print(f"ERROR: failed to initialize trades storage: {exc}", file=sys.stderr)
        return 1

    print(
        "Starting Polymarket trades poller. "
        f"Interval: {config.poll_interval_seconds}s. "
        f"Batch size: {config.market_batch_size} markets. "
        f"Bucket: {config.bucket_name}"
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
                print(f"ERROR: trades cycle failed: {exc}", file=sys.stderr)
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(config.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0
    finally:
        repository.close()
