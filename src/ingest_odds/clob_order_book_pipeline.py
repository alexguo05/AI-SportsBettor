"""Frequent bounded snapshots of public Polymarket CLOB order books."""

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
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

import requests

from src.common.gcs import canonical_json_bytes, create_gcs_client

CLOB_BOOKS_URL = "https://clob.polymarket.com/books"
SCHEMA_NAME = "polymarket_clob_order_books"
SCHEMA_VERSION = 1
STORAGE_PROVIDER = "polymarket"
STORAGE_SOURCE = "clob"
STORAGE_OBJECT = "order-books"
DEFAULT_DEPTH_USDC = Decimal("10000")
MAX_BATCH_SIZE = 500


def utc_now() -> datetime:
    return datetime.now(UTC)


def content_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


@dataclass(frozen=True)
class ClobOrderBookConfig:
    bucket_name: str = "ai-sports-bettor"
    poll_interval_seconds: int = 10
    depth_usdc: Decimal = DEFAULT_DEPTH_USDC
    batch_size: int = MAX_BATCH_SIZE
    timeout_seconds: float = 30
    max_attempts: int = 5

    def __post_init__(self) -> None:
        if self.poll_interval_seconds < 1:
            raise ValueError("clob_order_book_poll_interval_seconds must be positive")
        if not self.depth_usdc.is_finite() or self.depth_usdc <= 0:
            raise ValueError("clob_order_book_depth_usdc must be positive")
        if not 1 <= self.batch_size <= MAX_BATCH_SIZE:
            raise ValueError(f"clob_order_book_batch_size must be between 1 and {MAX_BATCH_SIZE}")
        if self.timeout_seconds <= 0:
            raise ValueError("clob_order_book_timeout_seconds must be positive")
        if self.max_attempts < 1:
            raise ValueError("clob_order_book_max_attempts must be positive")


@dataclass
class OrderBookResult:
    books: list[dict[str, Any]] = field(default_factory=list)
    request_batches: list[dict[str, Any]] = field(default_factory=list)


class OrderBookRepositoryProtocol(Protocol):
    def load_open_token_ids(self) -> list[str]: ...

    def persist_records(self, envelope: dict[str, Any]) -> None: ...

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None: ...


def load_config(path: Path) -> ClobOrderBookConfig:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    return ClobOrderBookConfig(
        bucket_name=str(payload.get("gcs_bucket", "ai-sports-bettor")),
        poll_interval_seconds=int(payload.get("clob_order_book_poll_interval_seconds", 10)),
        depth_usdc=Decimal(str(payload.get("clob_order_book_depth_usdc", "10000"))),
        batch_size=int(payload.get("clob_order_book_batch_size", MAX_BATCH_SIZE)),
        timeout_seconds=float(payload.get("clob_order_book_timeout_seconds", 30)),
        max_attempts=int(payload.get("clob_order_book_max_attempts", 5)),
    )


def query_fingerprint(config: ClobOrderBookConfig) -> str:
    return content_sha256(
        {
            "endpoint": CLOB_BOOKS_URL,
            "depth_usdc": decimal_text(config.depth_usdc),
            "schema_name": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
        }
    )


def _decimal(value: Any, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return parsed


def _source_timestamp(value: Any, observed_at: datetime) -> str:
    if value in (None, ""):
        return observed_at.isoformat()
    try:
        numeric = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        numeric = None
    if numeric is not None and numeric.is_finite():
        seconds = numeric / 1000 if numeric >= Decimal("100000000000") else numeric
        return datetime.fromtimestamp(float(seconds), tz=UTC).isoformat()
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("order book timestamp is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def _levels(payload: Any, side: str) -> list[tuple[Decimal, Decimal]]:
    if not isinstance(payload, list):
        raise ValueError(f"order book {side} must be a list")
    levels: list[tuple[Decimal, Decimal]] = []
    for raw_level in payload:
        if not isinstance(raw_level, dict):
            raise ValueError(f"order book {side} contains an invalid level")
        price = _decimal(raw_level.get("price"), f"{side}.price")
        size = _decimal(raw_level.get("size"), f"{side}.size")
        if not 0 < price <= 1 or size < 0:
            raise ValueError(f"order book {side} contains an invalid level")
        if size:
            levels.append((price, size))
    return sorted(levels, key=lambda level: level[0], reverse=side == "bids")


def _bounded_levels(
    levels: list[tuple[Decimal, Decimal]],
    depth_usdc: Decimal,
) -> tuple[list[dict[str, str]], Decimal, Decimal, Decimal, bool]:
    total_notional = sum((price * size for price, size in levels), Decimal())
    retained: list[dict[str, str]] = []
    captured_notional = Decimal()
    captured_shares = Decimal()
    for price, size in levels:
        retained.append({"price": decimal_text(price), "size": decimal_text(size)})
        captured_notional += price * size
        captured_shares += size
        if captured_notional >= depth_usdc:
            break
    return (
        retained,
        captured_notional,
        captured_shares,
        total_notional,
        len(retained) < len(levels),
    )


def normalize_book(
    payload: dict[str, Any],
    *,
    depth_usdc: Decimal,
    observed_at: datetime,
) -> dict[str, Any]:
    token_id = payload.get("asset_id") or payload.get("token_id")
    if not token_id:
        raise ValueError("order book is missing asset_id")
    bids = _levels(payload.get("bids"), "bids")
    asks = _levels(payload.get("asks"), "asks")
    bounded_bids = _bounded_levels(bids, depth_usdc)
    bounded_asks = _bounded_levels(asks, depth_usdc)
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    midpoint = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None

    def optional_decimal(field_name: str) -> str | None:
        value = payload.get(field_name)
        return decimal_text(_decimal(value, field_name)) if value not in (None, "") else None

    return {
        "token_id": str(token_id),
        "condition_id": str(payload["market"]) if payload.get("market") else None,
        "source_timestamp": _source_timestamp(payload.get("timestamp"), observed_at),
        "observed_at": observed_at.isoformat(),
        "book_hash": str(payload["hash"]) if payload.get("hash") else None,
        "depth_usdc": decimal_text(depth_usdc),
        "bids": bounded_bids[0],
        "asks": bounded_asks[0],
        "best_bid": decimal_text(best_bid) if best_bid is not None else None,
        "best_ask": decimal_text(best_ask) if best_ask is not None else None,
        "midpoint": decimal_text(midpoint) if midpoint is not None else None,
        "spread": decimal_text(spread) if spread is not None else None,
        "bid_captured_notional": decimal_text(bounded_bids[1]),
        "bid_captured_shares": decimal_text(bounded_bids[2]),
        "bid_total_notional": decimal_text(bounded_bids[3]),
        "bid_truncated": bounded_bids[4],
        "ask_captured_notional": decimal_text(bounded_asks[1]),
        "ask_captured_shares": decimal_text(bounded_asks[2]),
        "ask_total_notional": decimal_text(bounded_asks[3]),
        "ask_truncated": bounded_asks[4],
        "tick_size": optional_decimal("tick_size"),
        "min_order_size": optional_decimal("min_order_size"),
        "last_trade_price": optional_decimal("last_trade_price"),
    }


class ClobOrderBookClient:
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

    def fetch_batch(
        self,
        token_ids: list[str],
        *,
        batch_number: int,
    ) -> tuple[list[dict[str, Any]], int]:
        body = [{"token_id": token_id} for token_id in token_ids]
        retryable_statuses = {429, 500, 502, 503, 504}
        for attempt in range(1, self.max_attempts + 1):
            response: requests.Response | None = None
            try:
                response = self.session.post(
                    CLOB_BOOKS_URL,
                    json=body,
                    headers=self.headers,
                    timeout=self.timeout_seconds,
                )
                if response.status_code not in retryable_statuses:
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, list) or not all(
                        isinstance(book, dict) for book in payload
                    ):
                        raise ValueError("CLOB books response must be a list of objects")
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
                f"WARNING: CLOB book batch {batch_number} returned {status}; "
                f"retrying attempt {attempt + 1}/{self.max_attempts} in {delay:.1f}s",
                file=sys.stderr,
            )
            self.sleep(delay)
        raise RuntimeError("unreachable CLOB retry state")

    def fetch_books(
        self,
        *,
        token_ids: list[str],
        depth_usdc: Decimal,
        batch_size: int,
        observed_at: datetime,
        report_progress: bool = True,
    ) -> OrderBookResult:
        result = OrderBookResult()
        batches = [
            token_ids[offset : offset + batch_size]
            for offset in range(0, len(token_ids), batch_size)
        ]
        for batch_number, token_batch in enumerate(batches, start=1):
            payload, attempts = self.fetch_batch(token_batch, batch_number=batch_number)
            books_by_token = {
                str(book.get("asset_id") or book.get("token_id")): book for book in payload
            }
            missing = sorted(set(token_batch) - set(books_by_token))
            if missing:
                print(
                    f"WARNING: CLOB book batch {batch_number} omitted {len(missing)} "
                    f"requested token(s): {', '.join(missing[:3])}",
                    file=sys.stderr,
                )
            result.books.extend(
                normalize_book(
                    books_by_token[token_id],
                    depth_usdc=depth_usdc,
                    observed_at=observed_at,
                )
                for token_id in token_batch
                if token_id in books_by_token
            )
            result.request_batches.append(
                {
                    "batch_number": batch_number,
                    "token_count": len(token_batch),
                    "returned_token_count": len(token_batch) - len(missing),
                    "omitted_token_ids": missing,
                    "attempts": attempts,
                }
            )
            if report_progress:
                print(
                    f"Fetched CLOB order-book batch {batch_number}/{len(batches)} "
                    f"for {len(token_batch)} tokens"
                )
        if not result.books:
            raise ValueError("CLOB returned no order books for the requested tokens")
        return result


def build_object_path(ingested_at: datetime, ingest_run_id: str) -> str:
    utc = ingested_at.astimezone(UTC)
    return (
        f"raw/provider={STORAGE_PROVIDER}/source={STORAGE_SOURCE}/"
        f"object={STORAGE_OBJECT}/schema=v{SCHEMA_VERSION}/"
        f"date={utc:%Y-%m-%d}/hour={utc:%H}/"
        f"polymarket_order_books_{ingest_run_id}.json.gz"
    )


def build_envelope(
    *,
    config: ClobOrderBookConfig,
    result: OrderBookResult,
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
        "ingested_at": ingested_at.isoformat(),
        "storage_uri": storage_uri,
        "content_sha256": content_sha256(result.books),
        "record_count": len(result.books),
        "request": {
            "endpoint": CLOB_BOOKS_URL,
            "depth_usdc": decimal_text(config.depth_usdc),
            "batch_count": len(result.request_batches),
            "query_fingerprint": query_fingerprint(config),
            "batches": result.request_batches,
        },
        "records": result.books,
    }


def archive_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    """Return the exact bounded order-book envelope stored in GCS."""
    return {key: value for key, value in envelope.items() if not key.startswith("_")}


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return gzip.compress(canonical_json_bytes(envelope), compresslevel=6, mtime=0)


def prepare_envelope(
    *,
    config: ClobOrderBookConfig,
    client: ClobOrderBookClient,
    token_ids: list[str],
    now: datetime | None = None,
    report_progress: bool = True,
) -> dict[str, Any]:
    observed_at = (now or utc_now()).astimezone(UTC)
    result = client.fetch_books(
        token_ids=token_ids,
        depth_usdc=config.depth_usdc,
        batch_size=config.batch_size,
        observed_at=observed_at,
        report_progress=report_progress,
    )
    ingest_run_id = uuid.uuid4().hex
    object_path = build_object_path(observed_at, ingest_run_id)
    return build_envelope(
        config=config,
        result=result,
        ingest_run_id=ingest_run_id,
        ingested_at=observed_at,
        storage_uri=f"gs://{config.bucket_name}/{object_path}",
    )


def run_cycle(
    *,
    config: ClobOrderBookConfig,
    client: ClobOrderBookClient,
    bucket: Any,
    repository: OrderBookRepositoryProtocol,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    token_ids = repository.load_open_token_ids()
    if not token_ids:
        print("No open Polymarket token IDs are available; skipping order-book cycle")
        return None
    print(
        f"Starting CLOB order-book cycle for {len(token_ids)} tokens "
        f"at ${decimal_text(config.depth_usdc)} depth per side"
    )
    envelope = prepare_envelope(
        config=config,
        client=client,
        token_ids=token_ids,
        now=now,
    )
    object_path = envelope["storage_uri"].split(f"gs://{config.bucket_name}/", 1)[1]
    blob = bucket.blob(object_path)
    blob.metadata = {
        "schema_name": SCHEMA_NAME,
        "schema_version": str(SCHEMA_VERSION),
        "content_sha256": envelope["content_sha256"],
        "record_count": str(envelope["record_count"]),
        "depth_usdc": decimal_text(config.depth_usdc),
    }
    blob.content_encoding = "gzip"
    blob.upload_from_string(
        encode_envelope(archive_envelope(envelope)),
        content_type="application/json",
    )
    print(f"Uploaded {envelope['record_count']} CLOB order books to {envelope['storage_uri']}")
    repository.persist_records(envelope)
    checkpoint = {
        "query_fingerprint": query_fingerprint(config),
        "since_id": envelope["ingested_at"],
        "updated_at": envelope["ingested_at"],
        "last_successful_poll_at": envelope["ingested_at"],
    }
    repository.finalize_cycle(checkpoint)
    print("Committed CLOB order-book snapshots and advanced the checkpoint")
    return checkpoint


def run_dry_run(
    *,
    base_config: ClobOrderBookConfig,
    client: ClobOrderBookClient,
    token_ids: list[str],
    depths: list[Decimal],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    observed_at = (now or utc_now()).astimezone(UTC)
    raw_payloads: list[dict[str, Any]] = []
    batches = [
        token_ids[offset : offset + base_config.batch_size]
        for offset in range(0, len(token_ids), base_config.batch_size)
    ]
    request_batches: list[dict[str, Any]] = []
    for batch_number, token_batch in enumerate(batches, start=1):
        payload, attempts = client.fetch_batch(token_batch, batch_number=batch_number)
        books_by_token = {
            str(book.get("asset_id") or book.get("token_id")): book for book in payload
        }
        missing = sorted(set(token_batch) - set(books_by_token))
        if missing:
            print(
                f"WARNING: CLOB book batch {batch_number} omitted {len(missing)} "
                f"requested token(s): {', '.join(missing[:3])}",
                file=sys.stderr,
            )
        raw_payloads.extend(
            books_by_token[token_id] for token_id in token_batch if token_id in books_by_token
        )
        request_batches.append(
            {
                "batch_number": batch_number,
                "token_count": len(token_batch),
                "returned_token_count": len(token_batch) - len(missing),
                "omitted_token_ids": missing,
                "attempts": attempts,
            }
        )
    if not raw_payloads:
        raise ValueError("CLOB returned no order books for the requested tokens")

    envelopes: list[dict[str, Any]] = []
    for depth in depths:
        config = replace(base_config, depth_usdc=depth)
        books = [
            normalize_book(payload, depth_usdc=depth, observed_at=observed_at)
            for payload in raw_payloads
        ]
        result = OrderBookResult(books=books, request_batches=request_batches)
        ingest_run_id = uuid.uuid4().hex
        object_path = build_object_path(observed_at, ingest_run_id)
        envelope = build_envelope(
            config=config,
            result=result,
            ingest_run_id=ingest_run_id,
            ingested_at=observed_at,
            storage_uri=f"gs://{config.bucket_name}/{object_path}",
        )
        archived = archive_envelope(envelope)
        encoded = encode_envelope(archived)
        bid_levels = sum(len(book["bids"]) for book in books)
        ask_levels = sum(len(book["asks"]) for book in books)
        print(canonical_json_bytes(archived).decode("utf-8"))
        print(
            f"DRY RUN depth=${decimal_text(depth)} tokens={len(books)} "
            f"bid_levels={bid_levels} ask_levels={ask_levels} "
            f"json_bytes={len(canonical_json_bytes(archived))} "
            f"gzip_bytes={len(encoded)}; no GCS or PostgreSQL writes",
            file=sys.stderr,
        )
        envelopes.append(envelope)
    return envelopes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--token-id",
        action="append",
        default=[],
        help="Optional token ID for dry-run mode; otherwise tokens are read from PostgreSQL",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of PostgreSQL tokens used by a dry run without --token-id (default: 10)",
    )
    parser.add_argument(
        "--depth-usdc",
        action="append",
        type=Decimal,
        help="Cumulative USDC depth retained per side; repeat in dry-run mode to compare",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one live collection cycle instead of polling continuously",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    from src.db.odds_repository import OrderBookRepository

    args = build_parser().parse_args(argv)
    src_dir = Path(__file__).resolve().parents[1]
    config_path = src_dir / "config" / "polymarket_config.json"
    try:
        config = load_config(config_path)
        depths = args.depth_usdc or [config.depth_usdc]
        if any(not depth.is_finite() or depth <= 0 for depth in depths):
            raise ValueError("--depth-usdc values must be positive")
        if not args.dry_run and len(depths) > 1:
            raise ValueError("multiple --depth-usdc values are supported only with --dry-run")
        config = replace(config, depth_usdc=depths[0])
        client = ClobOrderBookClient(
            timeout_seconds=config.timeout_seconds,
            max_attempts=config.max_attempts,
        )
        if args.dry_run:
            if args.limit < 1:
                raise ValueError("--limit must be positive")
            token_ids = list(dict.fromkeys(args.token_id))
            if not token_ids:
                repository = OrderBookRepository.from_environment(src_dir)
                try:
                    token_ids = repository.load_open_token_ids()[: args.limit]
                finally:
                    repository.close()
                if not token_ids:
                    raise ValueError("PostgreSQL contains no open order-book token IDs")
                print(
                    f"DRY RUN: loaded {len(token_ids)} open token IDs from PostgreSQL",
                    file=sys.stderr,
                )
            run_dry_run(
                base_config=config,
                client=client,
                token_ids=token_ids,
                depths=depths,
            )
            return 0
    except Exception as exc:
        print(f"ERROR: failed to initialize CLOB order-book ingestion: {exc}", file=sys.stderr)
        return 1

    repository: OrderBookRepository | None = None
    try:
        gcs_client = create_gcs_client(src_dir)
        bucket = gcs_client.bucket(config.bucket_name)
        repository = OrderBookRepository.from_environment(src_dir)
    except Exception as exc:
        if repository:
            repository.close()
        print(f"ERROR: failed to initialize CLOB order-book storage: {exc}", file=sys.stderr)
        return 1

    print(
        "Starting Polymarket CLOB order-book poller. "
        f"Sleep between cycles: {config.poll_interval_seconds}s. "
        f"Depth: ${decimal_text(config.depth_usdc)} per side. "
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
                print(f"ERROR: CLOB order-book cycle failed: {exc}", file=sys.stderr)
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(config.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0
    finally:
        repository.close()
