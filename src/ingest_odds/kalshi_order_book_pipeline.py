"""Frequent bounded snapshots of Kalshi market order books.

Kalshi books quote resting bids on both sides only: a NO bid at price p is
economically a YES ask at (1 - p), so each snapshot is normalized into a
conventional yes-side bid/ask book to match the Polymarket CLOB snapshots.
The batch endpoint accepts up to 100 tickers per request, passed as repeated
``tickers`` query parameters (a comma-separated value is silently treated as
a single ticker).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

from src.common.gcs import canonical_json_bytes, create_gcs_client
from src.ingest_odds.kalshi_client import KalshiClient, load_kalshi_credentials

ORDERBOOKS_PATH = "/trade-api/v2/markets/orderbooks"
SCHEMA_NAME = "kalshi_order_books"
SCHEMA_VERSION = 1
STORAGE_PROVIDER = "kalshi"
STORAGE_SOURCE = "trade-api"
STORAGE_OBJECT = "order-books"
CURSOR_STREAM = "order_books"
# Effectively unbounded: retain the full ladder, matching the Polymarket
# CLOB collector as deployed.
DEFAULT_DEPTH_USDC = Decimal("1000000000")
MAX_BATCH_SIZE = 100
ONE = Decimal("1")


def utc_now() -> datetime:
    return datetime.now(UTC)


def content_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


@dataclass(frozen=True)
class KalshiOrderBookConfig:
    bucket_name: str = "ai-sports-bettor"
    base_url: str = "https://api.elections.kalshi.com"
    poll_interval_seconds: int = 15
    depth_usdc: Decimal = DEFAULT_DEPTH_USDC
    batch_size: int = MAX_BATCH_SIZE
    timeout_seconds: float = 30
    max_attempts: int = 5
    min_request_interval_seconds: float = 0.07

    def __post_init__(self) -> None:
        if self.poll_interval_seconds < 1:
            raise ValueError("kalshi_order_book_poll_interval_seconds must be positive")
        if not self.depth_usdc.is_finite() or self.depth_usdc <= 0:
            raise ValueError("kalshi_order_book_depth_usdc must be positive")
        if not 1 <= self.batch_size <= MAX_BATCH_SIZE:
            raise ValueError(
                f"kalshi_order_book_batch_size must be between 1 and {MAX_BATCH_SIZE}"
            )


@dataclass
class KalshiOrderBookResult:
    books: list[dict[str, Any]] = field(default_factory=list)
    request_batches: list[dict[str, Any]] = field(default_factory=list)


class KalshiOrderBookRepositoryProtocol(Protocol):
    def load_open_market_tickers(self) -> list[str]: ...

    def persist_records(self, envelope: dict[str, Any]) -> None: ...

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None: ...


def load_order_book_config(path: Path) -> KalshiOrderBookConfig:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    return KalshiOrderBookConfig(
        bucket_name=str(payload.get("gcs_bucket", "ai-sports-bettor")),
        base_url=str(payload.get("kalshi_base_url", "https://api.elections.kalshi.com")),
        poll_interval_seconds=int(payload.get("kalshi_order_book_poll_interval_seconds", 15)),
        depth_usdc=Decimal(
            str(payload.get("kalshi_order_book_depth_usdc", str(DEFAULT_DEPTH_USDC)))
        ),
        batch_size=int(payload.get("kalshi_order_book_batch_size", MAX_BATCH_SIZE)),
        timeout_seconds=float(payload.get("kalshi_timeout_seconds", 30)),
        max_attempts=int(payload.get("kalshi_max_attempts", 5)),
        min_request_interval_seconds=float(
            payload.get("kalshi_min_request_interval_seconds", 0.07)
        ),
    )


def query_fingerprint(config: KalshiOrderBookConfig) -> str:
    return content_sha256(
        {
            "endpoint": ORDERBOOKS_PATH,
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


def _side_levels(payload: Any, side: str) -> list[tuple[Decimal, Decimal]]:
    """Parse raw [price, contracts] pairs for one side of the book."""
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise ValueError(f"order book {side} must be a list")
    levels: list[tuple[Decimal, Decimal]] = []
    for raw_level in payload:
        if not isinstance(raw_level, (list, tuple)) or len(raw_level) != 2:
            raise ValueError(f"order book {side} contains an invalid level")
        price = _decimal(raw_level[0], f"{side}.price")
        size = _decimal(raw_level[1], f"{side}.size")
        if not 0 < price < 1 or size < 0:
            raise ValueError(f"order book {side} contains an invalid level")
        if size:
            levels.append((price, size))
    return levels


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
    ticker: str,
    orderbook_fp: dict[str, Any],
    *,
    depth_usdc: Decimal,
    observed_at: datetime,
) -> dict[str, Any]:
    yes_bids = sorted(
        _side_levels(orderbook_fp.get("yes_dollars"), "yes_dollars"),
        key=lambda level: level[0],
        reverse=True,
    )
    # A NO bid at price p is a YES ask at (1 - p) with the same size.
    yes_asks = sorted(
        (
            (ONE - price, size)
            for price, size in _side_levels(orderbook_fp.get("no_dollars"), "no_dollars")
        ),
        key=lambda level: level[0],
    )
    bounded_bids = _bounded_levels(yes_bids, depth_usdc)
    bounded_asks = _bounded_levels(yes_asks, depth_usdc)
    best_bid = yes_bids[0][0] if yes_bids else None
    best_ask = yes_asks[0][0] if yes_asks else None
    midpoint = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
    return {
        "ticker": ticker,
        "observed_at": observed_at.isoformat(),
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
    }


def fetch_books(
    *,
    client: KalshiClient,
    tickers: list[str],
    depth_usdc: Decimal,
    batch_size: int,
    observed_at: datetime,
    report_progress: bool = True,
) -> KalshiOrderBookResult:
    result = KalshiOrderBookResult()
    batches = [
        tickers[offset : offset + batch_size]
        for offset in range(0, len(tickers), batch_size)
    ]
    for batch_number, ticker_batch in enumerate(batches, start=1):
        params = [("tickers", ticker) for ticker in ticker_batch]
        payload, attempts = client.get_json(
            ORDERBOOKS_PATH, params, description=f"order-book batch {batch_number}"
        )
        raw_books = payload.get("orderbooks") or []
        books_by_ticker = {
            str(book.get("ticker")): book.get("orderbook_fp") or {}
            for book in raw_books
            if isinstance(book, dict) and book.get("ticker")
        }
        missing = sorted(set(ticker_batch) - set(books_by_ticker))
        if missing:
            print(
                f"WARNING: Kalshi order-book batch {batch_number} omitted "
                f"{len(missing)} requested ticker(s): {', '.join(missing[:3])}",
                file=sys.stderr,
            )
        result.books.extend(
            normalize_book(
                ticker,
                books_by_ticker[ticker],
                depth_usdc=depth_usdc,
                observed_at=observed_at,
            )
            for ticker in ticker_batch
            if ticker in books_by_ticker
        )
        result.request_batches.append(
            {
                "batch_number": batch_number,
                "ticker_count": len(ticker_batch),
                "returned_ticker_count": len(ticker_batch) - len(missing),
                "omitted_tickers": missing,
                "attempts": attempts,
            }
        )
        if report_progress:
            print(
                f"Fetched Kalshi order-book batch {batch_number}/{len(batches)} "
                f"for {len(ticker_batch)} tickers"
            )
    if not result.books:
        raise ValueError("Kalshi returned no order books for the requested tickers")
    return result


def build_object_path(ingested_at: datetime, ingest_run_id: str) -> str:
    utc = ingested_at.astimezone(UTC)
    return (
        f"raw/provider={STORAGE_PROVIDER}/source={STORAGE_SOURCE}/"
        f"object={STORAGE_OBJECT}/schema=v{SCHEMA_VERSION}/"
        f"date={utc:%Y-%m-%d}/hour={utc:%H}/"
        f"kalshi_order_books_{ingest_run_id}.json.gz"
    )


def build_envelope(
    *,
    config: KalshiOrderBookConfig,
    result: KalshiOrderBookResult,
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
            "endpoint": ORDERBOOKS_PATH,
            "depth_usdc": decimal_text(config.depth_usdc),
            "batch_count": len(result.request_batches),
            "query_fingerprint": query_fingerprint(config),
            "batches": result.request_batches,
        },
        "records": result.books,
    }


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return gzip.compress(canonical_json_bytes(envelope), compresslevel=6, mtime=0)


def run_cycle(
    *,
    config: KalshiOrderBookConfig,
    client: KalshiClient,
    bucket: Any,
    repository: KalshiOrderBookRepositoryProtocol,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    tickers = repository.load_open_market_tickers()
    if not tickers:
        print("No open Kalshi market tickers are available; skipping order-book cycle")
        return None
    observed_at = (now or utc_now()).astimezone(UTC)
    print(
        f"Starting Kalshi order-book cycle for {len(tickers)} markets "
        f"at ${decimal_text(config.depth_usdc)} depth per side"
    )
    result = fetch_books(
        client=client,
        tickers=tickers,
        depth_usdc=config.depth_usdc,
        batch_size=config.batch_size,
        observed_at=observed_at,
    )
    ingest_run_id = uuid.uuid4().hex
    object_path = build_object_path(observed_at, ingest_run_id)
    envelope = build_envelope(
        config=config,
        result=result,
        ingest_run_id=ingest_run_id,
        ingested_at=observed_at,
        storage_uri=f"gs://{config.bucket_name}/{object_path}",
    )
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
        encode_envelope(envelope),
        content_type="application/json",
    )
    print(f"Uploaded {envelope['record_count']} Kalshi order books to {envelope['storage_uri']}")
    repository.persist_records(envelope)
    checkpoint = {
        "query_fingerprint": query_fingerprint(config),
        "since_id": envelope["ingested_at"],
        "updated_at": envelope["ingested_at"],
        "last_successful_poll_at": envelope["ingested_at"],
    }
    repository.finalize_cycle(checkpoint)
    print("Committed Kalshi order-book snapshots and advanced the checkpoint")
    return checkpoint


def run_dry_run(
    *,
    config: KalshiOrderBookConfig,
    client: KalshiClient,
    tickers: list[str],
    now: datetime | None = None,
) -> KalshiOrderBookResult:
    observed_at = (now or utc_now()).astimezone(UTC)
    result = fetch_books(
        client=client,
        tickers=tickers,
        depth_usdc=config.depth_usdc,
        batch_size=config.batch_size,
        observed_at=observed_at,
    )
    print("DRY RUN: no GCS, PostgreSQL, or checkpoint writes will occur")
    for book in result.books[:5]:
        print(
            f"  {book['ticker']}: bid {book['best_bid']} / ask {book['best_ask']} "
            f"(mid {book['midpoint']}, spread {book['spread']}, "
            f"{len(book['bids'])} bid levels, {len(book['asks'])} ask levels)"
        )
    if len(result.books) > 5:
        print(f"  ... {len(result.books) - 5} additional books")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="Optional market ticker for dry-run mode; otherwise tickers are read "
        "from PostgreSQL",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of PostgreSQL tickers used by a dry run without --ticker "
        "(default: 10)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one live collection cycle instead of polling continuously",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    from src.db.kalshi_repository import KalshiOrderBookRepository

    args = build_parser().parse_args(argv)
    src_dir = Path(__file__).resolve().parents[1]
    config_path = src_dir / "config" / "kalshi_config.json"
    try:
        config = load_order_book_config(config_path)
        client = KalshiClient(
            credentials=load_kalshi_credentials(src_dir),
            base_url=config.base_url,
            timeout_seconds=config.timeout_seconds,
            max_attempts=config.max_attempts,
            min_request_interval_seconds=config.min_request_interval_seconds,
        )
        if args.dry_run:
            if args.limit < 1:
                raise ValueError("--limit must be positive")
            tickers = list(dict.fromkeys(args.ticker))
            if not tickers:
                repository = KalshiOrderBookRepository.from_environment(src_dir)
                try:
                    tickers = repository.load_open_market_tickers()[: args.limit]
                finally:
                    repository.close()
                if not tickers:
                    raise ValueError("PostgreSQL contains no open Kalshi market tickers")
                print(
                    f"DRY RUN: loaded {len(tickers)} open tickers from PostgreSQL",
                    file=sys.stderr,
                )
            run_dry_run(config=config, client=client, tickers=tickers)
            return 0
    except Exception as exc:
        print(f"ERROR: failed to initialize Kalshi order-book ingestion: {exc}", file=sys.stderr)
        return 1

    repository: KalshiOrderBookRepository | None = None
    try:
        gcs_client = create_gcs_client(src_dir)
        bucket = gcs_client.bucket(config.bucket_name)
        repository = KalshiOrderBookRepository.from_environment(src_dir)
    except Exception as exc:
        if repository:
            repository.close()
        print(f"ERROR: failed to initialize Kalshi order-book storage: {exc}", file=sys.stderr)
        return 1

    print(
        "Starting Kalshi order-book poller. "
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
                print(f"ERROR: Kalshi order-book cycle failed: {exc}", file=sys.stderr)
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(config.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0
    finally:
        repository.close()
