"""Raw-first collection of executed Kalshi trades for tracked markets.

Kalshi serves one exchange-wide trade feed, newest first, with server-side
``min_ts`` filtering and cursor pagination, so a single paginated pass per
cycle covers every tracked market. Records are filtered client-side to the
markets known to the structure collector before archiving. Each trade carries
a canonical ``trade_id``, which makes overlap re-reads idempotent.

The exchange prints roughly 5-10 thousand trades per minute (dominated by
recurring crypto markets), about 20-30 pages per cycle, so the initial
lookback is deliberately shallow: deep backfill through this feed is not
feasible and the page cap self-heals by advancing past unreachable history.
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
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

from src.common.gcs import canonical_json_bytes, create_gcs_client
from src.ingest_odds.kalshi_client import KalshiClient, load_kalshi_credentials

TRADES_PATH = "/trade-api/v2/markets/trades"
SCHEMA_NAME = "kalshi_trades"
SCHEMA_VERSION = 1
STORAGE_PROVIDER = "kalshi"
STORAGE_SOURCE = "trade-api"
STORAGE_OBJECT = "trades"
CURSOR_STREAM = "trades"


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


def parse_created_time(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError("trade created_time is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class KalshiTradesConfig:
    bucket_name: str = "ai-sports-bettor"
    base_url: str = "https://api.elections.kalshi.com"
    poll_interval_seconds: int = 60
    page_limit: int = 1000
    max_pages: int = 200
    overlap_seconds: int = 120
    initial_lookback_minutes: int = 15
    timeout_seconds: float = 30
    max_attempts: int = 5
    min_request_interval_seconds: float = 0.07

    def __post_init__(self) -> None:
        if self.poll_interval_seconds < 1:
            raise ValueError("kalshi_trades_poll_interval_seconds must be positive")
        if not 1 <= self.page_limit <= 1000:
            raise ValueError("kalshi_trades_page_limit must be between 1 and 1000")
        if self.max_pages < 1:
            raise ValueError("kalshi_trades_max_pages must be positive")
        if self.overlap_seconds < 0:
            raise ValueError("kalshi_trades_overlap_seconds cannot be negative")
        if self.initial_lookback_minutes < 1:
            raise ValueError("kalshi_trades_initial_lookback_minutes must be positive")


@dataclass
class KalshiTradesResult:
    trades: list[dict[str, Any]] = field(default_factory=list)
    request_pages: list[dict[str, Any]] = field(default_factory=list)


class KalshiTradesRepositoryProtocol(Protocol):
    def load_checkpoint(self) -> dict[str, Any]: ...

    def load_tracked_tickers(self, *, missing_cutoff: datetime) -> list[str]: ...

    def persist_records(self, envelope: dict[str, Any]) -> None: ...

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None: ...


def load_trades_config(path: Path) -> KalshiTradesConfig:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    return KalshiTradesConfig(
        bucket_name=str(payload.get("gcs_bucket", "ai-sports-bettor")),
        base_url=str(payload.get("kalshi_base_url", "https://api.elections.kalshi.com")),
        poll_interval_seconds=int(payload.get("kalshi_trades_poll_interval_seconds", 60)),
        page_limit=int(payload.get("kalshi_trades_page_limit", 1000)),
        max_pages=int(payload.get("kalshi_trades_max_pages", 200)),
        overlap_seconds=int(payload.get("kalshi_trades_overlap_seconds", 120)),
        initial_lookback_minutes=int(
            payload.get("kalshi_trades_initial_lookback_minutes", 15)
        ),
        timeout_seconds=float(payload.get("kalshi_timeout_seconds", 30)),
        max_attempts=int(payload.get("kalshi_max_attempts", 5)),
        min_request_interval_seconds=float(
            payload.get("kalshi_min_request_interval_seconds", 0.07)
        ),
    )


def query_fingerprint(config: KalshiTradesConfig) -> str:
    return content_sha256(
        {
            "endpoint": TRADES_PATH,
            "schema_name": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
        }
    )


def normalize_trade(payload: dict[str, Any], observed_at: datetime) -> dict[str, Any]:
    trade_id = payload.get("trade_id")
    ticker = payload.get("ticker")
    if not trade_id or not ticker:
        raise ValueError("trade is missing trade_id or ticker")
    traded_at = parse_created_time(payload.get("created_time"))
    taker_outcome_side = payload.get("taker_outcome_side") or payload.get("taker_side")
    return {
        "trade_id": str(trade_id),
        "ticker": str(ticker),
        "count": decimal_text(payload.get("count_fp"), "trade.count_fp"),
        "yes_price": decimal_text(payload.get("yes_price_dollars"), "trade.yes_price_dollars"),
        "no_price": decimal_text(payload.get("no_price_dollars"), "trade.no_price_dollars"),
        "taker_outcome_side": str(taker_outcome_side) if taker_outcome_side else None,
        "taker_book_side": (
            str(payload["taker_book_side"]) if payload.get("taker_book_side") else None
        ),
        "is_block_trade": bool(payload.get("is_block_trade", False)),
        "traded_at": traded_at.isoformat(),
        "observed_at": observed_at.isoformat(),
    }


def fetch_new_trades(
    *,
    config: KalshiTradesConfig,
    client: KalshiClient,
    tracked_tickers: set[str],
    floor: datetime,
    observed_at: datetime,
) -> KalshiTradesResult:
    """Page the exchange-wide feed newest-first until it falls behind the floor."""
    result = KalshiTradesResult()
    seen_trade_ids: set[str] = set()
    floor_epoch = int(floor.timestamp())
    cursor: str | None = None
    for page_number in range(1, config.max_pages + 1):
        params = {
            "min_ts": str(floor_epoch),
            "limit": str(config.page_limit),
        }
        if cursor:
            params["cursor"] = cursor
        payload, attempts = client.get_json(
            TRADES_PATH, params, description=f"trades page {page_number}"
        )
        trades = payload.get("trades") or []
        tracked_in_page = 0
        oldest: datetime | None = None
        for raw_trade in trades:
            if not isinstance(raw_trade, dict):
                continue
            traded_at = parse_created_time(raw_trade.get("created_time"))
            if oldest is None or traded_at < oldest:
                oldest = traded_at
            if traded_at < floor:
                continue
            if str(raw_trade.get("ticker") or "") not in tracked_tickers:
                continue
            record = normalize_trade(raw_trade, observed_at)
            if record["trade_id"] in seen_trade_ids:
                continue
            seen_trade_ids.add(record["trade_id"])
            result.trades.append(record)
            tracked_in_page += 1
        result.request_pages.append(
            {
                "page_number": page_number,
                "returned_trade_count": len(trades),
                "tracked_trade_count": tracked_in_page,
                "attempts": attempts,
            }
        )
        cursor = payload.get("cursor") or None
        if not trades or not cursor:
            break
        if oldest is not None and oldest < floor:
            break
        if page_number == config.max_pages:
            print(
                f"WARNING: Kalshi trades hit the {config.max_pages}-page safety "
                "limit before reaching the watermark; older trades will be picked "
                "up by the overlap window or remain in the provider history",
                file=sys.stderr,
            )
    result.trades.sort(key=lambda trade: trade["traded_at"])
    return result


def build_object_path(ingested_at: datetime, ingest_run_id: str) -> str:
    utc = ingested_at.astimezone(UTC)
    return (
        f"raw/provider={STORAGE_PROVIDER}/source={STORAGE_SOURCE}/"
        f"object={STORAGE_OBJECT}/schema=v{SCHEMA_VERSION}/"
        f"date={utc:%Y-%m-%d}/hour={utc:%H}/"
        f"kalshi_trades_{ingest_run_id}.json.gz"
    )


def build_envelope(
    *,
    config: KalshiTradesConfig,
    result: KalshiTradesResult,
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
            "endpoint": TRADES_PATH,
            "page_limit": config.page_limit,
            "watermark": watermark.astimezone(UTC).isoformat(),
            "overlap_floor": floor.astimezone(UTC).isoformat(),
            "page_count": len(result.request_pages),
            "query_fingerprint": query_fingerprint(config),
            "pages": result.request_pages,
        },
        "records": result.trades,
    }


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return gzip.compress(canonical_json_bytes(envelope), compresslevel=6, mtime=0)


def load_watermark(
    checkpoint: dict[str, Any],
    config: KalshiTradesConfig,
    now: datetime,
) -> datetime:
    since_id = checkpoint.get("since_id")
    if since_id and checkpoint.get("query_fingerprint") == query_fingerprint(config):
        parsed = datetime.fromisoformat(str(since_id))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return now - timedelta(minutes=config.initial_lookback_minutes)


def run_cycle(
    *,
    config: KalshiTradesConfig,
    client: KalshiClient,
    bucket: Any,
    repository: KalshiTradesRepositoryProtocol,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    cycle_started_at = (now or utc_now()).astimezone(UTC)
    tracked = repository.load_tracked_tickers(
        missing_cutoff=cycle_started_at - timedelta(days=1),
    )
    if not tracked:
        print("No tracked Kalshi market tickers are available; skipping trades cycle")
        return None
    watermark = load_watermark(repository.load_checkpoint(), config, cycle_started_at)
    floor = watermark - timedelta(seconds=config.overlap_seconds)
    print(
        f"Starting Kalshi trades cycle for {len(tracked)} tracked markets "
        f"since {floor.isoformat()}"
    )
    result = fetch_new_trades(
        config=config,
        client=client,
        tracked_tickers=set(tracked),
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
        print("No new tracked trades since the watermark; advanced the checkpoint only")
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
    print(f"Uploaded {envelope['record_count']} Kalshi trades to {envelope['storage_uri']}")
    repository.persist_records(envelope)
    repository.finalize_cycle(checkpoint)
    print("Committed Kalshi trades and advanced the checkpoint")
    return checkpoint


def run_dry_run(
    *,
    config: KalshiTradesConfig,
    client: KalshiClient,
    tracked_tickers: set[str],
    now: datetime | None = None,
) -> KalshiTradesResult:
    observed_at = (now or utc_now()).astimezone(UTC)
    floor = observed_at - timedelta(minutes=config.initial_lookback_minutes)
    result = fetch_new_trades(
        config=config,
        client=client,
        tracked_tickers=tracked_tickers,
        floor=floor,
        observed_at=observed_at,
    )
    print("DRY RUN: no GCS, PostgreSQL, or checkpoint writes will occur")
    print(
        f"Fetched {len(result.trades)} tracked trades across "
        f"{len(result.request_pages)} pages since {floor.isoformat()}"
    )
    for trade in result.trades[:5]:
        print(
            f"  {trade['traded_at']} {trade['ticker']}: {trade['count']} @ "
            f"{trade['yes_price']} (taker {trade['taker_outcome_side']})"
        )
    if len(result.trades) > 5:
        print(f"  ... {len(result.trades) - 5} additional trades")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="Optional market ticker for dry-run mode; otherwise tracked markets "
        "are read from PostgreSQL",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of PostgreSQL tickers used by a dry run without --ticker "
        "(default: 100)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one live collection cycle instead of polling continuously",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    from src.db.kalshi_repository import KalshiTradesRepository

    args = build_parser().parse_args(argv)
    src_dir = Path(__file__).resolve().parents[1]
    config_path = src_dir / "config" / "kalshi_config.json"
    try:
        config = load_trades_config(config_path)
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
                repository = KalshiTradesRepository.from_environment(src_dir)
                try:
                    tickers = repository.load_tracked_tickers(
                        missing_cutoff=utc_now() - timedelta(days=1),
                    )[: args.limit]
                finally:
                    repository.close()
                if not tickers:
                    raise ValueError("PostgreSQL contains no tracked Kalshi tickers")
                print(
                    f"DRY RUN: loaded {len(tickers)} tracked tickers from PostgreSQL",
                    file=sys.stderr,
                )
            run_dry_run(config=config, client=client, tracked_tickers=set(tickers))
            return 0
    except Exception as exc:
        print(f"ERROR: failed to initialize Kalshi trades ingestion: {exc}", file=sys.stderr)
        return 1

    repository: KalshiTradesRepository | None = None
    try:
        gcs_client = create_gcs_client(src_dir)
        bucket = gcs_client.bucket(config.bucket_name)
        repository = KalshiTradesRepository.from_environment(src_dir)
    except Exception as exc:
        if repository:
            repository.close()
        print(f"ERROR: failed to initialize Kalshi trades storage: {exc}", file=sys.stderr)
        return 1

    print(
        "Starting Kalshi trades poller. "
        f"Interval: {config.poll_interval_seconds}s. "
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
                print(f"ERROR: Kalshi trades cycle failed: {exc}", file=sys.stderr)
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(config.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0
    finally:
        repository.close()
