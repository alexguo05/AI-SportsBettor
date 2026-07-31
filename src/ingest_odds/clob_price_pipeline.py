"""Incremental public CLOB price-history ingestion."""

from __future__ import annotations

import gzip
import hashlib
import json
import random
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

import requests

from src.common.gcs import canonical_json_bytes, create_gcs_client

CLOB_BATCH_PRICES_URL = "https://clob.polymarket.com/batch-prices-history"
SCHEMA_NAME = "polymarket_clob_price_history"
SCHEMA_VERSION = 1
STORAGE_PROVIDER = "polymarket"
STORAGE_SOURCE = "clob"
STORAGE_OBJECT = "price-history"


def utc_now() -> datetime:
    return datetime.now(UTC)


def content_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


@dataclass(frozen=True)
class ClobPriceConfig:
    bucket_name: str = "ai-sports-bettor"
    poll_interval_seconds: int = 900
    fidelity_minutes: int = 1
    initial_lookback_minutes: int = 15
    batch_size: int = 20
    timeout_seconds: float = 30
    max_attempts: int = 5

    def __post_init__(self) -> None:
        if self.poll_interval_seconds < 1:
            raise ValueError("clob_price_poll_interval_seconds must be positive")
        if self.fidelity_minutes < 1:
            raise ValueError("clob_price_fidelity_minutes must be positive")
        if self.initial_lookback_minutes < 1:
            raise ValueError("clob_price_initial_lookback_minutes must be positive")
        if not 1 <= self.batch_size <= 20:
            raise ValueError("clob_price_batch_size must be between 1 and 20")
        if self.timeout_seconds <= 0:
            raise ValueError("clob_price_timeout_seconds must be positive")
        if self.max_attempts < 1:
            raise ValueError("clob_price_max_attempts must be positive")


@dataclass
class ClobResult:
    raw_responses: list[dict[str, Any]] = field(default_factory=list)
    request_batches: list[dict[str, Any]] = field(default_factory=list)
    points: list[dict[str, Any]] = field(default_factory=list)


class PriceRepositoryProtocol(Protocol):
    def load_open_token_ids(self) -> list[str]: ...

    def load_checkpoint(self) -> dict[str, Any]: ...

    def load_price_cursors(self, token_ids: list[str]) -> dict[str, dict[str, Any]]: ...

    def persist_records(self, envelope: dict[str, Any]) -> None: ...

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None: ...


def load_config(path: Path) -> ClobPriceConfig:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    return ClobPriceConfig(
        bucket_name=str(payload.get("gcs_bucket", "ai-sports-bettor")),
        poll_interval_seconds=int(payload.get("clob_price_poll_interval_seconds", 900)),
        fidelity_minutes=int(payload.get("clob_price_fidelity_minutes", 1)),
        initial_lookback_minutes=int(
            payload.get("clob_price_initial_lookback_minutes", 15)
        ),
        batch_size=int(payload.get("clob_price_batch_size", 20)),
        timeout_seconds=float(payload.get("clob_price_timeout_seconds", 30)),
        max_attempts=int(payload.get("clob_price_max_attempts", 5)),
    )


def query_fingerprint(config: ClobPriceConfig) -> str:
    return content_sha256(
        {
            "endpoint": CLOB_BATCH_PRICES_URL,
            "fidelity_minutes": config.fidelity_minutes,
            "schema_name": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
        }
    )


class ClobPriceClient:
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
        *,
        token_ids: list[str],
        start_ts: int,
        end_ts: int,
        fidelity_minutes: int,
        batch_number: int,
    ) -> tuple[dict[str, Any], int]:
        body = {
            "markets": token_ids,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "fidelity": fidelity_minutes,
        }
        retryable_statuses = {429, 500, 502, 503, 504}
        for attempt in range(1, self.max_attempts + 1):
            response: requests.Response | None = None
            try:
                response = self.session.post(
                    CLOB_BATCH_PRICES_URL,
                    json=body,
                    headers=self.headers,
                    timeout=self.timeout_seconds,
                )
                if response.status_code not in retryable_statuses:
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, dict):
                        raise ValueError("CLOB batch history response must be an object")
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
                f"WARNING: CLOB batch {batch_number} returned {status}; "
                f"retrying attempt {attempt + 1}/{self.max_attempts} in {delay:.1f}s",
                file=sys.stderr,
            )
            self.sleep(delay)
        raise RuntimeError("unreachable CLOB retry state")

    def fetch_history(
        self,
        *,
        token_ids: list[str],
        start_ts: int,
        end_ts: int,
        fidelity_minutes: int,
        batch_size: int,
        observed_at: datetime,
    ) -> ClobResult:
        result = ClobResult()
        point_index: dict[tuple[str, int], dict[str, Any]] = {}
        batches = [
            token_ids[index : index + batch_size]
            for index in range(0, len(token_ids), batch_size)
        ]
        for batch_number, token_batch in enumerate(batches, start=1):
            payload, attempts = self.fetch_batch(
                token_ids=token_batch,
                start_ts=start_ts,
                end_ts=end_ts,
                fidelity_minutes=fidelity_minutes,
                batch_number=batch_number,
            )
            result.raw_responses.append(
                {
                    "token_ids": token_batch,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "response": payload,
                }
            )
            result.request_batches.append(
                {
                    "batch_number": batch_number,
                    "token_count": len(token_batch),
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "attempts": attempts,
                }
            )
            histories = payload.get("history", payload)
            if not isinstance(histories, dict):
                raise ValueError("CLOB batch history field must be an object")
            histories_by_token = {
                str(token_id): history for token_id, history in histories.items()
            }
            missing_token_ids = set(token_batch) - set(histories_by_token)
            if missing_token_ids:
                missing = ", ".join(sorted(missing_token_ids)[:3])
                raise ValueError(
                    f"CLOB batch {batch_number} omitted {len(missing_token_ids)} "
                    f"requested token(s): {missing}"
                )
            for token_id in token_batch:
                history = histories_by_token[token_id]
                if not isinstance(history, list):
                    raise ValueError(f"CLOB history for token {token_id} must be a list")
                for point in history:
                    if (
                        not isinstance(point, dict)
                        or point.get("t") is None
                        or point.get("p") is None
                    ):
                        raise ValueError(
                            f"CLOB history for token {token_id} contains an invalid point"
                        )
                    try:
                        timestamp = int(point["t"])
                        price = Decimal(str(point["p"]))
                    except (InvalidOperation, TypeError, ValueError) as exc:
                        raise ValueError(
                            f"CLOB history for token {token_id} contains an invalid point"
                        ) from exc
                    if timestamp <= 0 or not price.is_finite() or not 0 <= price <= 1:
                        raise ValueError(
                            f"CLOB history for token {token_id} contains an invalid point"
                        )
                    point_index[(token_id, timestamp)] = {
                        "token_id": token_id,
                        "source_timestamp": datetime.fromtimestamp(
                            timestamp,
                            tz=UTC,
                        ).isoformat(),
                        "price": format(price, "f"),
                        "fidelity_minutes": fidelity_minutes,
                        "observed_at": observed_at.isoformat(),
                    }
            print(
                f"Fetched CLOB batch {batch_number}/{len(batches)} "
                f"for {len(token_batch)} tokens"
            )
        result.points = list(point_index.values())
        return result


def choose_token_start(
    token_cursor: dict[str, Any] | None,
    config: ClobPriceConfig,
    end_ts: int,
) -> int:
    token_cursor = token_cursor or {}
    fingerprint_matches = token_cursor.get("query_fingerprint") == query_fingerprint(config)
    try:
        previous_end = int(token_cursor["last_end_ts"]) if fingerprint_matches else None
    except (KeyError, TypeError, ValueError):
        previous_end = None
    if previous_end is None:
        start_ts = end_ts - config.initial_lookback_minutes * 60
    else:
        start_ts = max(0, previous_end - config.fidelity_minutes * 60)
    if start_ts >= end_ts:
        start_ts = max(0, end_ts - config.fidelity_minutes * 60)
    return start_ts


def build_object_path(ingested_at: datetime, ingest_run_id: str) -> str:
    utc = ingested_at.astimezone(UTC)
    return (
        f"raw/provider={STORAGE_PROVIDER}/source={STORAGE_SOURCE}/"
        f"object={STORAGE_OBJECT}/schema=v{SCHEMA_VERSION}/"
        f"date={utc:%Y-%m-%d}/hour={utc:%H}/"
        f"polymarket_prices_{ingest_run_id}.json.gz"
    )


def build_envelope(
    *,
    config: ClobPriceConfig,
    result: ClobResult,
    ingest_run_id: str,
    ingested_at: datetime,
    storage_uri: str,
    start_ts: int,
    end_ts: int,
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
        "content_sha256": content_sha256(result.raw_responses),
        "record_count": len(result.points),
        "request": {
            "endpoint": CLOB_BATCH_PRICES_URL,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "fidelity_minutes": config.fidelity_minutes,
            "batch_count": len(result.raw_responses),
            "query_fingerprint": query_fingerprint(config),
            "batches": result.request_batches,
        },
        "records": result.points,
        "raw_api_responses": result.raw_responses,
    }


def archive_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in envelope.items()
        if key != "records" and not key.startswith("_")
    }


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return gzip.compress(canonical_json_bytes(envelope), compresslevel=6, mtime=0)


def run_cycle(
    *,
    config: ClobPriceConfig,
    client: ClobPriceClient,
    bucket: Any,
    repository: PriceRepositoryProtocol,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    cycle_started_at = (now or utc_now()).astimezone(UTC)
    token_ids = repository.load_open_token_ids()
    if not token_ids:
        print("No open Polymarket token IDs are available; skipping CLOB cycle")
        return None
    token_cursors = repository.load_price_cursors(token_ids)
    end_ts = int(cycle_started_at.timestamp())
    token_groups: dict[int, list[str]] = {}
    for token_id in token_ids:
        start_ts = choose_token_start(token_cursors.get(token_id), config, end_ts)
        token_groups.setdefault(start_ts, []).append(token_id)
    earliest_start = min(token_groups)
    print(
        f"Starting CLOB price cycle for {len(token_ids)} tokens "
        f"across {len(token_groups)} cursor window(s) through {end_ts}"
    )
    result = ClobResult()
    for start_ts, grouped_token_ids in sorted(token_groups.items()):
        group_result = client.fetch_history(
            token_ids=grouped_token_ids,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity_minutes=config.fidelity_minutes,
            batch_size=config.batch_size,
            observed_at=cycle_started_at,
        )
        result.raw_responses.extend(group_result.raw_responses)
        result.request_batches.extend(group_result.request_batches)
        result.points.extend(group_result.points)
    ingest_run_id = uuid.uuid4().hex
    object_path = build_object_path(cycle_started_at, ingest_run_id)
    storage_uri = f"gs://{config.bucket_name}/{object_path}"
    envelope = build_envelope(
        config=config,
        result=result,
        ingest_run_id=ingest_run_id,
        ingested_at=cycle_started_at,
        storage_uri=storage_uri,
        start_ts=earliest_start,
        end_ts=end_ts,
    )
    envelope["_token_cursor_candidates"] = {
        token_id: end_ts for token_id in token_ids
    }
    blob = bucket.blob(object_path)
    blob.metadata = {
        "schema_name": SCHEMA_NAME,
        "schema_version": str(SCHEMA_VERSION),
        "content_sha256": envelope["content_sha256"],
        "record_count": str(envelope["record_count"]),
    }
    blob.content_encoding = "gzip"
    blob.upload_from_string(
        encode_envelope(archive_envelope(envelope)),
        content_type="application/json",
    )
    print(
        f"Uploaded {envelope['record_count']} CLOB price points to "
        f"{envelope['storage_uri']}"
    )
    repository.persist_records(envelope)
    next_checkpoint = {
        "query_fingerprint": query_fingerprint(config),
        "since_id": str(end_ts),
        "updated_at": cycle_started_at.isoformat(),
        "last_successful_poll_at": cycle_started_at.isoformat(),
    }
    repository.finalize_cycle(next_checkpoint)
    print("Committed CLOB price points and advanced the price checkpoint")
    return next_checkpoint


def main() -> int:
    from src.db.odds_repository import PriceRepository

    src_dir = Path(__file__).resolve().parents[1]
    config_path = src_dir / "config" / "polymarket_config.json"
    repository: PriceRepository | None = None
    try:
        config = load_config(config_path)
        client = ClobPriceClient(
            timeout_seconds=config.timeout_seconds,
            max_attempts=config.max_attempts,
        )
        gcs_client = create_gcs_client(src_dir)
        bucket = gcs_client.bucket(config.bucket_name)
        repository = PriceRepository.from_environment(src_dir)
    except Exception as exc:
        if repository:
            repository.close()
        print(f"ERROR: failed to initialize CLOB price ingestion: {exc}", file=sys.stderr)
        return 1

    print(
        "Starting Polymarket CLOB price poller. "
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
                print(f"ERROR: CLOB price cycle failed: {exc}", file=sys.stderr)
            time.sleep(config.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0
    finally:
        repository.close()
