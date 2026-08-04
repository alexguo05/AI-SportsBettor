"""Reconcile resolution outcomes for Polymarket markets that left the open feed.

The open-event discovery poller filters on ``closed=false``, so a market that
closes simply disappears from that feed and its stored state freezes. This
poller re-queries Gamma by event ID for markets whose resolution is still
unknown, archives the raw responses, and persists final outcome state
(``closed`` flags, ``outcomePrices``, UMA resolution status, winning outcome).
"""

from __future__ import annotations

import argparse
import gzip
import random
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import requests

from src.common.gcs import canonical_json_bytes, create_gcs_client
from src.ingest_odds.polymarket_pipeline import (
    content_sha256,
    normalize_event,
    structural_fingerprint,
)

GAMMA_EVENTS_BY_ID_URL = "https://gamma-api.polymarket.com/events"
SCHEMA_NAME = "polymarket_gamma_resolutions"
SCHEMA_VERSION = 1
STORAGE_PROVIDER = "polymarket"
STORAGE_SOURCE = "gamma"
STORAGE_OBJECT = "resolutions"
CURSOR_STREAM = "gamma_event_resolutions"


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class ResolutionConfig:
    bucket_name: str = "ai-sports-bettor"
    poll_interval_seconds: int = 3600
    batch_size: int = 20
    max_event_age_days: int = 45
    timeout_seconds: float = 30
    max_attempts: int = 5

    def __post_init__(self) -> None:
        if self.poll_interval_seconds < 1:
            raise ValueError("resolution_poll_interval_seconds must be positive")
        if not 1 <= self.batch_size <= 100:
            raise ValueError("resolution_batch_size must be between 1 and 100")
        if self.max_event_age_days < 1:
            raise ValueError("resolution_max_event_age_days must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("resolution_timeout_seconds must be positive")
        if self.max_attempts < 1:
            raise ValueError("resolution_max_attempts must be positive")


@dataclass
class ResolutionResult:
    api_pages: list[list[dict[str, Any]]] = field(default_factory=list)
    request_batches: list[dict[str, Any]] = field(default_factory=list)

    @property
    def events(self) -> list[dict[str, Any]]:
        by_id: dict[str, dict[str, Any]] = {}
        for page in self.api_pages:
            for event in page:
                if isinstance(event, dict) and event.get("id") is not None:
                    by_id[str(event["id"])] = event
        return list(by_id.values())


class ResolutionRepositoryProtocol(Protocol):
    def load_checkpoint(self) -> dict[str, Any]: ...

    def load_pending_event_ids(self, *, cutoff: datetime | None) -> list[str]: ...

    def persist_records(self, envelope: dict[str, Any]) -> None: ...

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None: ...


def load_resolution_config(path: Path) -> ResolutionConfig:
    import json

    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    return ResolutionConfig(
        bucket_name=str(payload.get("gcs_bucket", "ai-sports-bettor")),
        poll_interval_seconds=int(payload.get("resolution_poll_interval_seconds", 3600)),
        batch_size=int(payload.get("resolution_batch_size", 20)),
        max_event_age_days=int(payload.get("resolution_max_event_age_days", 45)),
        timeout_seconds=float(payload.get("polymarket_timeout_seconds", 30)),
        max_attempts=int(payload.get("polymarket_max_attempts", 5)),
    )


def query_fingerprint(config: ResolutionConfig) -> str:
    return content_sha256(
        {
            "endpoint": GAMMA_EVENTS_BY_ID_URL,
            "schema_name": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
        }
    )


class GammaResolutionClient:
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

    def _request_batch(
        self,
        event_ids: list[str],
        batch_number: int,
    ) -> tuple[list[dict[str, Any]], int]:
        params = [("id", event_id) for event_id in event_ids]
        retryable_statuses = {429, 500, 502, 503, 504}
        for attempt in range(1, self.max_attempts + 1):
            response: requests.Response | None = None
            try:
                response = self.session.get(
                    GAMMA_EVENTS_BY_ID_URL,
                    params=params,
                    headers=self.headers,
                    timeout=self.timeout_seconds,
                )
                if response.status_code not in retryable_statuses:
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, list) or not all(
                        isinstance(event, dict) for event in payload
                    ):
                        raise ValueError(
                            "Gamma events-by-id response must be a list of objects"
                        )
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
                f"WARNING: Gamma resolution batch {batch_number} returned {status}; "
                f"retrying attempt {attempt + 1}/{self.max_attempts} in {delay:.1f}s",
                file=sys.stderr,
            )
            self.sleep(delay)
        raise RuntimeError("unreachable Gamma resolution retry state")

    def fetch_events_by_id(
        self,
        *,
        event_ids: list[str],
        batch_size: int,
    ) -> ResolutionResult:
        result = ResolutionResult()
        batches = [
            event_ids[offset : offset + batch_size]
            for offset in range(0, len(event_ids), batch_size)
        ]
        for batch_number, id_batch in enumerate(batches, start=1):
            payload, attempts = self._request_batch(id_batch, batch_number)
            returned_ids = {
                str(event["id"]) for event in payload if event.get("id") is not None
            }
            missing = sorted(set(id_batch) - returned_ids)
            if missing:
                print(
                    f"WARNING: Gamma resolution batch {batch_number} omitted "
                    f"{len(missing)} requested event(s): {', '.join(missing[:3])}",
                    file=sys.stderr,
                )
            result.api_pages.append(payload)
            result.request_batches.append(
                {
                    "batch_number": batch_number,
                    "requested_event_ids": id_batch,
                    "returned_event_count": len(returned_ids),
                    "omitted_event_ids": missing,
                    "attempts": attempts,
                }
            )
            print(
                f"Fetched Gamma resolution batch {batch_number}/{len(batches)}: "
                f"{len(returned_ids)} of {len(id_batch)} events"
            )
        return result


def build_object_path(ingested_at: datetime, ingest_run_id: str) -> str:
    utc = ingested_at.astimezone(UTC)
    return (
        f"raw/provider={STORAGE_PROVIDER}/source={STORAGE_SOURCE}/"
        f"object={STORAGE_OBJECT}/schema=v{SCHEMA_VERSION}/"
        f"date={utc:%Y-%m-%d}/hour={utc:%H}/"
        f"polymarket_resolutions_{ingest_run_id}.json.gz"
    )


def build_envelope(
    *,
    config: ResolutionConfig,
    result: ResolutionResult,
    records: list[dict[str, Any]],
    requested_event_ids: list[str],
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
        "content_sha256": content_sha256(result.api_pages),
        "structural_sha256": structural_fingerprint(records),
        "record_count": len(records),
        "request": {
            "endpoint": GAMMA_EVENTS_BY_ID_URL,
            "requested_event_ids": requested_event_ids,
            "batch_count": len(result.request_batches),
            "query_fingerprint": query_fingerprint(config),
            "batches": result.request_batches,
        },
        "records": records,
        "raw_api_responses": result.api_pages,
    }


def archive_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    """Remove transient normalized records; raw pages remain replayable."""
    return {key: value for key, value in envelope.items() if key != "records"}


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return gzip.compress(canonical_json_bytes(envelope), compresslevel=6, mtime=0)


def prepare_envelope(
    *,
    config: ResolutionConfig,
    client: GammaResolutionClient,
    event_ids: list[str],
    now: datetime | None = None,
) -> dict[str, Any]:
    observed_at = (now or utc_now()).astimezone(UTC)
    result = client.fetch_events_by_id(
        event_ids=event_ids,
        batch_size=config.batch_size,
    )
    records = [
        record
        for event in result.events
        if (record := normalize_event(event, observed_at)) is not None
    ]
    resolved_market_count = sum(
        1
        for record in records
        for market in record["markets"]
        if market["uma_resolution_status"] == "resolved"
    )
    print(
        f"Normalized {len(records)} events; "
        f"{resolved_market_count} markets carry a final UMA resolution"
    )
    ingest_run_id = uuid.uuid4().hex
    object_path = build_object_path(observed_at, ingest_run_id)
    return build_envelope(
        config=config,
        result=result,
        records=records,
        requested_event_ids=event_ids,
        ingest_run_id=ingest_run_id,
        ingested_at=observed_at,
        storage_uri=f"gs://{config.bucket_name}/{object_path}",
    )


def run_cycle(
    *,
    config: ResolutionConfig,
    client: GammaResolutionClient,
    bucket: Any,
    repository: ResolutionRepositoryProtocol,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    cycle_started_at = (now or utc_now()).astimezone(UTC)
    cutoff = cycle_started_at - timedelta(days=config.max_event_age_days)
    event_ids = repository.load_pending_event_ids(cutoff=cutoff)
    if not event_ids:
        print("No Polymarket events are awaiting resolution; skipping cycle")
        return None
    print(f"Starting Gamma resolution cycle for {len(event_ids)} pending events")
    envelope = prepare_envelope(
        config=config,
        client=client,
        event_ids=event_ids,
        now=cycle_started_at,
    )
    checkpoint = {
        "query_fingerprint": query_fingerprint(config),
        "last_structural_sha256": envelope["structural_sha256"],
        "updated_at": envelope["ingested_at"],
        "last_successful_poll_at": envelope["ingested_at"],
    }
    previous_checkpoint = repository.load_checkpoint()
    if (
        previous_checkpoint.get("query_fingerprint") == checkpoint["query_fingerprint"]
        and previous_checkpoint.get("last_structural_sha256")
        == checkpoint["last_structural_sha256"]
    ):
        repository.finalize_cycle(checkpoint)
        print(
            "Pending resolution states are unchanged; skipped GCS and state writes "
            "and advanced the successful-poll timestamp"
        )
        return checkpoint

    object_path = envelope["storage_uri"].split(f"gs://{config.bucket_name}/", 1)[1]
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
    print(f"Uploaded Gamma resolution envelope to {envelope['storage_uri']}")
    repository.persist_records(envelope)
    repository.finalize_cycle(checkpoint)
    print("Committed Polymarket resolution states and advanced the checkpoint")
    return checkpoint


def run_dry_run(
    *,
    config: ResolutionConfig,
    client: GammaResolutionClient,
    event_ids: list[str],
    now: datetime | None = None,
) -> dict[str, Any]:
    envelope = prepare_envelope(
        config=config,
        client=client,
        event_ids=event_ids,
        now=now,
    )
    encoded = encode_envelope(archive_envelope(envelope))
    print("DRY RUN: no GCS, PostgreSQL, or checkpoint writes will occur")
    print(f"Planned GCS URI: {envelope['storage_uri']}")
    print(
        f"Envelope: {len(envelope['raw_api_responses'])} raw batches, "
        f"{len(encoded)} compressed bytes, SHA-256 {envelope['content_sha256']}"
    )
    for record in envelope["records"][:5]:
        for market in record["markets"]:
            print(
                f"  Market {market['market_id']} ({market['question'][:60]!r}): "
                f"closed={market['closed']} "
                f"uma={market['uma_resolution_status']} "
                f"prices={market['outcome_prices']} "
                f"winner_index={market['winning_outcome_index']}"
            )
    if len(envelope["records"]) > 5:
        print(f"  ... {len(envelope['records']) - 5} additional events")
    return envelope


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--event-id",
        action="append",
        default=[],
        help="Optional event ID for dry-run mode; otherwise pending events are "
        "read from PostgreSQL",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of pending PostgreSQL events used by a dry run without "
        "--event-id (default: 10)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one live reconciliation cycle instead of polling continuously",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    from src.db.odds_repository import ResolutionRepository

    args = build_parser().parse_args(argv)
    src_dir = Path(__file__).resolve().parents[1]
    config_path = src_dir / "config" / "polymarket_config.json"
    try:
        config = load_resolution_config(config_path)
        client = GammaResolutionClient(
            timeout_seconds=config.timeout_seconds,
            max_attempts=config.max_attempts,
        )
        if args.dry_run:
            if args.limit < 1:
                raise ValueError("--limit must be positive")
            event_ids = list(dict.fromkeys(args.event_id))
            if not event_ids:
                repository = ResolutionRepository.from_environment(src_dir)
                try:
                    cutoff = utc_now() - timedelta(days=config.max_event_age_days)
                    event_ids = repository.load_pending_event_ids(cutoff=cutoff)[
                        : args.limit
                    ]
                finally:
                    repository.close()
                if not event_ids:
                    raise ValueError(
                        "PostgreSQL contains no events awaiting resolution"
                    )
                print(
                    f"DRY RUN: loaded {len(event_ids)} pending event IDs from PostgreSQL",
                    file=sys.stderr,
                )
            run_dry_run(config=config, client=client, event_ids=event_ids)
            return 0
    except Exception as exc:
        print(
            f"ERROR: failed to initialize Gamma resolution ingestion: {exc}",
            file=sys.stderr,
        )
        return 1

    repository: ResolutionRepository | None = None
    try:
        gcs_client = create_gcs_client(src_dir)
        bucket = gcs_client.bucket(config.bucket_name)
        repository = ResolutionRepository.from_environment(src_dir)
    except Exception as exc:
        if repository:
            repository.close()
        print(
            f"ERROR: failed to initialize Gamma resolution storage: {exc}",
            file=sys.stderr,
        )
        return 1

    print(
        "Starting Polymarket Gamma resolution poller. "
        f"Interval: {config.poll_interval_seconds}s. "
        f"Batch size: {config.batch_size}. Bucket: {config.bucket_name}"
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
                print(f"ERROR: Gamma resolution cycle failed: {exc}", file=sys.stderr)
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(config.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0
    finally:
        repository.close()
