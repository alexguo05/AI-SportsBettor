"""Raw-first Polymarket Gamma NFL event discovery."""

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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import requests

from src.common.gcs import canonical_json_bytes, create_gcs_client

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events/keyset"
SCHEMA_NAME = "polymarket_gamma_events"
SCHEMA_VERSION = 1
STORAGE_PROVIDER = "polymarket"
STORAGE_SOURCE = "gamma"
STORAGE_OBJECT = "events"
CURSOR_STREAM = "gamma_nfl_events"

EVENT_STRUCTURAL_FIELDS = (
    "id",
    "ticker",
    "slug",
    "title",
    "description",
    "category",
    "active",
    "closed",
    "archived",
    "startDate",
    "startDateIso",
    "endDate",
    "endDateIso",
    "gameStartTime",
    "closedTime",
    "negRisk",
    "enableNegRisk",
    "gameId",
)
MARKET_STRUCTURAL_FIELDS = (
    "id",
    "question",
    "conditionId",
    "slug",
    "description",
    "resolutionSource",
    "startDate",
    "startDateIso",
    "endDate",
    "endDateIso",
    "gameStartTime",
    "closedTime",
    "sportsMarketType",
    "line",
    "groupItemTitle",
    "groupItemThreshold",
    "outcomes",
    "clobTokenIds",
    "active",
    "closed",
    "archived",
    "acceptingOrders",
    "enableOrderBook",
    "resolvedBy",
    "umaResolutionStatus",
    "negRisk",
    "negRiskMarketID",
    "orderPriceMinTickSize",
    "orderMinSize",
    "secondsDelay",
    "makerBaseFee",
    "takerBaseFee",
    "feeType",
    "umaBond",
    "umaReward",
    "questionID",
    "umaEndDate",
    "negRiskRequestID",
    "ready",
    "funded",
    "pendingDeployment",
    "deploying",
    "feesEnabled",
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_timestamp(value: Any) -> str | None:
    if not value:
        return None
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def content_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        return decoded if isinstance(decoded, list) else []
    return []


def _structural_projection(
    payload: dict[str, Any],
    fields: tuple[str, ...],
) -> dict[str, Any]:
    return {field: payload[field] for field in fields if field in payload}


def _event_structural_projection(payload: dict[str, Any]) -> dict[str, Any]:
    projection = _structural_projection(payload, EVENT_STRUCTURAL_FIELDS)
    projection["tags"] = sorted(
        (
            {key: tag[key] for key in ("id", "slug", "label") if key in tag}
            for tag in payload.get("tags", []) or []
            if isinstance(tag, dict)
        ),
        key=lambda tag: (str(tag.get("id", "")), str(tag.get("slug", ""))),
    )
    return projection


def _market_structural_projection(payload: dict[str, Any]) -> dict[str, Any]:
    projection = _structural_projection(payload, MARKET_STRUCTURAL_FIELDS)
    projection["outcomes"] = [str(value) for value in _json_list(payload.get("outcomes"))]
    projection["clobTokenIds"] = [
        str(value) for value in _json_list(payload.get("clobTokenIds"))
    ]
    return projection


@dataclass(frozen=True)
class PolymarketConfig:
    bucket_name: str = "ai-sports-bettor"
    tag_slug: str = "nfl"
    closed: bool | None = False
    page_size: int = 500
    max_pages: int = 100
    poll_interval_seconds: int = 900
    timeout_seconds: float = 30
    max_attempts: int = 5

    def __post_init__(self) -> None:
        if not self.tag_slug.strip():
            raise ValueError("polymarket_tag_slug cannot be blank")
        if not 1 <= self.page_size <= 500:
            raise ValueError("polymarket_page_size must be between 1 and 500")
        if self.max_pages < 1:
            raise ValueError("polymarket_max_pages must be positive")
        if self.poll_interval_seconds < 1:
            raise ValueError("polymarket_poll_interval_seconds must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("polymarket_timeout_seconds must be positive")
        if self.max_attempts < 1:
            raise ValueError("polymarket_max_attempts must be positive")


@dataclass
class GammaResult:
    api_pages: list[dict[str, Any]] = field(default_factory=list)
    request_pages: list[dict[str, Any]] = field(default_factory=list)

    @property
    def events(self) -> list[dict[str, Any]]:
        by_id: dict[str, dict[str, Any]] = {}
        for page in self.api_pages:
            for event in page.get("events", []) or []:
                if isinstance(event, dict) and event.get("id") is not None:
                    by_id[str(event["id"])] = event
        return list(by_id.values())


@dataclass(frozen=True)
class PreparedCycle:
    envelope: dict[str, Any]
    checkpoint: dict[str, Any]
    object_path: str
    market_count: int
    token_count: int


class PolymarketRepositoryProtocol(Protocol):
    def load_checkpoint(self) -> dict[str, Any]: ...

    def persist_records(self, envelope: dict[str, Any]) -> None: ...

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None: ...


def load_config(path: Path) -> PolymarketConfig:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    closed_value = payload.get("polymarket_closed", False)
    if closed_value is not None and not isinstance(closed_value, bool):
        raise ValueError("polymarket_closed must be true, false, or null")
    return PolymarketConfig(
        bucket_name=str(payload.get("gcs_bucket", "ai-sports-bettor")),
        tag_slug=str(payload.get("polymarket_tag_slug", "nfl")),
        closed=closed_value,
        page_size=int(payload.get("polymarket_page_size", 500)),
        max_pages=int(payload.get("polymarket_max_pages", 100)),
        poll_interval_seconds=int(payload.get("polymarket_poll_interval_seconds", 900)),
        timeout_seconds=float(payload.get("polymarket_timeout_seconds", 30)),
        max_attempts=int(payload.get("polymarket_max_attempts", 5)),
    )


def query_fingerprint(config: PolymarketConfig) -> str:
    query_contract = {
        "endpoint": GAMMA_EVENTS_URL,
        "tag_slug": config.tag_slug,
        "closed": config.closed,
        "page_size": config.page_size,
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
    }
    return content_sha256(query_contract)


class GammaClient:
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

    def _request_page(
        self,
        params: dict[str, str],
        page_number: int,
    ) -> tuple[requests.Response, int]:
        retryable_statuses = {429, 500, 502, 503, 504}
        for attempt in range(1, self.max_attempts + 1):
            response: requests.Response | None = None
            try:
                response = self.session.get(
                    GAMMA_EVENTS_URL,
                    params=params,
                    headers=self.headers,
                    timeout=self.timeout_seconds,
                )
                if response.status_code not in retryable_statuses:
                    response.raise_for_status()
                    return response, attempt
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
                f"WARNING: Gamma page {page_number} returned {status}; "
                f"retrying attempt {attempt + 1}/{self.max_attempts} in {delay:.1f}s",
                file=sys.stderr,
            )
            self.sleep(delay)
        raise RuntimeError("unreachable Gamma retry state")

    def fetch_events(
        self,
        *,
        tag_slug: str,
        closed: bool | None,
        page_size: int,
        max_pages: int,
    ) -> GammaResult:
        result = GammaResult()
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for page_number in range(1, max_pages + 1):
            params = {
                "tag_slug": tag_slug,
                "limit": str(page_size),
            }
            if closed is not None:
                params["closed"] = str(closed).lower()
            if cursor:
                params["after_cursor"] = cursor
            response, attempts = self._request_page(params, page_number)
            payload = response.json()
            if isinstance(payload, list):
                page = {"events": payload, "next_cursor": None}
            elif isinstance(payload, dict):
                raw_events = payload.get("events", [])
                if not isinstance(raw_events, list):
                    raise ValueError("Gamma events response has a non-list events field")
                page = payload
            else:
                raise ValueError("Gamma events response must be an object or list")
            result.api_pages.append(page)
            result.request_pages.append(
                {
                    "page_number": page_number,
                    "params": {
                        key: value for key, value in params.items() if key != "after_cursor"
                    },
                    "used_after_cursor": bool(cursor),
                    "attempts": attempts,
                }
            )
            event_count = len(page.get("events", []))
            print(
                f"Fetched Gamma page {page_number}: {event_count} events "
                f"(request attempts: {attempts})"
            )
            next_value = page.get("next_cursor") or page.get("nextCursor")
            next_cursor = str(next_value) if next_value else None
            if not next_cursor or not event_count:
                return result
            if next_cursor in seen_cursors:
                raise RuntimeError("Gamma pagination repeated a cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise RuntimeError(
            f"Gamma result exceeded polymarket_max_pages={max_pages}; "
            "checkpoint was not advanced"
        )


def normalize_market(
    event_id: str,
    payload: dict[str, Any],
    observed_at: datetime,
) -> dict[str, Any] | None:
    market_id = payload.get("id")
    question = payload.get("question")
    if market_id is None or not question:
        return None
    structural_state = _market_structural_projection(payload)
    outcomes = _json_list(payload.get("outcomes"))
    token_ids = _json_list(payload.get("clobTokenIds"))
    tokens = [
        {
            "token_id": str(token_id),
            "outcome_index": index,
            "outcome": str(outcomes[index]),
        }
        for index, token_id in enumerate(token_ids)
        if token_id is not None and index < len(outcomes)
    ]
    return {
        "market_id": str(market_id),
        "event_id": event_id,
        "condition_id": (
            str(payload["conditionId"]) if payload.get("conditionId") is not None else None
        ),
        "slug": payload.get("slug"),
        "question": str(question),
        "sports_market_type": payload.get("sportsMarketType"),
        "line": payload.get("line"),
        "active": bool(payload.get("active", False)),
        "closed": bool(payload.get("closed", False)),
        "accepting_orders": bool(payload.get("acceptingOrders", False)),
        "enable_order_book": bool(payload.get("enableOrderBook", False)),
        "observed_at": observed_at.isoformat(),
        "content_sha256": content_sha256(structural_state),
        "tokens": tokens,
    }


def normalize_event(payload: dict[str, Any], observed_at: datetime) -> dict[str, Any] | None:
    event_id = payload.get("id")
    title = payload.get("title")
    if event_id is None or not title:
        return None
    event_id_text = str(event_id)
    markets = [
        market
        for market_payload in payload.get("markets", []) or []
        if isinstance(market_payload, dict)
        and (market := normalize_market(event_id_text, market_payload, observed_at))
        is not None
    ]
    structural_state = _event_structural_projection(payload)
    return {
        "event_id": event_id_text,
        "slug": payload.get("slug"),
        "ticker": payload.get("ticker"),
        "title": str(title),
        "description": payload.get("description"),
        "category": payload.get("category"),
        "active": bool(payload.get("active", False)),
        "closed": bool(payload.get("closed", False)),
        "start_at": parse_timestamp(payload.get("startDate")),
        "end_at": parse_timestamp(payload.get("endDate")),
        "tags": structural_state["tags"],
        "observed_at": observed_at.isoformat(),
        "content_sha256": content_sha256(structural_state),
        "markets": markets,
    }


def structural_fingerprint(records: list[dict[str, Any]]) -> str:
    graph = [
        {
            "event_id": event["event_id"],
            "content_sha256": event["content_sha256"],
            "markets": [
                {
                    "market_id": market["market_id"],
                    "content_sha256": market["content_sha256"],
                    "tokens": market["tokens"],
                }
                for market in sorted(event["markets"], key=lambda item: item["market_id"])
            ],
        }
        for event in sorted(records, key=lambda item: item["event_id"])
    ]
    return content_sha256(graph)


def build_object_path(ingested_at: datetime, ingest_run_id: str) -> str:
    utc = ingested_at.astimezone(UTC)
    return (
        f"raw/provider={STORAGE_PROVIDER}/source={STORAGE_SOURCE}/"
        f"object={STORAGE_OBJECT}/schema=v{SCHEMA_VERSION}/"
        f"date={utc:%Y-%m-%d}/hour={utc:%H}/"
        f"polymarket_events_{ingest_run_id}.json.gz"
    )


def build_envelope(
    *,
    ingest_run_id: str,
    ingested_at: datetime,
    storage_uri: str,
    config: PolymarketConfig,
    result: GammaResult,
    records: list[dict[str, Any]],
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
            "endpoint": GAMMA_EVENTS_URL,
            "tag_slug": config.tag_slug,
            "closed": config.closed,
            "page_size": config.page_size,
            "page_count": len(result.api_pages),
            "query_fingerprint": query_fingerprint(config),
            "structural_sha256": structural_fingerprint(records),
            "pages": result.request_pages,
        },
        "records": records,
        "raw_api_responses": result.api_pages,
    }


def archive_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    """Remove transient normalized records; raw pages remain replayable."""
    return {key: value for key, value in envelope.items() if key != "records"}


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return gzip.compress(canonical_json_bytes(envelope), compresslevel=6, mtime=0)


def decode_envelope(data: bytes) -> dict[str, Any]:
    payload = json.loads(gzip.decompress(data))
    if payload.get("schema_name") != SCHEMA_NAME or payload.get("schema_version") != 1:
        raise ValueError("unsupported Polymarket Gamma envelope schema")
    return payload


def prepare_cycle(
    *,
    config: PolymarketConfig,
    client: GammaClient,
    now: datetime | None = None,
) -> PreparedCycle:
    cycle_started_at = (now or utc_now()).astimezone(UTC)
    print(
        f"Starting Polymarket Gamma cycle for tag_slug={config.tag_slug!r}, "
        f"closed={config.closed!r}"
    )
    result = client.fetch_events(
        tag_slug=config.tag_slug,
        closed=config.closed,
        page_size=config.page_size,
        max_pages=config.max_pages,
    )
    records = [
        record
        for event in result.events
        if (record := normalize_event(event, cycle_started_at)) is not None
    ]
    market_count = sum(len(record["markets"]) for record in records)
    token_count = sum(
        len(market["tokens"]) for record in records for market in record["markets"]
    )
    print(
        f"Normalized {len(records)} events, {market_count} markets, "
        f"and {token_count} outcome tokens"
    )
    ingest_run_id = uuid.uuid4().hex
    object_path = build_object_path(cycle_started_at, ingest_run_id)
    storage_uri = f"gs://{config.bucket_name}/{object_path}"
    envelope = build_envelope(
        ingest_run_id=ingest_run_id,
        ingested_at=cycle_started_at,
        storage_uri=storage_uri,
        config=config,
        result=result,
        records=records,
    )
    checkpoint = {
        "query_fingerprint": query_fingerprint(config),
        "last_structural_sha256": envelope["structural_sha256"],
        "updated_at": cycle_started_at.isoformat(),
        "last_successful_poll_at": cycle_started_at.isoformat(),
    }
    return PreparedCycle(
        envelope=envelope,
        checkpoint=checkpoint,
        object_path=object_path,
        market_count=market_count,
        token_count=token_count,
    )


def run_dry_cycle(
    *,
    config: PolymarketConfig,
    client: GammaClient,
    now: datetime | None = None,
) -> dict[str, Any]:
    prepared = prepare_cycle(config=config, client=client, now=now)
    envelope = prepared.envelope
    encoded = encode_envelope(archive_envelope(envelope))
    print("DRY RUN: no GCS, PostgreSQL, migration, or checkpoint writes will occur")
    print(f"Planned GCS URI: {envelope['storage_uri']}")
    print(
        f"Envelope: {len(envelope['raw_api_responses'])} raw pages, "
        f"{len(encoded)} compressed bytes, SHA-256 {envelope['content_sha256']}"
    )
    print(f"Structural SHA-256: {envelope['structural_sha256']}")
    print(
        f"Database preview: 1 raw object, {envelope['record_count']} events, "
        f"{prepared.market_count} markets, {prepared.token_count} outcome tokens"
    )
    for event in envelope["records"][:5]:
        print(
            f"  Event {event['event_id']}: {event['title']} "
            f"({len(event['markets'])} markets)"
        )
    if len(envelope["records"]) > 5:
        print(f"  ... {len(envelope['records']) - 5} additional events")
    return envelope


def run_cycle(
    *,
    config: PolymarketConfig,
    client: GammaClient,
    bucket: Any,
    repository: PolymarketRepositoryProtocol,
    now: datetime | None = None,
) -> dict[str, Any]:
    prepared = prepare_cycle(config=config, client=client, now=now)
    envelope = prepared.envelope
    previous_checkpoint = repository.load_checkpoint()
    if (
        previous_checkpoint.get("query_fingerprint")
        == prepared.checkpoint["query_fingerprint"]
        and previous_checkpoint.get("last_structural_sha256")
        == prepared.checkpoint["last_structural_sha256"]
    ):
        repository.finalize_cycle(prepared.checkpoint)
        print(
            "Polymarket structure is unchanged; skipped GCS and event-graph writes "
            "and advanced the successful-poll timestamp"
        )
        return prepared.checkpoint

    object_path = prepared.object_path
    blob = bucket.blob(object_path)
    blob.metadata = {
        "schema_name": SCHEMA_NAME,
        "schema_version": str(SCHEMA_VERSION),
        "content_sha256": envelope["content_sha256"],
        "structural_sha256": envelope["structural_sha256"],
        "record_count": str(envelope["record_count"]),
        "tag_slug": config.tag_slug,
    }
    blob.content_encoding = "gzip"
    blob.upload_from_string(
        encode_envelope(archive_envelope(envelope)),
        content_type="application/json",
    )
    print(f"Uploaded Polymarket Gamma envelope to {envelope['storage_uri']}")
    repository.persist_records(envelope)
    repository.finalize_cycle(prepared.checkpoint)
    print("Committed Polymarket event graph and advanced the Gamma checkpoint")
    return prepared.checkpoint


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect NFL events from Polymarket Gamma")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch and normalize one cycle without writing to GCS or PostgreSQL",
    )
    args = parser.parse_args(argv)
    src_dir = Path(__file__).resolve().parents[1]
    config_path = src_dir / "config" / "polymarket_config.json"
    try:
        config = load_config(config_path)
        client = GammaClient(
            timeout_seconds=config.timeout_seconds,
            max_attempts=config.max_attempts,
        )
    except Exception as exc:
        print(f"ERROR: failed to initialize Polymarket ingestion: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        try:
            run_dry_cycle(config=config, client=client)
        except Exception as exc:
            print(f"ERROR: Polymarket dry run failed: {exc}", file=sys.stderr)
            return 1
        return 0

    from src.db.odds_repository import OddsRepository

    repository: OddsRepository | None = None
    try:
        gcs_client = create_gcs_client(src_dir)
        bucket = gcs_client.bucket(config.bucket_name)
        repository = OddsRepository.from_environment(src_dir)
    except Exception as exc:
        if repository:
            repository.close()
        print(f"ERROR: failed to initialize Polymarket storage: {exc}", file=sys.stderr)
        return 1

    print(
        "Starting Polymarket Gamma poller. "
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
                print(f"ERROR: Polymarket cycle failed: {exc}", file=sys.stderr)
            time.sleep(config.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0
    finally:
        repository.close()
