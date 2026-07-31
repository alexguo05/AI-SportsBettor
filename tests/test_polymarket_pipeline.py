import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from src.db.models import polymarket_tokens
from src.db.odds_repository import (
    OddsRepository,
    event_values,
    market_values,
    should_append_version,
)
from src.ingest_odds.polymarket_pipeline import (
    GammaClient,
    PolymarketConfig,
    archive_envelope,
    build_object_path,
    decode_envelope,
    normalize_event,
    run_cycle,
    run_dry_cycle,
    structural_fingerprint,
)

NOW = datetime(2026, 7, 30, 21, 0, tzinfo=UTC)
FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "polymarket" / "gamma_nfl_event.json"
)


def fixture_event() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


class FakeResponse:
    def __init__(
        self,
        payload: Any,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def json(self) -> Any:
        return self.payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def get(self, _url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append(kwargs)
        return self.responses.pop(0)


class FakeBlob:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.metadata: dict[str, str] = {}
        self.content_encoding: str | None = None
        self.data: bytes | None = None

    def upload_from_string(self, data: bytes, **_kwargs: Any) -> None:
        if self.fail:
            raise RuntimeError("upload failed")
        self.data = data


class FakeBucket:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.paths: list[str] = []
        self.blobs: list[FakeBlob] = []

    def blob(self, path: str) -> FakeBlob:
        self.paths.append(path)
        blob = FakeBlob(self.fail)
        self.blobs.append(blob)
        return blob


class FakeRepository:
    def __init__(self, checkpoint: dict[str, Any] | None = None) -> None:
        self.envelopes: list[dict[str, Any]] = []
        self.checkpoints: list[dict[str, Any]] = []
        self.checkpoint = checkpoint or {}

    def load_checkpoint(self) -> dict[str, Any]:
        return self.checkpoint

    def persist_records(self, envelope: dict[str, Any]) -> None:
        self.envelopes.append(envelope)

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        self.checkpoints.append(checkpoint)
        self.checkpoint = checkpoint


class FakeConnection:
    def __init__(self) -> None:
        self.statements: list[Any] = []

    def execute(self, statement: Any) -> None:
        self.statements.append(statement)

    def scalar(self, statement: Any) -> None:
        self.statements.append(statement)
        return None


class FakeBegin:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeConnection:
        return self.connection

    def __exit__(self, *_args: object) -> None:
        return None


class FakeEngine:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def begin(self) -> FakeBegin:
        return FakeBegin(self.connection)


def test_gamma_keyset_pagination_uses_nfl_scope_and_cursor() -> None:
    session = FakeSession(
        [
            FakeResponse(
                {"events": [fixture_event()], "next_cursor": "next-page"}
            ),
            FakeResponse({"events": [], "next_cursor": None}),
        ]
    )
    client = GammaClient(session=session, sleep=lambda _seconds: None)

    result = client.fetch_events(
        tag_slug="nfl",
        closed=None,
        page_size=500,
        max_pages=3,
    )

    assert len(result.events) == 1
    assert session.calls[0]["params"] == {"tag_slug": "nfl", "limit": "500"}
    assert session.calls[1]["params"]["after_cursor"] == "next-page"


def test_normalization_extracts_markets_and_clob_tokens() -> None:
    record = normalize_event(fixture_event(), NOW)

    assert record is not None
    assert record["event_id"] == "event-100"
    assert len(record["markets"]) == 2
    assert record["markets"][0]["tokens"] == [
        {"token_id": "token-yes", "outcome_index": 0, "outcome": "Yes"},
        {"token_id": "token-no", "outcome_index": 1, "outcome": "No"},
    ]
    assert record["start_at"] == "2026-09-10T17:00:00+00:00"


def test_storage_path_matches_shared_provider_source_object_contract() -> None:
    assert build_object_path(NOW, "a" * 32) == (
        "raw/provider=polymarket/source=gamma/object=events/schema=v1/"
        "date=2026-07-30/hour=21/"
        f"polymarket_events_{'a' * 32}.json.gz"
    )


def test_cycle_archives_then_persists_and_advances_checkpoint() -> None:
    client = GammaClient(
        session=FakeSession(
            [FakeResponse({"events": [fixture_event()], "next_cursor": None})]
        )
    )
    bucket = FakeBucket()
    repository = FakeRepository()

    checkpoint = run_cycle(
        config=PolymarketConfig(),
        client=client,
        bucket=bucket,
        repository=repository,
        now=NOW,
    )

    assert len(repository.envelopes) == 1
    assert len(repository.checkpoints) == 1
    assert repository.checkpoints[0] == checkpoint
    assert repository.envelopes[0]["provider"] == "polymarket"
    assert repository.envelopes[0]["source"] == "gamma"
    assert repository.envelopes[0]["record_count"] == 1
    assert bucket.blobs[0].data is not None
    decoded = decode_envelope(bucket.blobs[0].data)
    assert "records" not in decoded
    assert decoded["raw_api_responses"][0]["events"][0]["id"] == "event-100"


def test_failed_raw_upload_does_not_write_database_or_checkpoint() -> None:
    client = GammaClient(
        session=FakeSession(
            [FakeResponse({"events": [fixture_event()], "next_cursor": None})]
        )
    )
    repository = FakeRepository()

    with pytest.raises(RuntimeError, match="upload failed"):
        run_cycle(
            config=PolymarketConfig(),
            client=client,
            bucket=FakeBucket(fail=True),
            repository=repository,
            now=NOW,
        )

    assert repository.envelopes == []
    assert repository.checkpoints == []


def test_dry_run_previews_envelope_without_storage_dependencies(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = GammaClient(
        session=FakeSession(
            [FakeResponse({"events": [fixture_event()], "next_cursor": None})]
        )
    )

    envelope = run_dry_cycle(
        config=PolymarketConfig(),
        client=client,
        now=NOW,
    )

    output = capsys.readouterr().out
    assert "DRY RUN: no GCS, PostgreSQL, migration, or checkpoint writes" in output
    assert "Database preview: 1 raw object, 1 events, 2 markets, 4 outcome tokens" in output
    assert "Kansas City Chiefs vs Buffalo Bills" in output
    assert envelope["record_count"] == 1
    assert envelope["storage_uri"].startswith(
        "gs://ai-sports-bettor/raw/provider=polymarket/source=gamma/"
    )


def test_volatile_gamma_prices_do_not_change_structural_fingerprint() -> None:
    original = fixture_event()
    changed_prices = copy.deepcopy(original)
    changed_prices["volume"] = 999999
    changed_prices["markets"][0]["outcomePrices"] = "[\"0.10\",\"0.90\"]"
    changed_prices["markets"][0]["bestBid"] = 0.09
    original_record = normalize_event(original, NOW)
    changed_record = normalize_event(changed_prices, NOW)
    assert original_record is not None
    assert changed_record is not None

    assert structural_fingerprint([original_record]) == structural_fingerprint(
        [changed_record]
    )

    changed_prices["markets"][0]["closed"] = True
    closed_record = normalize_event(changed_prices, NOW)
    assert closed_record is not None
    assert structural_fingerprint([original_record]) != structural_fingerprint(
        [closed_record]
    )


def test_unchanged_structure_skips_gcs_and_graph_writes() -> None:
    event = fixture_event()
    config = PolymarketConfig()
    first_client = GammaClient(
        session=FakeSession([FakeResponse({"events": [event], "next_cursor": None})])
    )
    second_payload = copy.deepcopy(event)
    second_payload["markets"][0]["outcomePrices"] = "[\"0.40\",\"0.60\"]"
    second_client = GammaClient(
        session=FakeSession(
            [FakeResponse({"events": [second_payload], "next_cursor": None})]
        )
    )
    bucket = FakeBucket()
    repository = FakeRepository()

    run_cycle(
        config=config,
        client=first_client,
        bucket=bucket,
        repository=repository,
        now=NOW,
    )
    run_cycle(
        config=config,
        client=second_client,
        bucket=bucket,
        repository=repository,
        now=NOW.replace(minute=15),
    )

    assert len(bucket.blobs) == 1
    assert len(repository.envelopes) == 1
    assert len(repository.checkpoints) == 2
    assert "records" not in archive_envelope(repository.envelopes[0])


def test_relational_mapping_and_reversion_version_rule() -> None:
    record = normalize_event(fixture_event(), NOW)
    assert record is not None

    event_row = event_values(record, "a" * 32)
    market_row = market_values(record["markets"][0], "a" * 32)

    assert event_row["event_id"] == "event-100"
    assert event_row["start_at"] == datetime(2026, 9, 10, 17, 0, tzinfo=UTC)
    assert market_row["market_id"] == "market-team"
    assert market_row["last_observed_at"] == NOW
    assert should_append_version(None, "A")
    assert not should_append_version("A", "A")
    assert should_append_version("A", "B")
    assert should_append_version("B", "A")


def test_repository_sql_guards_stale_state_and_checkpoint_updates() -> None:
    record = normalize_event(fixture_event(), NOW)
    assert record is not None
    connection = FakeConnection()
    resources = SimpleNamespace(
        engine=FakeEngine(connection),
        close=lambda: None,
    )
    repository = OddsRepository(resources)  # type: ignore[arg-type]
    envelope = {
        "ingest_run_id": "a" * 32,
        "provider": "polymarket",
        "source": "gamma",
        "object_type": "events",
        "schema_name": "polymarket_gamma_events",
        "schema_version": 1,
        "storage_uri": "gs://bucket/object.json.gz",
        "content_sha256": "b" * 64,
        "record_count": 1,
        "ingested_at": NOW.isoformat(),
        "request": {},
        "records": [record],
    }

    repository.persist_records(envelope)
    repository.finalize_cycle(
        {
            "query_fingerprint": "c" * 64,
            "last_structural_sha256": "d" * 64,
            "updated_at": NOW.isoformat(),
            "last_successful_poll_at": NOW.isoformat(),
        }
    )

    sql = "\n".join(
        str(statement.compile(dialect=postgresql.dialect()))
        for statement in connection.statements
    )
    assert "ORDER BY polymarket_event_versions.observed_at DESC" in sql
    assert "ORDER BY polymarket_market_versions.observed_at DESC" in sql
    assert "least(polymarket_events.first_observed_at" in sql
    assert "least(polymarket_markets.first_observed_at" in sql
    assert "least(polymarket_tokens.first_observed_at" in sql
    assert "excluded.last_successful_poll_at >= ingest_cursors.last_successful_poll_at" in sql
    assert "polymarket_events.missing_since IS NOT NULL" in sql
    assert "polymarket_events.missing_since IS NULL" in sql
    assert "polymarket_events.last_observed_at <=" in sql
    assert "polymarket_events.event_id NOT IN" in sql
    assert "polymarket_markets.missing_since IS NOT NULL" in sql
    assert "polymarket_markets.missing_since IS NULL" in sql
    assert "polymarket_markets.last_observed_at <=" in sql
    assert "polymarket_markets.market_id NOT IN" in sql
    assert not any(
        constraint.__class__.__name__ == "UniqueConstraint"
        for constraint in polymarket_tokens.constraints
    )
