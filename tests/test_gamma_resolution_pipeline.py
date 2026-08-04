import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from src.db.odds_repository import (
    ResolutionRepository,
    market_resolution_values,
)
from src.ingest_odds.gamma_resolution_pipeline import (
    GammaResolutionClient,
    ResolutionConfig,
    archive_envelope,
    build_object_path,
    run_cycle,
    run_dry_run,
)
from src.ingest_odds.polymarket_pipeline import (
    derive_winning_outcome_index,
    normalize_event,
)

NOW = datetime(2026, 8, 4, 18, 0, tzinfo=UTC)
FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "polymarket" / "gamma_nfl_event.json"
)


def fixture_event() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def resolved_event() -> dict[str, Any]:
    payload = fixture_event()
    payload["closed"] = True
    payload["closedTime"] = "2026-08-03T23:50:00Z"
    for market in payload["markets"]:
        market["closed"] = True
        market["acceptingOrders"] = False
        market["umaResolutionStatus"] = "resolved"
        market["closedTime"] = "2026-08-03 23:45:00+00"
    payload["markets"][0]["outcomePrices"] = '["1","0"]'
    payload["markets"][1]["outcomePrices"] = '["0","1"]'
    return payload


class FakeResponse:
    def __init__(self, payload: Any, *, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers: dict[str, str] = {}

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
    def __init__(
        self,
        pending_event_ids: list[str],
        checkpoint: dict[str, Any] | None = None,
    ) -> None:
        self.pending_event_ids = pending_event_ids
        self.checkpoint = checkpoint or {}
        self.envelopes: list[dict[str, Any]] = []
        self.checkpoints: list[dict[str, Any]] = []
        self.cutoffs: list[datetime | None] = []

    def load_checkpoint(self) -> dict[str, Any]:
        return self.checkpoint

    def load_pending_event_ids(self, *, cutoff: datetime | None) -> list[str]:
        self.cutoffs.append(cutoff)
        return self.pending_event_ids

    def persist_records(self, envelope: dict[str, Any]) -> None:
        self.envelopes.append(envelope)

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        self.checkpoints.append(checkpoint)
        self.checkpoint = checkpoint


class FakeConnection:
    def __init__(self) -> None:
        self.statements: list[Any] = []

    def execute(self, statement: Any) -> list[Any]:
        self.statements.append(statement)
        return []

    def scalar(self, statement: Any) -> None:
        self.statements.append(statement)
        return None


class FakeTransaction:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeConnection:
        return self.connection

    def __exit__(self, *_args: object) -> None:
        return None


class FakeEngine:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def begin(self) -> FakeTransaction:
        return FakeTransaction(self.connection)

    def connect(self) -> FakeTransaction:
        return FakeTransaction(self.connection)


def test_winning_outcome_requires_unambiguous_resolved_settlement() -> None:
    assert derive_winning_outcome_index("resolved", ["1", "0"]) == 0
    assert derive_winning_outcome_index("resolved", ["0", "1"]) == 1
    assert derive_winning_outcome_index(None, ["1", "0"]) is None
    assert derive_winning_outcome_index("proposed", ["1", "0"]) is None
    assert derive_winning_outcome_index("resolved", ["0.5", "0.5"]) is None
    assert derive_winning_outcome_index("resolved", ["1", "1"]) is None
    assert derive_winning_outcome_index("resolved", ["1", "junk"]) is None
    assert derive_winning_outcome_index("resolved", []) is None


def test_normalization_carries_resolution_state() -> None:
    record = normalize_event(resolved_event(), NOW)

    assert record is not None
    team_market, player_market = record["markets"]
    assert team_market["closed"] is True
    assert team_market["uma_resolution_status"] == "resolved"
    assert team_market["outcome_prices"] == ["1", "0"]
    assert team_market["winning_outcome_index"] == 0
    assert team_market["closed_time"] == "2026-08-03T23:45:00+00:00"
    assert player_market["winning_outcome_index"] == 1


def test_open_market_normalization_has_no_winner() -> None:
    record = normalize_event(fixture_event(), NOW)

    assert record is not None
    market = record["markets"][0]
    assert market["uma_resolution_status"] is None
    assert market["winning_outcome_index"] is None
    assert market["closed_time"] is None
    assert market["outcome_prices"] == ["0.55", "0.45"]


def test_market_resolution_values_map_relational_columns() -> None:
    record = normalize_event(resolved_event(), NOW)
    assert record is not None

    row = market_resolution_values(record["markets"][0], "a" * 32)

    assert row["market_id"] == "market-team"
    assert row["outcome_prices"] == ["1", "0"]
    assert row["uma_resolution_status"] == "resolved"
    assert row["winning_outcome_index"] == 0
    assert row["closed_time"] == datetime(2026, 8, 3, 23, 45, tzinfo=UTC)
    assert row["resolution_observed_at"] == NOW
    assert row["last_observed_at"] == NOW


def test_client_batches_event_ids_and_reports_omissions(
    capsys: pytest.CaptureFixture[str],
) -> None:
    first_event = resolved_event()
    second_event = resolved_event()
    second_event["id"] = "event-200"
    session = FakeSession(
        [
            FakeResponse([first_event, second_event]),
            FakeResponse([]),
        ]
    )
    client = GammaResolutionClient(session=session, sleep=lambda _seconds: None)

    result = client.fetch_events_by_id(
        event_ids=["event-100", "event-200", "event-300"],
        batch_size=2,
    )

    assert session.calls[0]["params"] == [("id", "event-100"), ("id", "event-200")]
    assert session.calls[1]["params"] == [("id", "event-300")]
    assert {event["id"] for event in result.events} == {"event-100", "event-200"}
    assert result.request_batches[1]["omitted_event_ids"] == ["event-300"]
    assert "omitted 1 requested event(s)" in capsys.readouterr().err


def test_storage_path_matches_shared_provider_source_object_contract() -> None:
    assert build_object_path(NOW, "a" * 32) == (
        "raw/provider=polymarket/source=gamma/object=resolutions/schema=v1/"
        "date=2026-08-04/hour=18/"
        f"polymarket_resolutions_{'a' * 32}.json.gz"
    )


def test_cycle_archives_then_persists_and_advances_checkpoint() -> None:
    client = GammaResolutionClient(
        session=FakeSession([FakeResponse([resolved_event()])])
    )
    bucket = FakeBucket()
    repository = FakeRepository(pending_event_ids=["event-100"])

    checkpoint = run_cycle(
        config=ResolutionConfig(),
        client=client,
        bucket=bucket,
        repository=repository,
        now=NOW,
    )

    assert checkpoint is not None
    assert repository.checkpoints == [checkpoint]
    assert len(repository.envelopes) == 1
    envelope = repository.envelopes[0]
    assert envelope["provider"] == "polymarket"
    assert envelope["source"] == "gamma"
    assert envelope["object_type"] == "resolutions"
    assert envelope["request"]["requested_event_ids"] == ["event-100"]
    assert envelope["records"][0]["markets"][0]["winning_outcome_index"] == 0
    assert repository.cutoffs == [
        NOW - timedelta(days=ResolutionConfig().max_event_age_days)
    ]
    assert bucket.blobs[0].data is not None
    assert "records" not in archive_envelope(envelope)


def test_cycle_skips_when_nothing_is_pending() -> None:
    client = GammaResolutionClient(session=FakeSession([]))
    repository = FakeRepository(pending_event_ids=[])

    checkpoint = run_cycle(
        config=ResolutionConfig(),
        client=client,
        bucket=FakeBucket(),
        repository=repository,
        now=NOW,
    )

    assert checkpoint is None
    assert repository.envelopes == []
    assert repository.checkpoints == []


def test_unchanged_resolution_state_skips_gcs_and_state_writes() -> None:
    payload = resolved_event()
    config = ResolutionConfig()
    bucket = FakeBucket()
    repository = FakeRepository(pending_event_ids=["event-100"])

    run_cycle(
        config=config,
        client=GammaResolutionClient(
            session=FakeSession([FakeResponse([copy.deepcopy(payload)])])
        ),
        bucket=bucket,
        repository=repository,
        now=NOW,
    )
    run_cycle(
        config=config,
        client=GammaResolutionClient(
            session=FakeSession([FakeResponse([copy.deepcopy(payload)])])
        ),
        bucket=bucket,
        repository=repository,
        now=NOW.replace(hour=19),
    )

    assert len(bucket.blobs) == 1
    assert len(repository.envelopes) == 1
    assert len(repository.checkpoints) == 2


def test_failed_raw_upload_does_not_write_database_or_checkpoint() -> None:
    client = GammaResolutionClient(
        session=FakeSession([FakeResponse([resolved_event()])])
    )
    repository = FakeRepository(pending_event_ids=["event-100"])

    with pytest.raises(RuntimeError, match="upload failed"):
        run_cycle(
            config=ResolutionConfig(),
            client=client,
            bucket=FakeBucket(fail=True),
            repository=repository,
            now=NOW,
        )

    assert repository.envelopes == []
    assert repository.checkpoints == []


def test_dry_run_previews_resolutions_without_storage_dependencies(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = GammaResolutionClient(
        session=FakeSession([FakeResponse([resolved_event()])])
    )

    envelope = run_dry_run(
        config=ResolutionConfig(),
        client=client,
        event_ids=["event-100"],
        now=NOW,
    )

    output = capsys.readouterr().out
    assert "DRY RUN: no GCS, PostgreSQL, or checkpoint writes" in output
    assert "winner_index=0" in output
    assert envelope["record_count"] == 1
    assert envelope["storage_uri"].startswith(
        "gs://ai-sports-bettor/raw/provider=polymarket/source=gamma/"
        "object=resolutions/"
    )


def test_repository_sql_updates_resolution_state_without_missing_sweep() -> None:
    record = normalize_event(resolved_event(), NOW)
    assert record is not None
    connection = FakeConnection()
    resources = SimpleNamespace(
        engine=FakeEngine(connection),
        close=lambda: None,
    )
    repository = ResolutionRepository(resources)  # type: ignore[arg-type]
    envelope = {
        "ingest_run_id": "a" * 32,
        "provider": "polymarket",
        "source": "gamma",
        "object_type": "resolutions",
        "schema_name": "polymarket_gamma_resolutions",
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
    repository.load_pending_event_ids(cutoff=NOW)

    sql = "\n".join(
        str(statement.compile(dialect=postgresql.dialect()))
        for statement in connection.statements
    )
    assert "uma_resolution_status" in sql
    assert "winning_outcome_index" in sql
    assert "ORDER BY polymarket_event_versions.observed_at DESC" in sql
    assert "ORDER BY polymarket_market_versions.observed_at DESC" in sql
    assert "least(polymarket_events.first_observed_at" in sql
    assert "least(polymarket_markets.first_observed_at" in sql
    assert "excluded.last_successful_poll_at >= ingest_cursors.last_successful_poll_at" in sql
    assert "IS DISTINCT FROM" in sql
    assert "polymarket_events.missing_since >" in sql
    # Partial resolution fetches must never sweep other rows into missing state.
    assert "NOT IN" not in sql
    assert "INSERT INTO job_outbox" not in sql
