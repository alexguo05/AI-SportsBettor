from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from src.db.odds_repository import PriceRepository
from src.ingest_odds.clob_price_pipeline import (
    ClobPriceClient,
    ClobPriceConfig,
    archive_envelope,
    build_object_path,
    choose_token_start,
    run_cycle,
)

NOW = datetime(2026, 7, 30, 22, 0, tzinfo=UTC)


class FakeResponse:
    status_code = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def json(self) -> dict[str, Any]:
        return self.payload

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def post(self, _url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append(kwargs)
        return self.responses.pop(0)


class FakeBlob:
    def __init__(self) -> None:
        self.metadata: dict[str, str] = {}
        self.content_encoding: str | None = None
        self.data: bytes | None = None

    def upload_from_string(self, data: bytes, **_kwargs: Any) -> None:
        self.data = data


class FakeBucket:
    def __init__(self) -> None:
        self.paths: list[str] = []
        self.blobs: list[FakeBlob] = []

    def blob(self, path: str) -> FakeBlob:
        self.paths.append(path)
        blob = FakeBlob()
        self.blobs.append(blob)
        return blob


class FakeRepository:
    def __init__(self) -> None:
        self.envelopes: list[dict[str, Any]] = []
        self.checkpoints: list[dict[str, Any]] = []

    def load_open_token_ids(self) -> list[str]:
        return ["token-a", "token-b"]

    def load_checkpoint(self) -> dict[str, Any]:
        return {}

    def load_price_cursors(self, _token_ids: list[str]) -> dict[str, dict[str, Any]]:
        return {}

    def persist_records(self, envelope: dict[str, Any]) -> None:
        self.envelopes.append(envelope)

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        self.checkpoints.append(checkpoint)


class FakeSqlResult:
    def first(self) -> tuple[str]:
        return ("token-a",)

    def mappings(self) -> "FakeSqlResult":
        return self

    def all(self) -> list[dict[str, Any]]:
        return []


class FakeSqlConnection:
    def __init__(self) -> None:
        self.statements: list[Any] = []

    def execute(self, statement: Any) -> FakeSqlResult:
        self.statements.append(statement)
        return FakeSqlResult()


class FakeBegin:
    def __init__(self, connection: FakeSqlConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeSqlConnection:
        return self.connection

    def __exit__(self, *_args: object) -> None:
        return None


class FakeEngine:
    def __init__(self, connection: FakeSqlConnection) -> None:
        self.connection = connection

    def begin(self) -> FakeBegin:
        return FakeBegin(self.connection)


def test_clob_batch_response_normalizes_price_points() -> None:
    session = FakeSession(
        [
            FakeResponse(
                {
                    "history": {
                        "token-a": [{"t": 1785448200, "p": 0.55}],
                        "token-b": [{"t": 1785448200, "p": 0.45}],
                    }
                }
            )
        ]
    )
    client = ClobPriceClient(session=session)

    result = client.fetch_history(
        token_ids=["token-a", "token-b"],
        start_ts=1785447900,
        end_ts=1785448800,
        fidelity_minutes=1,
        batch_size=20,
        observed_at=NOW,
    )

    assert len(result.points) == 2
    assert result.points[0]["source_timestamp"] == "2026-07-30T21:50:00+00:00"
    assert session.calls[0]["json"]["markets"] == ["token-a", "token-b"]


def test_clob_batch_rejects_omitted_requested_tokens() -> None:
    client = ClobPriceClient(
        session=FakeSession(
            [FakeResponse({"history": {"token-a": []}})]
        )
    )

    with pytest.raises(ValueError, match="omitted 1 requested token"):
        client.fetch_history(
            token_ids=["token-a", "token-b"],
            start_ts=1785447900,
            end_ts=1785448800,
            fidelity_minutes=1,
            batch_size=20,
            observed_at=NOW,
        )


def test_clob_batch_rejects_malformed_token_history() -> None:
    client = ClobPriceClient(
        session=FakeSession(
            [FakeResponse({"history": {"token-a": {"t": 1, "p": 0.5}}})]
        )
    )

    with pytest.raises(ValueError, match="must be a list"):
        client.fetch_history(
            token_ids=["token-a"],
            start_ts=1785447900,
            end_ts=1785448800,
            fidelity_minutes=1,
            batch_size=20,
            observed_at=NOW,
        )


def test_token_cursor_window_overlaps_one_fidelity_interval() -> None:
    config = ClobPriceConfig(fidelity_minutes=2)
    end_ts = int(NOW.timestamp())

    assert (
        choose_token_start(
            {"last_end_ts": 1000, "query_fingerprint": "wrong"},
            config,
            end_ts,
        )
        == end_ts - 900
    )

    from src.ingest_odds.clob_price_pipeline import query_fingerprint

    assert choose_token_start(
        {
            "last_end_ts": 1785448500,
            "query_fingerprint": query_fingerprint(config),
        },
        config,
        end_ts,
    ) == 1785448380


def test_clob_path_uses_shared_storage_contract() -> None:
    assert build_object_path(NOW, "a" * 32) == (
        "raw/provider=polymarket/source=clob/object=price-history/schema=v1/"
        "date=2026-07-30/hour=22/"
        f"polymarket_prices_{'a' * 32}.json.gz"
    )


def test_clob_cycle_uploads_raw_once_and_persists_points() -> None:
    session = FakeSession(
        [
            FakeResponse(
                {
                    "history": {
                        "token-a": [{"t": 1785448200, "p": 0.55}],
                        "token-b": [{"t": 1785448200, "p": 0.45}],
                    }
                }
            )
        ]
    )
    bucket = FakeBucket()
    repository = FakeRepository()

    checkpoint = run_cycle(
        config=ClobPriceConfig(),
        client=ClobPriceClient(session=session),
        bucket=bucket,
        repository=repository,
        now=NOW,
    )

    assert checkpoint is not None
    assert len(bucket.blobs) == 1
    assert bucket.blobs[0].data is not None
    assert len(repository.envelopes) == 1
    assert repository.envelopes[0]["record_count"] == 2
    assert "records" not in archive_envelope(repository.envelopes[0])
    assert repository.checkpoints == [checkpoint]


def test_price_repository_only_versions_new_or_corrected_values() -> None:
    connection = FakeSqlConnection()
    repository = PriceRepository(  # type: ignore[arg-type]
        SimpleNamespace(engine=FakeEngine(connection), close=lambda: None)
    )
    repository.persist_records(
        {
            "ingest_run_id": "a" * 32,
            "provider": "polymarket",
            "source": "clob",
            "object_type": "price-history",
            "schema_name": "polymarket_clob_price_history",
            "schema_version": 1,
            "storage_uri": "gs://bucket/prices.json.gz",
            "content_sha256": "b" * 64,
            "record_count": 1,
            "ingested_at": NOW.isoformat(),
            "request": {"query_fingerprint": "c" * 64},
            "_token_cursor_candidates": {"token-a": 1785448800},
            "records": [
                {
                    "token_id": "token-a",
                    "source_timestamp": "2026-07-30T21:50:00+00:00",
                    "price": "0.55",
                    "fidelity_minutes": 1,
                    "observed_at": NOW.isoformat(),
                }
            ],
        }
    )

    sql = "\n".join(
        str(statement.compile(dialect=postgresql.dialect()))
        for statement in connection.statements
    )
    assert "INSERT INTO polymarket_price_point_versions" in sql
    assert "SELECT polymarket_price_points.token_id" in sql
    assert "last_observed_at = excluded.last_observed_at" in sql
    assert "INSERT INTO polymarket_price_cursors" in sql
