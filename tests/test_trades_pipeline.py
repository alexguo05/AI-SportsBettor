from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from src.db.odds_repository import TradesRepository
from src.ingest_odds.trades_pipeline import (
    TradesClient,
    TradesConfig,
    build_object_path,
    fetch_new_trades,
    load_watermark,
    normalize_trade,
    query_fingerprint,
    run_cycle,
    run_dry_run,
)

NOW = datetime(2026, 8, 4, 18, 0, tzinfo=UTC)
NOW_EPOCH = int(NOW.timestamp())
CONDITION = "0x" + "a" * 64


def raw_trade(**overrides: Any) -> dict[str, Any]:
    payload = {
        "proxyWallet": "0xd98eb7d93990a77a07eee6ef65d60f0f9f02fbd9",
        "side": "BUY",
        "asset": "5552442384777186369011911104545487296206450549967025016601627",
        "conditionId": CONDITION,
        "size": 60,
        "price": 0.014165,
        "timestamp": NOW_EPOCH - 30,
        "title": "Tush Push banned for 2026 NFL Season?",
        "outcome": "Yes",
        "outcomeIndex": 0,
        "name": "someuser",
        "pseudonym": "Some-User",
        "transactionHash": "0x" + "b" * 64,
    }
    payload.update(overrides)
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
        condition_ids: list[str],
        checkpoint: dict[str, Any] | None = None,
    ) -> None:
        self.condition_ids = condition_ids
        self.checkpoint = checkpoint or {}
        self.envelopes: list[dict[str, Any]] = []
        self.checkpoints: list[dict[str, Any]] = []

    def load_checkpoint(self) -> dict[str, Any]:
        return self.checkpoint

    def load_open_condition_ids(self, *, missing_cutoff: datetime) -> list[str]:
        return self.condition_ids

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


def test_normalization_is_deterministic_and_strips_profile_noise() -> None:
    first = normalize_trade(raw_trade(), NOW)
    second = normalize_trade(raw_trade(), NOW)

    assert first["trade_uid"] == second["trade_uid"]
    assert first["traded_at"] == datetime.fromtimestamp(
        NOW_EPOCH - 30, tz=UTC
    ).isoformat()
    assert first["price"] == "0.014165"
    assert first["size"] == "60"
    assert first["outcome_index"] == 0
    assert "pseudonym" not in first
    assert "name" not in first

    different = normalize_trade(raw_trade(price=0.02), NOW)
    assert different["trade_uid"] != first["trade_uid"]


def test_normalization_rejects_invalid_trades() -> None:
    with pytest.raises(ValueError, match="asset or conditionId"):
        normalize_trade(raw_trade(asset=None), NOW)
    with pytest.raises(ValueError, match="unsupported side"):
        normalize_trade(raw_trade(side="HOLD"), NOW)
    with pytest.raises(ValueError, match="timestamp is invalid"):
        normalize_trade(raw_trade(timestamp="junk"), NOW)
    with pytest.raises(ValueError, match="trade.price"):
        normalize_trade(raw_trade(price="junk"), NOW)


def test_fetch_filters_old_trades_and_deduplicates_across_pages() -> None:
    fresh = raw_trade()
    stale = raw_trade(timestamp=NOW_EPOCH - 10_000, transactionHash="0x" + "c" * 64)
    config = TradesConfig(page_limit=2, market_batch_size=20)
    session = FakeSession(
        [
            FakeResponse([fresh, fresh]),
            FakeResponse([fresh, stale]),
        ]
    )
    client = TradesClient(session=session, sleep=lambda _seconds: None)

    result = fetch_new_trades(
        config=config,
        client=client,
        condition_ids=[CONDITION],
        floor=NOW - timedelta(seconds=120),
        observed_at=NOW,
    )

    # Second page ends pagination because its oldest trade is behind the floor.
    assert len(session.calls) == 2
    assert len(result.trades) == 1
    assert result.request_batches[0]["new_trade_count"] == 1
    assert result.request_batches[1]["new_trade_count"] == 0


def test_fetch_stops_on_short_page_and_batches_markets() -> None:
    second_condition = "0x" + "d" * 64
    config = TradesConfig(page_limit=500, market_batch_size=1)
    session = FakeSession(
        [
            FakeResponse([raw_trade()]),
            FakeResponse(
                [raw_trade(conditionId=second_condition, asset="9" * 60)]
            ),
        ]
    )
    client = TradesClient(session=session, sleep=lambda _seconds: None)

    result = fetch_new_trades(
        config=config,
        client=client,
        condition_ids=[CONDITION, second_condition],
        floor=NOW - timedelta(seconds=120),
        observed_at=NOW,
    )

    assert len(session.calls) == 2
    assert session.calls[0]["params"]["market"] == CONDITION
    assert session.calls[1]["params"]["market"] == second_condition
    assert len(result.trades) == 2


def test_fetch_respects_page_safety_limit(capsys: pytest.CaptureFixture[str]) -> None:
    config = TradesConfig(page_limit=1, max_pages_per_batch=2)
    session = FakeSession(
        [
            FakeResponse([raw_trade()]),
            FakeResponse([raw_trade(transactionHash="0x" + "e" * 64)]),
        ]
    )
    client = TradesClient(session=session, sleep=lambda _seconds: None)

    result = fetch_new_trades(
        config=config,
        client=client,
        condition_ids=[CONDITION],
        floor=NOW - timedelta(seconds=120),
        observed_at=NOW,
    )

    assert len(session.calls) == 2
    assert len(result.trades) == 2
    assert "page safety limit" in capsys.readouterr().err


def test_storage_path_matches_shared_provider_source_object_contract() -> None:
    assert build_object_path(NOW, "a" * 32) == (
        "raw/provider=polymarket/source=data-api/object=trades/schema=v1/"
        "date=2026-08-04/hour=18/"
        f"polymarket_trades_{'a' * 32}.json.gz"
    )


def test_watermark_falls_back_to_lookback_for_new_or_changed_fingerprint() -> None:
    config = TradesConfig()

    assert load_watermark({}, config, NOW) == NOW - timedelta(hours=24)
    assert load_watermark(
        {"since_id": NOW.isoformat(), "query_fingerprint": "stale"},
        config,
        NOW,
    ) == NOW - timedelta(hours=24)
    assert (
        load_watermark(
            {
                "since_id": NOW.isoformat(),
                "query_fingerprint": query_fingerprint(config),
            },
            config,
            NOW,
        )
        == NOW
    )


def test_cycle_archives_then_persists_and_advances_checkpoint() -> None:
    client = TradesClient(session=FakeSession([FakeResponse([raw_trade()])]))
    bucket = FakeBucket()
    repository = FakeRepository(condition_ids=[CONDITION])

    checkpoint = run_cycle(
        config=TradesConfig(),
        client=client,
        bucket=bucket,
        repository=repository,
        now=NOW,
    )

    assert checkpoint is not None
    assert checkpoint["since_id"] == NOW.isoformat()
    assert repository.checkpoints == [checkpoint]
    assert len(repository.envelopes) == 1
    envelope = repository.envelopes[0]
    assert envelope["object_type"] == "trades"
    assert envelope["record_count"] == 1
    assert envelope["records"][0]["condition_id"] == CONDITION
    assert bucket.blobs[0].data is not None


def test_cycle_without_new_trades_only_advances_checkpoint() -> None:
    client = TradesClient(session=FakeSession([FakeResponse([])]))
    bucket = FakeBucket()
    repository = FakeRepository(condition_ids=[CONDITION])

    checkpoint = run_cycle(
        config=TradesConfig(),
        client=client,
        bucket=bucket,
        repository=repository,
        now=NOW,
    )

    assert checkpoint is not None
    assert repository.checkpoints == [checkpoint]
    assert repository.envelopes == []
    assert bucket.blobs == []


def test_cycle_skips_without_open_markets() -> None:
    client = TradesClient(session=FakeSession([]))
    repository = FakeRepository(condition_ids=[])

    checkpoint = run_cycle(
        config=TradesConfig(),
        client=client,
        bucket=FakeBucket(),
        repository=repository,
        now=NOW,
    )

    assert checkpoint is None
    assert repository.checkpoints == []


def test_failed_raw_upload_does_not_write_database_or_checkpoint() -> None:
    client = TradesClient(session=FakeSession([FakeResponse([raw_trade()])]))
    repository = FakeRepository(condition_ids=[CONDITION])

    with pytest.raises(RuntimeError, match="upload failed"):
        run_cycle(
            config=TradesConfig(),
            client=client,
            bucket=FakeBucket(fail=True),
            repository=repository,
            now=NOW,
        )

    assert repository.envelopes == []
    assert repository.checkpoints == []


def test_dry_run_previews_trades_without_storage_dependencies(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = TradesClient(session=FakeSession([FakeResponse([raw_trade()])]))

    result = run_dry_run(
        config=TradesConfig(),
        client=client,
        condition_ids=[CONDITION],
        now=NOW,
    )

    output = capsys.readouterr().out
    assert "DRY RUN: no GCS, PostgreSQL, or checkpoint writes" in output
    assert "BUY 60 @ 0.014165" in output
    assert len(result.trades) == 1


def test_repository_sql_appends_idempotently_with_grace_window() -> None:
    record = normalize_trade(raw_trade(), NOW)
    connection = FakeConnection()
    resources = SimpleNamespace(
        engine=FakeEngine(connection),
        close=lambda: None,
    )
    repository = TradesRepository(resources)  # type: ignore[arg-type]
    envelope = {
        "ingest_run_id": "a" * 32,
        "provider": "polymarket",
        "source": "data-api",
        "object_type": "trades",
        "schema_name": "polymarket_data_api_trades",
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
            "since_id": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
            "last_successful_poll_at": NOW.isoformat(),
        }
    )
    repository.load_open_condition_ids(missing_cutoff=NOW - timedelta(days=1))

    sql = "\n".join(
        str(statement.compile(dialect=postgresql.dialect()))
        for statement in connection.statements
    )
    assert "INSERT INTO polymarket_trades" in sql
    assert "ON CONFLICT (trade_uid) DO NOTHING" in sql
    assert "polymarket_markets.missing_since IS NULL" in sql
    assert "polymarket_markets.missing_since >" in sql
    assert "polymarket_markets.enable_order_book IS true" in sql
    assert "excluded.last_successful_poll_at >= ingest_cursors.last_successful_poll_at" in sql
