import gzip
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from sqlalchemy.dialects import postgresql

from src.db.odds_repository import OrderBookRepository
from src.ingest_odds.clob_order_book_pipeline import (
    ClobOrderBookClient,
    ClobOrderBookConfig,
    archive_envelope,
    build_object_path,
    build_parser,
    normalize_book,
    run_cycle,
    run_dry_run,
)

NOW = datetime(2026, 7, 31, 18, 0, tzinfo=UTC)


def raw_book(token_id: str = "token-a") -> dict[str, Any]:
    return {
        "market": "condition-a",
        "asset_id": token_id,
        "timestamp": "1785520800000",
        "hash": f"hash-{token_id}",
        "bids": [
            {"price": "0.40", "size": "200"},
            {"price": "0.50", "size": "100"},
            {"price": "0.30", "size": "500"},
        ],
        "asks": [
            {"price": "0.80", "size": "500"},
            {"price": "0.60", "size": "100"},
            {"price": "0.70", "size": "100"},
        ],
        "tick_size": "0.01",
        "min_order_size": "5",
        "last_trade_price": "0.55",
    }


class FakeResponse:
    status_code = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: list[dict[str, Any]]) -> None:
        self.payload = payload

    def json(self) -> list[dict[str, Any]]:
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

    def persist_records(self, envelope: dict[str, Any]) -> None:
        self.envelopes.append(envelope)

    def finalize_cycle(self, checkpoint: dict[str, Any]) -> None:
        self.checkpoints.append(checkpoint)


class FakeSqlConnection:
    def __init__(self) -> None:
        self.statements: list[Any] = []

    def execute(self, statement: Any) -> None:
        self.statements.append(statement)


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


def test_normalize_book_retains_levels_through_notional_boundary() -> None:
    book = normalize_book(raw_book(), depth_usdc=Decimal("100"), observed_at=NOW)

    assert book["bids"] == [
        {"price": "0.50", "size": "100"},
        {"price": "0.40", "size": "200"},
    ]
    assert book["asks"] == [
        {"price": "0.60", "size": "100"},
        {"price": "0.70", "size": "100"},
    ]
    assert book["bid_captured_notional"] == "130.00"
    assert book["ask_captured_notional"] == "130.00"
    assert book["bid_total_notional"] == "280.00"
    assert book["ask_total_notional"] == "530.00"
    assert book["bid_truncated"] is True
    assert book["ask_truncated"] is True
    assert book["best_bid"] == "0.50"
    assert book["best_ask"] == "0.60"
    assert book["spread"] == "0.10"


def test_fetch_books_batches_token_requests_and_preserves_order() -> None:
    session = FakeSession([FakeResponse([raw_book("token-b"), raw_book("token-a")])])
    client = ClobOrderBookClient(session=session)

    result = client.fetch_books(
        token_ids=["token-a", "token-b"],
        depth_usdc=Decimal("10000"),
        batch_size=500,
        observed_at=NOW,
        report_progress=False,
    )

    assert session.calls[0]["json"] == [
        {"token_id": "token-a"},
        {"token_id": "token-b"},
    ]
    assert [book["token_id"] for book in result.books] == ["token-a", "token-b"]


def test_fetch_books_records_and_skips_tokens_omitted_by_clob(capsys: Any) -> None:
    client = ClobOrderBookClient(session=FakeSession([FakeResponse([raw_book("token-a")])]))

    result = client.fetch_books(
        token_ids=["token-a", "stale-token"],
        depth_usdc=Decimal("10000"),
        batch_size=500,
        observed_at=NOW,
        report_progress=False,
    )

    assert [book["token_id"] for book in result.books] == ["token-a"]
    assert result.request_batches[0]["omitted_token_ids"] == ["stale-token"]
    assert "omitted 1 requested token" in capsys.readouterr().err


def test_order_book_cycle_uploads_bounded_cloud_envelope_and_persists() -> None:
    client = ClobOrderBookClient(
        session=FakeSession([FakeResponse([raw_book("token-a"), raw_book("token-b")])])
    )
    bucket = FakeBucket()
    repository = FakeRepository()
    config = ClobOrderBookConfig(depth_usdc=Decimal("100"), batch_size=500)

    checkpoint = run_cycle(
        config=config,
        client=client,
        bucket=bucket,
        repository=repository,
        now=NOW,
    )

    assert checkpoint is not None
    assert len(repository.envelopes) == 1
    assert len(repository.checkpoints) == 1
    archived = json.loads(gzip.decompress(bucket.blobs[0].data or b""))
    assert archived == archive_envelope(repository.envelopes[0])
    assert archived["records"][0]["depth_usdc"] == "100"
    assert len(archived["records"][0]["bids"]) == 2
    assert bucket.blobs[0].metadata["depth_usdc"] == "100"


def test_dry_run_fetches_once_and_emits_each_depth(capsys: Any) -> None:
    session = FakeSession([FakeResponse([raw_book("token-a")])])
    client = ClobOrderBookClient(session=session)

    envelopes = run_dry_run(
        base_config=ClobOrderBookConfig(),
        client=client,
        token_ids=["token-a"],
        depths=[Decimal("100"), Decimal("500")],
        now=NOW,
    )

    captured = capsys.readouterr()
    cloud_lines = [json.loads(line) for line in captured.out.splitlines()]
    assert len(session.calls) == 1
    assert len(envelopes) == 2
    assert [line["request"]["depth_usdc"] for line in cloud_lines] == ["100", "500"]
    assert "json_bytes=" in captured.err
    assert "gzip_bytes=" in captured.err
    assert "no GCS or PostgreSQL writes" in captured.err


def test_order_book_path_uses_shared_storage_contract() -> None:
    assert build_object_path(NOW, "a" * 32) == (
        "raw/provider=polymarket/source=clob/object=order-books/schema=v1/"
        "date=2026-07-31/hour=18/"
        f"polymarket_order_books_{'a' * 32}.json.gz"
    )


def test_dry_run_defaults_to_ten_database_tokens() -> None:
    args = build_parser().parse_args(["--dry-run", "--depth-usdc", "10000"])

    assert args.token_id == []
    assert args.limit == 10


def test_repository_keeps_depth_current_and_snapshots_only_summaries() -> None:
    record = normalize_book(raw_book(), depth_usdc=Decimal("100"), observed_at=NOW)
    connection = FakeSqlConnection()
    repository = OrderBookRepository(  # type: ignore[arg-type]
        SimpleNamespace(engine=FakeEngine(connection), close=lambda: None)
    )

    repository.persist_records(
        {
            "ingest_run_id": "a" * 32,
            "provider": "polymarket",
            "source": "clob",
            "object_type": "order-books",
            "schema_name": "polymarket_clob_order_books",
            "schema_version": 1,
            "storage_uri": "gs://bucket/books.json.gz",
            "content_sha256": "b" * 64,
            "record_count": 1,
            "ingested_at": NOW.isoformat(),
            "request": {"query_fingerprint": "c" * 64},
            "records": [record],
        }
    )

    sql = [
        str(statement.compile(dialect=postgresql.dialect())) for statement in connection.statements
    ]
    snapshot_insert = next(
        statement
        for statement in sql
        if statement.startswith("INSERT INTO polymarket_order_book_snapshots")
    )
    current_insert = next(
        statement
        for statement in sql
        if statement.startswith("INSERT INTO polymarket_current_order_books")
    )
    assert "bids" not in snapshot_insert
    assert "asks" not in snapshot_insert
    assert "bids" in current_insert
    assert "asks" in current_insert
    assert "WHERE excluded.observed_at >= polymarket_current_order_books.observed_at" in (
        current_insert
    )
