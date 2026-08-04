"""Price-reaction labels for tweet-market links.

For each link, the builder reconstructs the market's price around the tweet
from the archived CLOB order-book envelopes in GCS: the last snapshot before
publish is the baseline, and the first snapshot at each horizon gives the
"after" price. Only the handful of envelopes actually needed are downloaded
(baseline plus one per horizon), never the full window, and each envelope
serves every link that shares its timestamp through an LRU cache. Gaps in
the snapshot series never fail a label; they are recorded as coverage stats
so training can decide how much to trust each row.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import OrderedDict, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.common.gcs import create_gcs_client
from src.db.engine import DatabaseResources, create_database_resources
from src.db.models import (
    news_market_links,
    news_market_reactions,
    polymarket_tokens,
    polymarket_trades,
    raw_ingest_objects,
)

LABEL_VERSION = "midpoint_reaction_v1"
HORIZONS: tuple[tuple[str, int], ...] = (
    ("plus_1m", 60),
    ("plus_5m", 300),
    ("plus_30m", 1800),
    ("plus_2h", 7200),
)
POST_WINDOW_SECONDS = 7200
BASELINE_LOOKBACK_SECONDS = 600
HORIZON_TOLERANCE_SECONDS = 120
ENVELOPE_CACHE_SIZE = 64
ORDER_BOOK_PROVIDER = "polymarket"
ORDER_BOOK_SOURCE = "clob"
ORDER_BOOK_OBJECT = "order-books"


def utc_now() -> datetime:
    return datetime.now(UTC)


class EnvelopeReaderProtocol(Protocol):
    def fetch_midpoints(self, ingest_run_id: str, storage_uri: str) -> dict[str, str | None]: ...


class GcsEnvelopeReader:
    """Download archived order-book envelopes and keep recent extractions."""

    def __init__(self, *, src_dir: Path, cache_size: int = ENVELOPE_CACHE_SIZE) -> None:
        self.client = create_gcs_client(src_dir)
        self.cache_size = cache_size
        self._cache: OrderedDict[str, dict[str, str | None]] = OrderedDict()

    def fetch_midpoints(self, ingest_run_id: str, storage_uri: str) -> dict[str, str | None]:
        cached = self._cache.get(ingest_run_id)
        if cached is not None:
            self._cache.move_to_end(ingest_run_id)
            return cached
        bucket_name, _, object_path = storage_uri.removeprefix("gs://").partition("/")
        raw = self.client.bucket(bucket_name).blob(object_path).download_as_bytes()
        # GCS may decompressively transcode envelopes uploaded with
        # Content-Encoding: gzip, so both raw and decoded bytes are valid.
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        envelope = json.loads(raw)
        midpoints = {
            record["token_id"]: record.get("midpoint")
            for record in envelope.get("records", [])
        }
        self._cache[ingest_run_id] = midpoints
        while len(self._cache) > self.cache_size:
            self._cache.popitem(last=False)
        return midpoints


def select_envelopes(
    index: list[dict[str, Any]],
    published_at: datetime,
) -> dict[str, dict[str, Any] | None]:
    """Choose the baseline envelope and one envelope per horizon.

    Baseline is the newest snapshot at or before publish (within the
    lookback); a horizon envelope is the oldest snapshot at or after the
    horizon, but only if it lands within the tolerance so a long collector
    gap cannot masquerade as a later price.
    """
    chosen: dict[str, dict[str, Any] | None] = {}
    baseline_floor = published_at - timedelta(seconds=BASELINE_LOOKBACK_SECONDS)
    baseline = None
    for item in index:
        if baseline_floor <= item["ingested_at"] <= published_at:
            if baseline is None or item["ingested_at"] > baseline["ingested_at"]:
                baseline = item
    chosen["baseline"] = baseline
    for name, offset in HORIZONS:
        target = published_at + timedelta(seconds=offset)
        limit = target + timedelta(seconds=HORIZON_TOLERANCE_SECONDS)
        best = None
        for item in index:
            if target <= item["ingested_at"] <= limit:
                if best is None or item["ingested_at"] < best["ingested_at"]:
                    best = item
        chosen[name] = best
    return chosen


def coverage_stats(
    index: list[dict[str, Any]],
    published_at: datetime,
) -> tuple[int, Decimal | None]:
    """Snapshot count and worst spacing across the post-publish window."""
    window_end = published_at + timedelta(seconds=POST_WINDOW_SECONDS)
    timestamps = sorted(
        item["ingested_at"] for item in index if published_at <= item["ingested_at"] <= window_end
    )
    if not timestamps:
        return 0, None
    edges = [published_at, *timestamps, window_end]
    max_gap = max(
        (later - earlier).total_seconds()
        for earlier, later in zip(edges, edges[1:], strict=False)
    )
    return len(timestamps), Decimal(str(max_gap))


def _midpoint(
    envelope_item: dict[str, Any] | None,
    token_id: str,
    reader: EnvelopeReaderProtocol,
) -> tuple[Decimal | None, datetime | None]:
    if envelope_item is None:
        return None, None
    midpoints = reader.fetch_midpoints(
        envelope_item["ingest_run_id"], envelope_item["storage_uri"]
    )
    value = midpoints.get(token_id)
    if value is None:
        return None, envelope_item["ingested_at"]
    return Decimal(str(value)), envelope_item["ingested_at"]


def build_reaction_rows(
    *,
    link: dict[str, Any],
    tokens: list[dict[str, Any]],
    envelope_index: list[dict[str, Any]],
    reader: EnvelopeReaderProtocol,
    trade_stats: dict[str, tuple[int, Decimal]] | None,
    computed_at: datetime,
) -> list[dict[str, Any]]:
    """One reaction row per outcome token of the linked market.

    trade_stats of None means trades were not being collected for this
    window; the trade columns stay NULL instead of claiming zero volume.
    """
    published_at = link["published_at"]
    chosen = select_envelopes(envelope_index, published_at)
    snapshot_count, max_gap = coverage_stats(envelope_index, published_at)
    rows: list[dict[str, Any]] = []
    for token in tokens:
        token_id = token["token_id"]
        baseline_midpoint, baseline_observed_at = _midpoint(
            chosen["baseline"], token_id, reader
        )
        row: dict[str, Any] = {
            "news_id": link["news_id"],
            "market_id": link["market_id"],
            "token_id": token_id,
            "label_version": LABEL_VERSION,
            "outcome_index": token["outcome_index"],
            "published_at": published_at,
            "baseline_midpoint": baseline_midpoint,
            "baseline_observed_at": baseline_observed_at,
            "snapshot_count": snapshot_count,
            "max_gap_seconds": max_gap,
            "computed_at": computed_at,
        }
        for name, _offset in HORIZONS:
            midpoint, _observed = _midpoint(chosen[name], token_id, reader)
            row[f"midpoint_{name}"] = midpoint
            row[f"delta_{name}"] = (
                midpoint - baseline_midpoint
                if midpoint is not None and baseline_midpoint is not None
                else None
            )
        if trade_stats is None:
            row["trade_count"] = None
            row["trade_notional"] = None
        else:
            count, notional = trade_stats.get(token_id, (0, Decimal("0")))
            row["trade_count"] = count
            row["trade_notional"] = notional
        rows.append(row)
    return rows


class ReactionsRepository:
    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> ReactionsRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_pending_links(self, *, cutoff: datetime, limit: int | None) -> list[dict[str, Any]]:
        """Links whose post-publish window has fully elapsed and which have no
        reaction rows at the current label version yet."""
        reaction_exists = (
            select(news_market_reactions.c.news_id)
            .where(
                news_market_reactions.c.news_id == news_market_links.c.news_id,
                news_market_reactions.c.market_id == news_market_links.c.market_id,
                news_market_reactions.c.label_version == LABEL_VERSION,
            )
            .exists()
        )
        statement = (
            select(
                news_market_links.c.news_id,
                news_market_links.c.market_id,
                news_market_links.c.published_at,
            )
            .where(
                news_market_links.c.published_at <= cutoff,
                ~reaction_exists,
            )
            .order_by(news_market_links.c.published_at, news_market_links.c.news_id)
        )
        if limit is not None:
            statement = statement.limit(limit)
        with self.resources.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings()]

    def load_market_tokens(self, market_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        if not market_ids:
            return {}
        statement = (
            select(
                polymarket_tokens.c.market_id,
                polymarket_tokens.c.token_id,
                polymarket_tokens.c.outcome_index,
            )
            .where(polymarket_tokens.c.market_id.in_(sorted(set(market_ids))))
            .order_by(polymarket_tokens.c.market_id, polymarket_tokens.c.outcome_index)
        )
        tokens: dict[str, list[dict[str, Any]]] = defaultdict(list)
        with self.resources.engine.connect() as connection:
            for row in connection.execute(statement):
                tokens[row.market_id].append(
                    {"token_id": row.token_id, "outcome_index": row.outcome_index}
                )
        return dict(tokens)

    def load_envelope_index(
        self,
        *,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, Any]]:
        statement = (
            select(
                raw_ingest_objects.c.ingest_run_id,
                raw_ingest_objects.c.storage_uri,
                raw_ingest_objects.c.ingested_at,
            )
            .where(
                raw_ingest_objects.c.provider == ORDER_BOOK_PROVIDER,
                raw_ingest_objects.c.source == ORDER_BOOK_SOURCE,
                raw_ingest_objects.c.object_type == ORDER_BOOK_OBJECT,
                raw_ingest_objects.c.ingested_at >= window_start,
                raw_ingest_objects.c.ingested_at <= window_end,
            )
            .order_by(raw_ingest_objects.c.ingested_at)
        )
        with self.resources.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings()]

    def trades_floor(self) -> datetime | None:
        with self.resources.engine.connect() as connection:
            return connection.scalar(select(func.min(polymarket_trades.c.traded_at)))

    def load_trade_stats(
        self,
        *,
        token_ids: list[str],
        window_start: datetime,
        window_end: datetime,
    ) -> dict[str, tuple[int, Decimal]]:
        if not token_ids:
            return {}
        statement = (
            select(
                polymarket_trades.c.token_id,
                func.count().label("trade_count"),
                func.sum(polymarket_trades.c.price * polymarket_trades.c.size).label(
                    "trade_notional"
                ),
            )
            .where(
                polymarket_trades.c.token_id.in_(sorted(set(token_ids))),
                polymarket_trades.c.traded_at >= window_start,
                polymarket_trades.c.traded_at <= window_end,
            )
            .group_by(polymarket_trades.c.token_id)
        )
        with self.resources.engine.connect() as connection:
            return {
                row.token_id: (int(row.trade_count), Decimal(row.trade_notional))
                for row in connection.execute(statement)
            }

    def persist_reactions(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self.resources.engine.begin() as connection:
            for offset in range(0, len(rows), 500):
                statement = insert(news_market_reactions).values(rows[offset : offset + 500])
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            news_market_reactions.c.news_id,
                            news_market_reactions.c.market_id,
                            news_market_reactions.c.token_id,
                            news_market_reactions.c.label_version,
                        ],
                        set_={
                            column.name: getattr(statement.excluded, column.name)
                            for column in news_market_reactions.c
                            if column.name
                            not in ("news_id", "market_id", "token_id", "label_version")
                        },
                    )
                )


def run_builder(
    *,
    repository: ReactionsRepository,
    reader: EnvelopeReaderProtocol,
    limit: int | None = None,
    now: datetime | None = None,
) -> int:
    started_at = (now or utc_now()).astimezone(UTC)
    cutoff = started_at - timedelta(
        seconds=POST_WINDOW_SECONDS + HORIZON_TOLERANCE_SECONDS
    )
    links = repository.load_pending_links(cutoff=cutoff, limit=limit)
    if not links:
        print("No links are awaiting reaction labels")
        return 0
    print(f"Building reaction labels for {len(links)} links at {LABEL_VERSION}")
    tokens_by_market = repository.load_market_tokens(
        [link["market_id"] for link in links]
    )
    trades_floor = repository.trades_floor()

    links_by_news: dict[tuple[str, datetime], list[dict[str, Any]]] = defaultdict(list)
    for link in links:
        links_by_news[(link["news_id"], link["published_at"])].append(link)

    written = 0
    for (news_id, published_at), news_links in sorted(links_by_news.items(), key=lambda i: i[0][1]):
        window_start = published_at - timedelta(seconds=BASELINE_LOOKBACK_SECONDS)
        window_end = published_at + timedelta(
            seconds=POST_WINDOW_SECONDS + HORIZON_TOLERANCE_SECONDS
        )
        envelope_index = repository.load_envelope_index(
            window_start=window_start,
            window_end=window_end,
        )
        token_ids = [
            token["token_id"]
            for link in news_links
            for token in tokens_by_market.get(link["market_id"], [])
        ]
        trades_collected = trades_floor is not None and (
            published_at + timedelta(seconds=POST_WINDOW_SECONDS) >= trades_floor
        )
        trade_stats = (
            repository.load_trade_stats(
                token_ids=token_ids,
                window_start=published_at,
                window_end=published_at + timedelta(seconds=POST_WINDOW_SECONDS),
            )
            if trades_collected
            else None
        )
        rows: list[dict[str, Any]] = []
        for link in news_links:
            rows.extend(
                build_reaction_rows(
                    link=link,
                    tokens=tokens_by_market.get(link["market_id"], []),
                    envelope_index=envelope_index,
                    reader=reader,
                    trade_stats=trade_stats,
                    computed_at=started_at,
                )
            )
        repository.persist_reactions(rows)
        written += len(rows)
    print(f"Committed {written} reaction rows across {len(links)} links")
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of links to label this run (default: all pending)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.limit is not None and args.limit < 1:
        print("ERROR: --limit must be positive", file=sys.stderr)
        return 1
    src_dir = Path(__file__).resolve().parents[1]
    repository: ReactionsRepository | None = None
    try:
        repository = ReactionsRepository.from_environment(src_dir)
        reader = GcsEnvelopeReader(src_dir=src_dir)
        run_builder(repository=repository, reader=reader, limit=args.limit)
        return 0
    except Exception as exc:
        print(f"ERROR: reaction label build failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if repository:
            repository.close()
