"""Deterministic tweet-market linking through shared resolved entities.

Every training example needs a (tweet, market) pair. This linker joins
resolved news mentions to resolved market mentions on canonical entity IDs
and records quality features (shared-entity count, mention roles, market
topic) instead of filtering: weak links stay in the table so training-time
filtering remains a choice, not a data loss. Markets that were provably
final before the tweet published are the only exclusions, because no price
reaction can exist for them.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.engine import DatabaseResources, create_database_resources
from src.db.models import (
    entity_mentions,
    ingest_cursors,
    news_events,
    news_market_links,
    polymarket_market_classifications,
    polymarket_markets,
)

LINKER_VERSION = "entity_overlap_v1"
LINKER_CURSOR_SOURCE = "linking"
LINKER_CURSOR_STREAM = "news_market_links"
RESOLVED_STATUS = "resolved"


def utc_now() -> datetime:
    return datetime.now(UTC)


def linker_fingerprint() -> str:
    return hashlib.sha256(LINKER_VERSION.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class NewsMention:
    news_id: str
    entity_id: str
    mention_role: str
    person_role_hint: str | None
    published_at: datetime


@dataclass(frozen=True)
class MarketMention:
    market_id: str
    entity_id: str
    mention_role: str
    person_role_hint: str | None
    event_id: str
    closed_time: datetime | None
    resolution_observed_at: datetime | None
    first_observed_at: datetime | None
    market_topic: str | None
    contract_type: str | None


def market_final_before(mention: MarketMention, published_at: datetime) -> bool:
    """True when the market was provably final before the tweet published."""
    if mention.closed_time is not None and mention.closed_time < published_at:
        return True
    return (
        mention.resolution_observed_at is not None
        and mention.resolution_observed_at < published_at
    )


def _role_map(pairs: set[tuple[str, str, str | None]]) -> dict[str, dict[str, list[str]]]:
    roles: dict[str, set[str]] = defaultdict(set)
    hints: dict[str, set[str]] = defaultdict(set)
    for entity_id, mention_role, person_role_hint in pairs:
        roles[entity_id].add(mention_role)
        if person_role_hint:
            hints[entity_id].add(person_role_hint)
    return {
        entity_id: {
            "mention_roles": sorted(roles[entity_id]),
            "person_role_hints": sorted(hints.get(entity_id, set())),
        }
        for entity_id in roles
    }


def build_links(
    news_mentions: list[NewsMention],
    market_mentions: list[MarketMention],
) -> list[dict[str, Any]]:
    """Join the two mention sides on entity_id and aggregate per pair."""
    markets_by_entity: dict[str, list[MarketMention]] = defaultdict(list)
    for market_mention in market_mentions:
        markets_by_entity[market_mention.entity_id].append(market_mention)

    news_roles: dict[tuple[str, str], set[tuple[str, str, str | None]]] = defaultdict(set)
    market_roles: dict[tuple[str, str], set[tuple[str, str, str | None]]] = defaultdict(set)
    pair_market: dict[tuple[str, str], MarketMention] = {}
    pair_published: dict[tuple[str, str], datetime] = {}

    for news_mention in news_mentions:
        for market_mention in markets_by_entity.get(news_mention.entity_id, ()):
            if market_final_before(market_mention, news_mention.published_at):
                continue
            pair = (news_mention.news_id, market_mention.market_id)
            pair_market.setdefault(pair, market_mention)
            pair_published.setdefault(pair, news_mention.published_at)
            news_roles[pair].add(
                (
                    news_mention.entity_id,
                    news_mention.mention_role,
                    news_mention.person_role_hint,
                )
            )
            market_roles[pair].add(
                (
                    market_mention.entity_id,
                    market_mention.mention_role,
                    market_mention.person_role_hint,
                )
            )

    rows: list[dict[str, Any]] = []
    for pair in sorted(pair_market):
        news_id, market_id = pair
        market_mention = pair_market[pair]
        published_at = pair_published[pair]
        shared_entity_ids = sorted(
            {entity_id for entity_id, _, _ in news_roles[pair]}
        )
        market_open = (
            market_mention.first_observed_at is None
            or market_mention.first_observed_at <= published_at
        )
        rows.append(
            {
                "news_id": news_id,
                "market_id": market_id,
                "event_id": market_mention.event_id,
                "published_at": published_at,
                "shared_entity_ids": shared_entity_ids,
                "shared_entity_count": len(shared_entity_ids),
                "news_mention_roles": _role_map(news_roles[pair]),
                "market_mention_roles": _role_map(market_roles[pair]),
                "market_topic": market_mention.market_topic,
                "contract_type": market_mention.contract_type,
                "market_open_at_publish": market_open,
                "linker_version": LINKER_VERSION,
            }
        )
    return rows


class LinkerRepository:
    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> LinkerRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_news_mentions(self) -> list[NewsMention]:
        statement = (
            select(
                entity_mentions.c.news_id,
                entity_mentions.c.entity_id,
                entity_mentions.c.mention_role,
                entity_mentions.c.person_role_hint,
                news_events.c.published_at,
            )
            .join(news_events, news_events.c.news_id == entity_mentions.c.news_id)
            .where(
                entity_mentions.c.news_id.is_not(None),
                entity_mentions.c.resolution_status == RESOLVED_STATUS,
                entity_mentions.c.entity_id.is_not(None),
            )
            .order_by(entity_mentions.c.news_id)
        )
        with self.resources.engine.connect() as connection:
            return [
                NewsMention(
                    news_id=row.news_id,
                    entity_id=row.entity_id,
                    mention_role=row.mention_role,
                    person_role_hint=row.person_role_hint,
                    published_at=row.published_at,
                )
                for row in connection.execute(statement)
            ]

    def load_market_mentions(self) -> list[MarketMention]:
        statement = (
            select(
                entity_mentions.c.polymarket_market_id,
                entity_mentions.c.entity_id,
                entity_mentions.c.mention_role,
                entity_mentions.c.person_role_hint,
                polymarket_markets.c.event_id,
                polymarket_markets.c.closed_time,
                polymarket_markets.c.resolution_observed_at,
                polymarket_markets.c.first_observed_at,
                polymarket_market_classifications.c.market_topic,
                polymarket_market_classifications.c.contract_type,
            )
            .join(
                polymarket_markets,
                polymarket_markets.c.market_id == entity_mentions.c.polymarket_market_id,
            )
            .join(
                polymarket_market_classifications,
                polymarket_market_classifications.c.market_id
                == polymarket_markets.c.market_id,
                isouter=True,
            )
            .where(
                entity_mentions.c.polymarket_market_id.is_not(None),
                entity_mentions.c.resolution_status == RESOLVED_STATUS,
                entity_mentions.c.entity_id.is_not(None),
            )
            .order_by(entity_mentions.c.polymarket_market_id)
        )
        with self.resources.engine.connect() as connection:
            return [
                MarketMention(
                    market_id=row.polymarket_market_id,
                    entity_id=row.entity_id,
                    mention_role=row.mention_role,
                    person_role_hint=row.person_role_hint,
                    event_id=row.event_id,
                    closed_time=row.closed_time,
                    resolution_observed_at=row.resolution_observed_at,
                    first_observed_at=row.first_observed_at,
                    market_topic=row.market_topic,
                    contract_type=row.contract_type,
                )
                for row in connection.execute(statement)
            ]

    def persist_links(
        self,
        rows: list[dict[str, Any]],
        *,
        run_started_at: datetime,
        prune: bool = True,
    ) -> int:
        """Upsert the full recomputed link set; prune rows not regenerated.

        The linker recomputes every link each run, so any row that did not
        receive the current run's updated_at is stale (its mention resolution
        changed or the market became final) and is removed on full runs.
        """
        with self.resources.engine.begin() as connection:
            for offset in range(0, len(rows), 500):
                batch = [
                    {**row, "updated_at": run_started_at}
                    for row in rows[offset : offset + 500]
                ]
                statement = insert(news_market_links).values(batch)
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            news_market_links.c.news_id,
                            news_market_links.c.market_id,
                        ],
                        set_={
                            column: getattr(statement.excluded, column)
                            for column in (
                                "event_id",
                                "published_at",
                                "shared_entity_ids",
                                "shared_entity_count",
                                "news_mention_roles",
                                "market_mention_roles",
                                "market_topic",
                                "contract_type",
                                "market_open_at_publish",
                                "linker_version",
                                "updated_at",
                            )
                        },
                    )
                )
            pruned = 0
            if prune:
                result = connection.execute(
                    news_market_links.delete().where(
                        news_market_links.c.updated_at < run_started_at
                    )
                )
                pruned = result.rowcount or 0
        return pruned

    def finalize_run(self, *, run_started_at: datetime) -> None:
        cursor_insert = insert(ingest_cursors).values(
            source=LINKER_CURSOR_SOURCE,
            stream=LINKER_CURSOR_STREAM,
            query_fingerprint=linker_fingerprint(),
            last_structural_sha256=None,
            since_id=None,
            updated_at=run_started_at,
            last_successful_poll_at=run_started_at,
        )
        with self.resources.engine.begin() as connection:
            connection.execute(
                cursor_insert.on_conflict_do_update(
                    index_elements=[
                        ingest_cursors.c.source,
                        ingest_cursors.c.stream,
                    ],
                    set_={
                        "query_fingerprint": cursor_insert.excluded.query_fingerprint,
                        "updated_at": cursor_insert.excluded.updated_at,
                        "last_successful_poll_at": (
                            cursor_insert.excluded.last_successful_poll_at
                        ),
                    },
                    where=(
                        cursor_insert.excluded.last_successful_poll_at
                        >= ingest_cursors.c.last_successful_poll_at
                    ),
                )
            )


def run_linker(repository: LinkerRepository, *, dry_run: bool = False) -> list[dict[str, Any]]:
    run_started_at = utc_now()
    news_mentions = repository.load_news_mentions()
    market_mentions = repository.load_market_mentions()
    print(
        f"Loaded {len(news_mentions)} resolved news mentions and "
        f"{len(market_mentions)} resolved market mentions"
    )
    rows = build_links(news_mentions, market_mentions)
    linked_news = len({row["news_id"] for row in rows})
    linked_markets = len({row["market_id"] for row in rows})
    print(
        f"Computed {len(rows)} tweet-market links covering "
        f"{linked_news} tweets and {linked_markets} markets"
    )
    if dry_run:
        print("DRY RUN: skipped PostgreSQL writes")
        return rows
    pruned = repository.persist_links(rows, run_started_at=run_started_at)
    repository.finalize_run(run_started_at=run_started_at)
    print(f"Committed {len(rows)} links (pruned {pruned} stale) at {LINKER_VERSION}")
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and report links without writing to PostgreSQL",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    src_dir = Path(__file__).resolve().parents[1]
    repository: LinkerRepository | None = None
    try:
        repository = LinkerRepository.from_environment(src_dir)
        run_linker(repository, dry_run=args.dry_run)
        return 0
    except Exception as exc:
        print(f"ERROR: tweet-market linking failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if repository:
            repository.close()
