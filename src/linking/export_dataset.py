"""Export the training dataset joining tweets, links, reactions, and outcomes.

One output row per (tweet, market, outcome token) reaction: the tweet text
and enrichment give the model input, the link features describe why the pair
exists, the reaction deltas are the market-response target, and the final
resolution rides along for outcome-based evaluation. Nested values (claims,
role maps, entity lists) are serialized as JSON strings so both parquet and
JSONL stay flat and portable.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select

from src.db.engine import DatabaseResources, create_database_resources
from src.db.models import (
    news_enrichments,
    news_events,
    news_market_links,
    news_market_reactions,
    polymarket_markets,
    polymarket_tokens,
)

COMPLETED_STATUSES = ("completed", "completed_with_warnings")


def utc_now() -> datetime:
    return datetime.now(UTC)


def _float(value: Any) -> float | None:
    return float(value) if value is not None else None


def _json_text(value: Any) -> str | None:
    return json.dumps(value, ensure_ascii=False, sort_keys=True) if value is not None else None


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


class ExportRepository:
    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    @classmethod
    def from_environment(cls, src_dir: Path) -> ExportRepository:
        return cls(create_database_resources(src_dir))

    def close(self) -> None:
        self.resources.close()

    def load_rows(
        self,
        *,
        since: datetime | None,
        until: datetime | None,
    ) -> list[dict[str, Any]]:
        statement = (
            select(
                news_market_reactions,
                news_market_links.c.event_id,
                news_market_links.c.shared_entity_ids,
                news_market_links.c.shared_entity_count,
                news_market_links.c.news_mention_roles,
                news_market_links.c.market_mention_roles,
                news_market_links.c.market_topic,
                news_market_links.c.contract_type,
                news_market_links.c.market_open_at_publish,
                news_market_links.c.linker_version,
                news_events.c.author_username,
                news_events.c.text,
                polymarket_markets.c.question,
                polymarket_markets.c.sports_market_type,
                polymarket_markets.c.group_item_title,
                polymarket_markets.c.line,
                polymarket_markets.c.uma_resolution_status,
                polymarket_markets.c.winning_outcome_index,
                polymarket_tokens.c.outcome,
            )
            .join(
                news_market_links,
                (news_market_links.c.news_id == news_market_reactions.c.news_id)
                & (news_market_links.c.market_id == news_market_reactions.c.market_id),
            )
            .join(news_events, news_events.c.news_id == news_market_reactions.c.news_id)
            .join(
                polymarket_markets,
                polymarket_markets.c.market_id == news_market_reactions.c.market_id,
            )
            .join(
                polymarket_tokens,
                polymarket_tokens.c.token_id == news_market_reactions.c.token_id,
            )
            .order_by(
                news_market_reactions.c.published_at,
                news_market_reactions.c.news_id,
                news_market_reactions.c.market_id,
                news_market_reactions.c.token_id,
            )
        )
        if since is not None:
            statement = statement.where(news_market_reactions.c.published_at >= since)
        if until is not None:
            statement = statement.where(news_market_reactions.c.published_at < until)
        with self.resources.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings()]

    def load_enrichments(self, news_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Latest completed enrichment per tweet."""
        if not news_ids:
            return {}
        statement = (
            select(
                news_enrichments.c.news_id,
                news_enrichments.c.summary,
                news_enrichments.c.information_status,
                news_enrichments.c.usefulness,
                news_enrichments.c.claims,
                news_enrichments.c.updated_at,
            )
            .where(
                news_enrichments.c.news_id.in_(sorted(set(news_ids))),
                news_enrichments.c.status.in_(COMPLETED_STATUSES),
            )
            .order_by(news_enrichments.c.news_id, news_enrichments.c.updated_at)
        )
        latest: dict[str, dict[str, Any]] = {}
        with self.resources.engine.connect() as connection:
            for row in connection.execute(statement).mappings():
                latest[row["news_id"]] = dict(row)
        return latest


def build_dataset_rows(
    rows: list[dict[str, Any]],
    enrichments: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    dataset: list[dict[str, Any]] = []
    for row in rows:
        enrichment = enrichments.get(row["news_id"], {})
        winning_index = row["winning_outcome_index"]
        dataset.append(
            {
                "news_id": row["news_id"],
                "published_at": _iso(row["published_at"]),
                "author_username": row["author_username"],
                "text": row["text"],
                "summary": enrichment.get("summary"),
                "information_status": enrichment.get("information_status"),
                "usefulness": enrichment.get("usefulness"),
                "claims": _json_text(enrichment.get("claims")),
                "market_id": row["market_id"],
                "event_id": row["event_id"],
                "question": row["question"],
                "sports_market_type": row["sports_market_type"],
                "group_item_title": row["group_item_title"],
                "line": _float(row["line"]),
                "market_topic": row["market_topic"],
                "contract_type": row["contract_type"],
                "shared_entity_ids": _json_text(row["shared_entity_ids"]),
                "shared_entity_count": row["shared_entity_count"],
                "news_mention_roles": _json_text(row["news_mention_roles"]),
                "market_mention_roles": _json_text(row["market_mention_roles"]),
                "market_open_at_publish": row["market_open_at_publish"],
                "linker_version": row["linker_version"],
                "token_id": row["token_id"],
                "outcome": row["outcome"],
                "outcome_index": row["outcome_index"],
                "label_version": row["label_version"],
                "baseline_midpoint": _float(row["baseline_midpoint"]),
                "baseline_observed_at": _iso(row["baseline_observed_at"]),
                "midpoint_plus_1m": _float(row["midpoint_plus_1m"]),
                "midpoint_plus_5m": _float(row["midpoint_plus_5m"]),
                "midpoint_plus_30m": _float(row["midpoint_plus_30m"]),
                "midpoint_plus_2h": _float(row["midpoint_plus_2h"]),
                "delta_plus_1m": _float(row["delta_plus_1m"]),
                "delta_plus_5m": _float(row["delta_plus_5m"]),
                "delta_plus_30m": _float(row["delta_plus_30m"]),
                "delta_plus_2h": _float(row["delta_plus_2h"]),
                "trade_count": row["trade_count"],
                "trade_notional": _float(row["trade_notional"]),
                "snapshot_count": row["snapshot_count"],
                "max_gap_seconds": _float(row["max_gap_seconds"]),
                "uma_resolution_status": row["uma_resolution_status"],
                "winning_outcome_index": winning_index,
                "outcome_won": (
                    winning_index == row["outcome_index"]
                    if winning_index is not None and row["outcome_index"] is not None
                    else None
                ),
            }
        )
    return dataset


def write_jsonl(dataset: list[dict[str, Any]], path: Path) -> None:
    with path.open("w", encoding="utf-8") as file:
        for row in dataset:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            file.write("\n")


def write_parquet(dataset: list[dict[str, Any]], path: Path) -> None:
    import pandas as pd

    pd.DataFrame(dataset).to_parquet(path, index=False)


def _parse_timestamp(value: str | None, flag: str) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{flag} must be an ISO timestamp or date") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", help="Only include tweets published at or after this time")
    parser.add_argument("--until", help="Only include tweets published before this time")
    parser.add_argument(
        "--output-dir",
        default="data/local/datasets",
        help="Directory for the exported files (default: data/local/datasets)",
    )
    parser.add_argument(
        "--format",
        choices=("jsonl", "parquet", "both"),
        default="both",
        help="Output format (default: both)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    src_dir = Path(__file__).resolve().parents[1]
    repository: ExportRepository | None = None
    try:
        since = _parse_timestamp(args.since, "--since")
        until = _parse_timestamp(args.until, "--until")
        repository = ExportRepository.from_environment(src_dir)
        rows = repository.load_rows(since=since, until=until)
        if not rows:
            print("No reaction rows match the requested range; nothing to export")
            return 0
        enrichments = repository.load_enrichments([row["news_id"] for row in rows])
        dataset = build_dataset_rows(rows, enrichments)
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
        written: list[Path] = []
        if args.format in ("jsonl", "both"):
            jsonl_path = output_dir / f"tweet_market_dataset_{stamp}.jsonl"
            write_jsonl(dataset, jsonl_path)
            written.append(jsonl_path)
        if args.format in ("parquet", "both"):
            parquet_path = output_dir / f"tweet_market_dataset_{stamp}.parquet"
            write_parquet(dataset, parquet_path)
            written.append(parquet_path)
        tweet_count = len({row["news_id"] for row in dataset})
        print(
            f"Exported {len(dataset)} rows covering {tweet_count} tweets to "
            + ", ".join(str(path) for path in written)
        )
        return 0
    except Exception as exc:
        print(f"ERROR: dataset export failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if repository:
            repository.close()


if __name__ == "__main__":
    raise SystemExit(main())
