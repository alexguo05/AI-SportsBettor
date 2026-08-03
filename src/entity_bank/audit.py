"""End-to-end live NFL entity audit with no GCS or database writes."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.enrich_news.config import load_enrichment_settings
from src.entity_bank.nflverse_pipeline import NflverseClient, fetch_snapshot, summary
from src.entity_bank.provider import ClaudeEntityProvider, DeterministicEntityProvider
from src.entity_bank.resolver import CandidateIndex
from src.entity_bank.worker import Batch, _write_records, process_market_events
from src.ingest_odds.polymarket_pipeline import GammaClient, load_config, normalize_event


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=datetime.now(UTC).year)
    parser.add_argument("--event-limit", type=int, default=10)
    parser.add_argument("--provider", choices=("mock", "claude"), default="claude")
    parser.add_argument("--model")
    parser.add_argument("--output-dir", type=Path)
    return parser


def _candidate_rows(snapshot: Any) -> list[dict[str, Any]]:
    team_names = {
        entity["entity_id"]: entity["canonical_name"]
        for entity in snapshot.entities
        if entity["entity_type"] == "team"
    }
    memberships: dict[str, set[str]] = {}
    for relationship in snapshot.relationships:
        team_name = team_names.get(relationship["object_entity_id"])
        if team_name:
            memberships.setdefault(relationship["subject_entity_id"], set()).add(
                team_name
            )
    return [
        {
            "entity_id": entity["entity_id"],
            "canonical_name": entity["canonical_name"],
            "entity_type": entity["entity_type"],
            "identity_status": "canonical",
            "aliases": [alias["alias"] for alias in entity["aliases"]],
            "roles": [role["role"] for role in entity["roles"]],
            "teams": sorted(memberships.get(entity["entity_id"], set())),
        }
        for entity in snapshot.entities
    ]


def _normalized_events(raw_events: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
    events = []
    for payload in raw_events:
        event = normalize_event(payload, now)
        if event is None:
            continue
        events.append(
            {
                "event_id": event["event_id"],
                "title": event["title"],
                "slug": event["slug"],
                "game_id": event.get("game_id"),
                "markets": [
                    {
                        "market_id": market["market_id"],
                        "question": market["question"],
                        "slug": market["slug"],
                        "group_item_title": market.get("group_item_title"),
                        "group_item_threshold": market.get("group_item_threshold"),
                        "sports_market_type": market.get("sports_market_type"),
                        "source_content_sha256": market["content_sha256"],
                        "prior_entity_input_sha256": None,
                        "prior_extractor_version": None,
                        "outcomes": [
                            token["outcome"] for token in market.get("tokens", [])
                        ],
                    }
                    for market in event["markets"]
                ],
            }
        )
    return events


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 1 <= args.event_limit <= 100:
        print("ERROR: --event-limit must be between 1 and 100", file=sys.stderr)
        return 2
    project_root = Path(__file__).resolve().parents[2]
    src_dir = project_root / "src"
    settings = load_enrichment_settings(src_dir)
    if args.provider == "claude":
        if not settings.api_key:
            print("ERROR: ANTHROPIC_API_KEY is not configured", file=sys.stderr)
            return 2
        provider = ClaudeEntityProvider(
            settings.api_key,
            model_name=args.model or settings.model_name,
            max_tokens=settings.max_output_tokens,
        )
    else:
        provider = DeterministicEntityProvider()

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else project_root / "data" / "local" / "entity_bank" / f"audit_{timestamp}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)
    now = datetime.now(UTC)
    snapshot = fetch_snapshot(NflverseClient(), season=args.season, now=now)
    config = load_config(src_dir / "config" / "polymarket_config.json")
    gamma_result = GammaClient(
        timeout_seconds=config.timeout_seconds,
        max_attempts=config.max_attempts,
    ).fetch_events(
        tag_slug=config.tag_slug,
        closed=config.closed,
        page_size=config.page_size,
        max_pages=config.max_pages,
    )
    events = _normalized_events(gamma_result.events, now)[: args.event_limit]
    batch = Batch()
    process_market_events(
        events=events,
        provider=provider,
        index=CandidateIndex(_candidate_rows(snapshot)),
        bank_version_id=None,
        batch=batch,
        observed_at=now,
    )

    _write_records(output_dir / "classifications.jsonl", batch.classifications.values())
    _write_records(output_dir / "mentions.jsonl", batch.mentions.values())
    _write_records(
        output_dir / "provisional_entities.jsonl",
        batch.provisional_entities.values(),
    )
    _write_records(output_dir / "resolution_attempts.jsonl", batch.attempts.values())
    _write_records(output_dir / "failures.jsonl", batch.failures)
    report = {
        "dry_run": True,
        "database_reads": False,
        "database_writes": False,
        "gcs_reads": False,
        "gcs_writes": False,
        "network_reads": ["nflverse_github_releases", "polymarket_gamma"],
        "provider": provider.provider_name,
        "model_name": provider.model_name,
        "events": len(events),
        "markets": sum(len(event["markets"]) for event in events),
        "classifications": len(batch.classifications),
        "mentions": len(batch.mentions),
        "resolved": sum(
            row["resolution_status"] == "resolved" for row in batch.mentions.values()
        ),
        "ambiguous": sum(
            row["resolution_status"] == "ambiguous" for row in batch.mentions.values()
        ),
        "unresolved": sum(
            row["resolution_status"] == "unresolved" for row in batch.mentions.values()
        ),
        "ignored": sum(
            row["resolution_status"] == "ignored" for row in batch.mentions.values()
        ),
        "provisional_entities": len(batch.provisional_entities),
        "failures": len(batch.failures),
        "input_tokens": batch.input_tokens,
        "output_tokens": batch.output_tokens,
        "nflverse": summary(snapshot),
        "output_dir": str(output_dir),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if batch.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
