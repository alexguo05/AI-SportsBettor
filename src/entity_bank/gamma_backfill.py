"""Backfill entity-relevant Gamma fields; no database writes without --apply."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import bindparam, select, update

from src.common.gcs import create_gcs_client
from src.db.engine import create_database_resources
from src.db.models import polymarket_events, polymarket_markets, raw_ingest_objects
from src.entity_bank.resolver import serialize_audit_record
from src.ingest_odds.polymarket_pipeline import decode_envelope, normalize_event

WRITE_CONFIRMATION = "APPLY_GAMMA_ENTITY_BACKFILL"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--confirm-live-writes",
        help=f"Required with --apply; must equal {WRITE_CONFIRMATION}",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.apply and args.confirm_live_writes != WRITE_CONFIRMATION:
        print(
            f"ERROR: --apply requires --confirm-live-writes {WRITE_CONFIRMATION}",
            file=sys.stderr,
        )
        return 2

    project_root = Path(__file__).resolve().parents[2]
    src_dir = project_root / "src"
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else project_root / "data" / "local" / "entity_bank" / f"gamma_backfill_{timestamp}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    resources = create_database_resources(src_dir)
    try:
        with resources.engine.connect() as connection:
            storage_uri = connection.scalar(
                select(raw_ingest_objects.c.storage_uri)
                .where(
                    raw_ingest_objects.c.provider == "polymarket",
                    raw_ingest_objects.c.source == "gamma",
                    raw_ingest_objects.c.object_type == "events",
                )
                .order_by(raw_ingest_objects.c.ingested_at.desc())
                .limit(1)
            )
        if not storage_uri:
            print("ERROR: no archived Gamma event envelope found", file=sys.stderr)
            return 2
        parsed = urlparse(storage_uri)
        if parsed.scheme != "gs":
            print(f"ERROR: unsupported storage URI {storage_uri}", file=sys.stderr)
            return 2
        data = (
            create_gcs_client(src_dir)
            .bucket(parsed.netloc)
            .blob(parsed.path.lstrip("/"))
            .download_as_bytes()
        )
        envelope = decode_envelope(data)
        observed_at = datetime.fromisoformat(envelope["ingested_at"])
        normalized: dict[str, dict] = {}
        for response in envelope.get("raw_api_responses", []):
            if isinstance(response, dict):
                raw_events = response.get("events", [])
            elif isinstance(response, list):
                raw_events = response
            else:
                raw_events = []
            for raw_event in raw_events:
                event = normalize_event(raw_event, observed_at)
                if event is not None:
                    normalized[event["event_id"]] = event

        event_rows = [
            {"_event_id": event["event_id"], "game_id": event.get("game_id")}
            for event in normalized.values()
        ]
        market_rows = [
            {
                "_market_id": market["market_id"],
                "group_item_title": market.get("group_item_title"),
                "group_item_threshold": market.get("group_item_threshold"),
            }
            for event in normalized.values()
            for market in event["markets"]
        ]
        with (output_dir / "event_updates.jsonl").open("w", encoding="utf-8") as output:
            for row in event_rows:
                output.write(serialize_audit_record(row) + "\n")
        with (output_dir / "market_updates.jsonl").open("w", encoding="utf-8") as output:
            for row in market_rows:
                output.write(serialize_audit_record(row) + "\n")

        if args.apply:
            with resources.engine.begin() as connection:
                if event_rows:
                    connection.execute(
                        update(polymarket_events)
                        .where(polymarket_events.c.event_id == bindparam("_event_id"))
                        .values(game_id=bindparam("game_id")),
                        event_rows,
                    )
                if market_rows:
                    connection.execute(
                        update(polymarket_markets)
                        .where(polymarket_markets.c.market_id == bindparam("_market_id"))
                        .values(
                            group_item_title=bindparam("group_item_title"),
                            group_item_threshold=bindparam("group_item_threshold"),
                        ),
                        market_rows,
                    )
    finally:
        resources.close()

    summary = {
        "dry_run": not args.apply,
        "database_reads": True,
        "database_writes": args.apply,
        "gcs_reads": True,
        "gcs_writes": False,
        "storage_uri": storage_uri,
        "event_updates": len(event_rows),
        "events_with_game_id": sum(bool(row["game_id"]) for row in event_rows),
        "market_updates": len(market_rows),
        "markets_with_group_item_title": sum(
            bool(row["group_item_title"]) for row in market_rows
        ),
        "output_dir": str(output_dir),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
