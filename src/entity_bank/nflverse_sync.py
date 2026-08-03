"""Synchronize nflverse entities; local-output dry run unless --apply is explicit."""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.common.gcs import create_gcs_client
from src.entity_bank.nflverse_pipeline import (
    NflverseClient,
    build_envelope,
    build_object_path,
    encode_envelope,
    fetch_snapshot,
    summary,
)
from src.entity_bank.repository import EntityBankRepository
from src.ingest_odds.polymarket_pipeline import load_config

WRITE_CONFIRMATION = "APPLY_NFLVERSE_ENTITY_BANK"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=datetime.now(UTC).year)
    parser.add_argument(
        "--limit",
        type=int,
        help="Dry-run-only cap on entities written to the local audit JSONL",
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Archive exact source files to GCS and persist normalized rows",
    )
    parser.add_argument(
        "--confirm-live-writes",
        help=f"Required with --apply; must equal {WRITE_CONFIRMATION}",
    )
    return parser


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as output:
        for record in records:
            output.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            output.write("\n")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.season < 1920 or args.season > datetime.now(UTC).year + 1:
        print("ERROR: --season is outside nflverse's supported NFL range", file=sys.stderr)
        return 2
    if args.limit is not None and args.limit < 1:
        print("ERROR: --limit must be positive", file=sys.stderr)
        return 2
    if args.apply and args.limit is not None:
        print("ERROR: --limit cannot be used with --apply", file=sys.stderr)
        return 2
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
        else project_root / "data" / "local" / "entity_bank" / f"nflverse_{timestamp}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    snapshot = fetch_snapshot(NflverseClient(), season=args.season)
    audit_entities = list(snapshot.entities)
    if args.limit is not None:
        audit_entities = audit_entities[: args.limit]
    _write_jsonl(output_dir / "proposed_entities.jsonl", audit_entities)
    current_players = [
        entity
        for entity in snapshot.entities
        if entity["entity_type"] == "person"
    ]
    _write_jsonl(output_dir / "current_players.jsonl", current_players)
    _write_jsonl(
        output_dir / "complete_player_history.jsonl",
        list(snapshot.complete_player_history),
    )
    _write_jsonl(
        output_dir / "proposed_relationships.jsonl",
        list(snapshot.relationships),
    )
    _write_jsonl(
        output_dir / "quarantined_records.jsonl",
        list(snapshot.quarantined_records),
    )
    _write_jsonl(
        output_dir / "source_mapping_conflicts.jsonl",
        list(snapshot.source_mapping_conflicts),
    )

    run_id = uuid.uuid4().hex
    object_path = build_object_path(snapshot.observed_at, run_id)
    config = load_config(src_dir / "config" / "polymarket_config.json")
    storage_uri = f"gs://{config.bucket_name}/{object_path}"
    envelope = build_envelope(
        snapshot,
        ingest_run_id=run_id,
        storage_uri=storage_uri,
    )
    report: dict[str, Any] = {
        "dry_run": not args.apply,
        "database_reads": args.apply,
        "database_writes": False,
        "gcs_writes": False,
        "output_dir": str(output_dir),
        "proposed_storage_uri": storage_uri,
        **summary(snapshot),
    }

    if args.apply:
        if snapshot.quality["unsafe_source_mapping_collisions"]:
            print(
                "ERROR: refusing --apply with unresolved source mapping collisions",
                file=sys.stderr,
            )
            return 2
        repository = EntityBankRepository.from_environment(src_dir)
        try:
            if repository.latest_content_sha256() == snapshot.content_sha256:
                report["skipped_unchanged_snapshot"] = True
            else:
                gcs_client = create_gcs_client(src_dir)
                blob = gcs_client.bucket(config.bucket_name).blob(object_path)
                blob.metadata = {
                    "schema_name": envelope["schema_name"],
                    "schema_version": str(envelope["schema_version"]),
                    "content_sha256": envelope["content_sha256"],
                    "ingest_run_id": run_id,
                }
                blob.content_encoding = "gzip"
                blob.upload_from_string(
                    encode_envelope(envelope),
                    content_type="application/json",
                )
                report["gcs_writes"] = True
                report["persisted"] = repository.persist_nflverse_snapshot(envelope)
                report["database_writes"] = True
        finally:
            repository.close()

    (output_dir / "summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
