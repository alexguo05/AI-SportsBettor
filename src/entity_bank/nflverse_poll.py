"""Continuously synchronize changed nflverse entity snapshots."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from src.common.gcs import create_gcs_client
from src.entity_bank.nflverse_pipeline import (
    NflverseClient,
    NflverseSnapshot,
    build_envelope,
    build_object_path,
    encode_envelope,
    fetch_snapshot,
    summary,
)
from src.entity_bank.repository import EntityBankRepository
from src.ingest_odds.polymarket_pipeline import load_config

WRITE_CONFIRMATION = "RUN_NFLVERSE_ENTITY_POLL"
DEFAULT_POLL_INTERVAL_SECONDS = 86_400
MINIMUM_POLL_INTERVAL_SECONDS = 300

LOGGER = logging.getLogger(__name__)


class PollRepository(Protocol):
    def latest_content_sha256(self, source: str = "nflverse") -> str | None: ...

    def persist_nflverse_snapshot(
        self,
        envelope: dict[str, Any],
    ) -> dict[str, int | str]: ...


def inferred_nfl_season(now: datetime) -> int:
    utc = now.astimezone(UTC)
    return utc.year if utc.month >= 3 else utc.year - 1


def run_cycle(
    *,
    client: NflverseClient,
    repository: PollRepository,
    bucket_factory: Callable[[], Any],
    bucket_name: str,
    season: int,
    now: datetime | None = None,
    run_id_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
) -> dict[str, Any]:
    snapshot: NflverseSnapshot = fetch_snapshot(
        client,
        season=season,
        now=now,
    )
    report: dict[str, Any] = {
        "database_reads": True,
        "database_writes": False,
        "gcs_writes": False,
        "skipped_unchanged_snapshot": False,
        **summary(snapshot),
    }
    if snapshot.quality["unsafe_source_mapping_collisions"]:
        raise RuntimeError(
            "refusing to persist nflverse snapshot with unresolved source "
            "mapping collisions"
        )
    if repository.latest_content_sha256() == snapshot.content_sha256:
        report["skipped_unchanged_snapshot"] = True
        return report

    run_id = run_id_factory()
    object_path = build_object_path(snapshot.observed_at, run_id)
    storage_uri = f"gs://{bucket_name}/{object_path}"
    envelope = build_envelope(
        snapshot,
        ingest_run_id=run_id,
        storage_uri=storage_uri,
    )
    bucket = bucket_factory()
    blob = bucket.blob(object_path)
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
    report["storage_uri"] = storage_uri
    report["persisted"] = repository.persist_nflverse_snapshot(envelope)
    report["database_writes"] = True
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--season",
        type=int,
        help="Override the inferred NFL season; normally unnecessary",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one applied cycle and exit (service smoke test)",
    )
    parser.add_argument(
        "--confirm-live-writes",
        help=f"Required; must equal {WRITE_CONFIRMATION}",
    )
    return parser


def _run_live_cycle(
    *,
    project_root: Path,
    season_override: int | None,
) -> dict[str, Any]:
    src_dir = project_root / "src"
    config = load_config(src_dir / "config" / "polymarket_config.json")
    repository = EntityBankRepository.from_environment(src_dir)
    now = datetime.now(UTC)
    season = season_override or inferred_nfl_season(now)
    try:
        return run_cycle(
            client=NflverseClient(),
            repository=repository,
            bucket_factory=lambda: create_gcs_client(src_dir).bucket(
                config.bucket_name
            ),
            bucket_name=config.bucket_name,
            season=season,
            now=now,
        )
    finally:
        repository.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.confirm_live_writes != WRITE_CONFIRMATION:
        print(
            f"ERROR: --confirm-live-writes must equal {WRITE_CONFIRMATION}",
            file=sys.stderr,
        )
        return 2
    if args.poll_interval_seconds < MINIMUM_POLL_INTERVAL_SECONDS:
        print(
            f"ERROR: --poll-interval-seconds must be at least "
            f"{MINIMUM_POLL_INTERVAL_SECONDS}",
            file=sys.stderr,
        )
        return 2
    current_year = datetime.now(UTC).year
    if args.season is not None and not 1920 <= args.season <= current_year + 1:
        print("ERROR: --season is outside nflverse's supported range", file=sys.stderr)
        return 2

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    project_root = Path(__file__).resolve().parents[2]
    while True:
        started_at = datetime.now(UTC)
        try:
            report = _run_live_cycle(
                project_root=project_root,
                season_override=args.season,
            )
            report["cycle_started_at"] = started_at.isoformat()
            report["cycle_completed_at"] = datetime.now(UTC).isoformat()
            LOGGER.info("NFLVERSE_POLL_RESULT %s", json.dumps(report, sort_keys=True))
        except Exception:
            LOGGER.exception("NFLVERSE_POLL_FAILED")
            if args.once:
                return 1
        if args.once:
            return 0
        try:
            time.sleep(args.poll_interval_seconds)
        except KeyboardInterrupt:
            LOGGER.info("NFLVERSE_POLL_STOPPED")
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
