"""Backfill normalized news with explicit write authorization.

Without --apply, this command reads candidate rows and writes results only to a
local JSONL file. --apply persists versioned results to PostgreSQL and requires
an additional confirmation phrase.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from src.db.engine import create_database_resources
from src.enrich_news.config import (
    MAX_OUTPUT_TOKENS,
    MIN_OUTPUT_TOKENS,
    load_enrichment_settings,
)
from src.enrich_news.models import EnrichmentResult
from src.enrich_news.pipeline import enrich_record
from src.enrich_news.provider import ClaudeProvider, DeterministicDryRunProvider
from src.enrich_news.repository import EnrichmentRepository

WRITE_CONFIRMATION = "APPLY_ENRICHMENTS"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--version")
    parser.add_argument("--provider", choices=("mock", "claude"), default="claude")
    parser.add_argument(
        "--model",
        help="Override NEWS_ENRICHMENT_MODEL",
    )
    parser.add_argument("--max-output-tokens", type=int)
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist results to PostgreSQL; otherwise this is a local-output dry run",
    )
    parser.add_argument(
        "--confirm-live-writes",
        help=f"Required with --apply; must equal {WRITE_CONFIRMATION}",
    )
    return parser


def _write_results(path: Path, results: list[EnrichmentResult]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as output_file:
        for result in results:
            output_file.write(result.model_dump_json(exclude_none=True))
            output_file.write("\n")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.limit < 1 or args.limit > 100:
        print("ERROR: --limit must be between 1 and 100", file=sys.stderr)
        return 2
    if args.apply and args.confirm_live_writes != WRITE_CONFIRMATION:
        print(
            f"ERROR: --apply requires --confirm-live-writes {WRITE_CONFIRMATION}",
            file=sys.stderr,
        )
        return 2

    project_root = Path(__file__).resolve().parents[2]
    src_dir = project_root / "src"
    try:
        settings = load_enrichment_settings(src_dir)
    except (TypeError, ValueError) as exc:
        print(f"ERROR: invalid enrichment configuration: {exc}", file=sys.stderr)
        return 2
    model_name = args.model or settings.model_name
    enrichment_version = args.version or settings.enrichment_version
    max_output_tokens = args.max_output_tokens or settings.max_output_tokens
    if not MIN_OUTPUT_TOKENS <= max_output_tokens <= MAX_OUTPUT_TOKENS:
        print(
            f"ERROR: --max-output-tokens must be between "
            f"{MIN_OUTPUT_TOKENS} and {MAX_OUTPUT_TOKENS}",
            file=sys.stderr,
        )
        return 2
    if args.provider == "claude":
        if not settings.api_key:
            print("ERROR: ANTHROPIC_API_KEY is not configured", file=sys.stderr)
            return 2
        provider = ClaudeProvider(
            settings.api_key,
            model_name=model_name,
            max_tokens=max_output_tokens,
        )
    else:
        provider = DeterministicDryRunProvider()

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else project_root / "data" / "local" / "enrichment_backfills" / timestamp
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    resources = create_database_resources(src_dir)
    repository = EnrichmentRepository(resources)
    try:
        records = repository.load_candidates(
            enrichment_version=enrichment_version,
            limit=args.limit,
        )
        results = [
            enrich_record(
                record,
                provider,
                enrichment_version=enrichment_version,
                allow_network=args.allow_network,
            )
            for record in records
        ]
        _write_results(output_dir / "enrichment_results.jsonl", results)
        if args.apply:
            for result in results:
                repository.persist_result(result)
    finally:
        resources.close()

    summary = {
        "dry_run": not args.apply,
        "database_reads": True,
        "database_writes": args.apply,
        "gcs_writes": False,
        "provider": provider.provider_name,
        "model_name": provider.model_name,
        "network_source_reads": args.allow_network,
        "record_count": len(results),
        "completed": sum(result.status.startswith("completed") for result in results),
        "failed": sum(result.status == "failed" for result in results),
        "input_tokens": sum(result.usage.input_tokens for result in results),
        "output_tokens": sum(result.usage.output_tokens for result in results),
        "output_dir": str(output_dir),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
