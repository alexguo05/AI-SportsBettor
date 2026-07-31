"""Local-only enrichment dry run.

This command never initializes the database or GCS clients. Inputs are read from
JSONL and outputs are written to a local directory.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from src.enrich_news.config import (
    MAX_OUTPUT_TOKENS,
    MIN_OUTPUT_TOKENS,
    load_enrichment_settings,
)
from src.enrich_news.models import EnrichmentResult, NewsRecord
from src.enrich_news.pipeline import enrich_record
from src.enrich_news.provider import ClaudeProvider, DeterministicDryRunProvider


def _records(path: Path, limit: int) -> list[NewsRecord]:
    records: list[NewsRecord] = []
    with path.open("r", encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, start=1):
            if not line.strip():
                continue
            try:
                records.append(NewsRecord.model_validate_json(line))
            except Exception as exc:
                raise ValueError(f"invalid JSONL record at line {line_number}: {exc}") from exc
            if len(records) >= limit:
                break
    return records


def _write_jsonl(path: Path, results: list[EnrichmentResult]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as output_file:
        for result in results:
            output_file.write(result.model_dump_json(exclude_none=True))
            output_file.write("\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Normalized local JSONL input")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Local output directory (default: data/local/enrichment_dry_runs/<timestamp>)",
    )
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--version")
    parser.add_argument(
        "--provider",
        choices=("mock", "claude"),
        default="mock",
        help="mock is fully offline; claude makes Anthropic API calls",
    )
    parser.add_argument(
        "--model",
        help="Override NEWS_ENRICHMENT_MODEL",
    )
    parser.add_argument(
        "--max-output-tokens",
        type=int,
        help="Override NEWS_ENRICHMENT_MAX_OUTPUT_TOKENS",
    )
    parser.add_argument(
        "--allow-network",
        action="store_true",
        help="Allow bounded public article/media reads; never enables cloud writes",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.limit < 1 or args.limit > 100:
        print("ERROR: --limit must be between 1 and 100", file=sys.stderr)
        return 2
    input_path = args.input.resolve()
    if not input_path.is_file():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
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
            print(
                "ERROR: ANTHROPIC_API_KEY is required in the process environment or src/.env",
                file=sys.stderr,
            )
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
        else project_root / "data" / "local" / "enrichment_dry_runs" / timestamp
    )
    output_dir.mkdir(parents=True, exist_ok=False)
    records = _records(input_path, args.limit)
    results = [
        enrich_record(
            record,
            provider,
            enrichment_version=enrichment_version,
            allow_network=args.allow_network,
        )
        for record in records
    ]
    output_path = output_dir / "enrichment_results.jsonl"
    _write_jsonl(output_path, results)
    summary = {
        "dry_run": True,
        "durable_external_writes": False,
        "provider": provider.provider_name,
        "model_name": provider.model_name,
        "network_reads_enabled": args.allow_network,
        "input_path": str(input_path),
        "output_path": str(output_path),
        "record_count": len(results),
        "completed": sum(result.status.startswith("completed") for result in results),
        "failed": sum(result.status == "failed" for result in results),
        "input_tokens": sum(result.usage.input_tokens for result in results),
        "output_tokens": sum(result.usage.output_tokens for result in results),
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
