"""Run a manually triggered, high-accuracy, no-write entity-resolution sweep."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.enrich_news.config import load_enrichment_settings
from src.entity_bank.models import (
    AccuracySweepDecision,
    CandidateEntity,
    EntityType,
    ExtractedMention,
    MentionRole,
    PersonRoleHint,
)
from src.entity_bank.provider import (
    ClaudeEntityProvider,
    DeterministicEntityProvider,
    EntityProvider,
)
from src.entity_bank.resolution_repository import ResolutionRepository
from src.entity_bank.resolver import CandidateIndex

NETWORK_CONFIRMATION = "RUN_ENTITY_ACCURACY_SWEEP"
DEFAULT_MODEL = "claude-opus-5"
DEFAULT_LIMIT = 50
MAX_LIMIT = 10_000
DEFAULT_CONFIDENCE_THRESHOLD = 0.9


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope",
        choices=("needs_review", "resolved", "all"),
        default="needs_review",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--provider", choices=("mock", "claude"), default="claude")
    parser.add_argument("--model", default=os.getenv("ENTITY_SWEEP_MODEL", DEFAULT_MODEL))
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--confidence-threshold",
        type=float,
        default=DEFAULT_CONFIDENCE_THRESHOLD,
    )
    parser.add_argument(
        "--confirm-network",
        help=f"Required with --provider claude; must equal {NETWORK_CONFIRMATION}",
    )
    return parser


def _source_context(row: dict[str, Any]) -> str:
    values = [
        row.get("news_text"),
        row.get("market_event_title"),
        row.get("direct_event_title"),
        row.get("market_question"),
        row.get("market_slug"),
        row.get("evidence"),
    ]
    unique: list[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        if cleaned and cleaned not in unique:
            unique.append(cleaned)
    return "\n\n".join(unique)[:20_000]


def _mention(row: dict[str, Any]) -> ExtractedMention:
    role_hint = row.get("person_role_hint")
    return ExtractedMention(
        text=str(row["mention_text"]),
        entity_type=EntityType(str(row["entity_type_hint"])),
        person_role_hint=PersonRoleHint(str(role_hint)) if role_hint else None,
        mention_role=MentionRole(str(row["mention_role"])),
        evidence=str(row.get("evidence") or row["mention_text"]),
        confidence=float(row.get("confidence") or 0),
        source_refs=[str(value) for value in row.get("source_refs") or []],
    )


def _decision_key(decision: AccuracySweepDecision) -> tuple[Any, ...]:
    candidates: tuple[str, ...] = ()
    if decision.status.value == "ambiguous":
        candidates = tuple(sorted(decision.candidate_entity_ids))
    return (decision.status.value, decision.entity_id, candidates)


def _current_key(row: dict[str, Any]) -> tuple[Any, ...]:
    candidates: tuple[str, ...] = ()
    if row["resolution_status"] == "ambiguous":
        candidates = tuple(sorted(str(value) for value in row.get("candidate_entity_ids") or []))
    return (str(row["resolution_status"]), row.get("entity_id"), candidates)


def _expanded_candidates(
    row: dict[str, Any],
    mention: ExtractedMention,
    index: CandidateIndex,
) -> list[CandidateEntity]:
    candidates = index.retrieve(mention, limit=20, minimum_score=0.45)
    current_entity_id = row.get("entity_id")
    if current_entity_id and all(
        candidate.entity_id != current_entity_id for candidate in candidates
    ):
        current = index.get(str(current_entity_id))
        if current is not None and current.entity_type == mention.entity_type:
            candidates.append(current)
    return candidates


def evaluate_record(
    row: dict[str, Any],
    *,
    index: CandidateIndex,
    provider: EntityProvider,
    confidence_threshold: float,
) -> dict[str, Any]:
    mention = _mention(row)
    candidates = _expanded_candidates(row, mention, index)
    current_resolution = {
        "resolution_status": str(row["resolution_status"]),
        "entity_id": row.get("entity_id"),
        "match_method": row.get("match_method"),
        "confidence": float(row.get("confidence") or 0),
        "candidate_entity_ids": [
            str(value) for value in row.get("candidate_entity_ids") or []
        ],
        "resolver_version": str(row.get("resolver_version") or "unknown"),
        "resolution_metadata": row.get("resolution_metadata") or {},
    }
    context = _source_context(row)
    as_of = row.get("last_observed_at") or datetime.now(UTC)
    passes = [
        provider.adjudicate_accuracy_sweep(
            mention=mention,
            candidates=candidates,
            source_context=context,
            current_resolution=current_resolution,
            as_of=as_of,
            pass_number=pass_number,
        )
        for pass_number in (1, 2)
    ]
    first: AccuracySweepDecision = passes[0].output
    second: AccuracySweepDecision = passes[1].output
    agrees = _decision_key(first) == _decision_key(second)
    minimum_confidence = min(first.confidence, second.confidence)
    if not agrees:
        outcome = "pass_disagreement"
        recommended = None
    elif minimum_confidence < confidence_threshold:
        outcome = "low_confidence"
        recommended = first
    elif _decision_key(first) == _current_key(row):
        outcome = "confirmed"
        recommended = first
    else:
        outcome = "proposed_change"
        recommended = first

    return {
        "mention_id": str(row["mention_id"]),
        "source_kind": (
            "news"
            if row.get("news_id")
            else "polymarket_market"
            if row.get("polymarket_market_id")
            else "polymarket_event"
        ),
        "source_id": str(
            row.get("news_id")
            or row.get("polymarket_market_id")
            or row.get("polymarket_event_id")
        ),
        "mention_text": mention.text,
        "entity_type_hint": mention.entity_type.value,
        "evidence": mention.evidence,
        "source_context": context,
        "expected_updated_at": row["updated_at"],
        "current_resolution": current_resolution,
        "candidate_snapshot": [
            candidate.model_dump(mode="json") for candidate in candidates
        ],
        "pass_decisions": [
            result.output.model_dump(mode="json") for result in passes
        ],
        "outcome": outcome,
        "recommended_resolution": (
            recommended.model_dump(mode="json") if recommended else None
        ),
        "minimum_confidence": minimum_confidence,
        "usage": {
            "input_tokens": sum(result.usage.input_tokens for result in passes),
            "output_tokens": sum(result.usage.output_tokens for result in passes),
        },
        "provider": passes[0].provider,
        "model_name": passes[0].model_name,
    }


def run_sweep_records(
    records: Iterable[dict[str, Any]],
    *,
    candidate_rows: list[dict[str, Any]],
    provider: EntityProvider,
    confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
    progress: Callable[[int, int, dict[str, int]], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = list(records)
    index = CandidateIndex(candidate_rows)
    findings: list[dict[str, Any]] = []
    counts = {
        "confirmed": 0,
        "proposed_change": 0,
        "pass_disagreement": 0,
        "low_confidence": 0,
        "error": 0,
    }
    input_tokens = 0
    output_tokens = 0
    for position, row in enumerate(rows, start=1):
        try:
            finding = evaluate_record(
                row,
                index=index,
                provider=provider,
                confidence_threshold=confidence_threshold,
            )
        except Exception as error:
            finding = {
                "mention_id": str(row.get("mention_id") or "unknown"),
                "mention_text": str(row.get("mention_text") or "Unknown mention"),
                "outcome": "error",
                "error": f"{type(error).__name__}: {error}",
            }
        findings.append(finding)
        outcome = str(finding["outcome"])
        counts[outcome] = counts.get(outcome, 0) + 1
        usage = finding.get("usage") or {}
        input_tokens += int(usage.get("input_tokens") or 0)
        output_tokens += int(usage.get("output_tokens") or 0)
        if progress:
            progress(position, len(rows), dict(counts))
    return findings, {
        "total": len(rows),
        "processed": len(findings),
        **counts,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }


def _write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(value, default=str, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _default_output_dir() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return Path("data/local/entity_accuracy_sweep") / f"{stamp}-{uuid.uuid4().hex[:8]}"


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 1 <= args.limit <= MAX_LIMIT:
        raise SystemExit(f"--limit must be between 1 and {MAX_LIMIT}")
    if not 0.5 <= args.confidence_threshold <= 1:
        raise SystemExit("--confidence-threshold must be between 0.5 and 1")
    if args.provider == "claude" and args.confirm_network != NETWORK_CONFIRMATION:
        raise SystemExit(
            "Claude sweep calls are billable; pass "
            f"--confirm-network {NETWORK_CONFIRMATION}"
        )

    output_dir = args.output_dir or _default_output_dir()
    output_dir.mkdir(parents=True, exist_ok=False)
    started_at = datetime.now(UTC)
    _write_json(
        output_dir / "progress.json",
        {
            "status": "starting",
            "processed": 0,
            "total": 0,
            "counts": {},
            "started_at": started_at,
        },
    )

    settings = load_enrichment_settings(Path("src"))
    provider: EntityProvider
    if args.provider == "mock":
        provider = DeterministicEntityProvider()
    else:
        provider = ClaudeEntityProvider(
            settings.api_key or "",
            model_name=args.model,
            max_tokens=int(os.getenv("ENTITY_SWEEP_MAX_OUTPUT_TOKENS", "4096")),
            timeout_seconds=180,
        )

    repository = ResolutionRepository.from_environment(Path("src"))
    try:
        records = repository.load_mentions_for_accuracy_sweep(
            scope=args.scope,
            limit=args.limit,
        )
        candidate_rows = repository.load_candidate_rows()

        def update_progress(processed: int, total: int, counts: dict[str, int]) -> None:
            _write_json(
                output_dir / "progress.json",
                {
                    "status": "running",
                    "processed": processed,
                    "total": total,
                    "counts": counts,
                    "started_at": started_at,
                    "updated_at": datetime.now(UTC),
                },
            )

        findings, counters = run_sweep_records(
            records,
            candidate_rows=candidate_rows,
            provider=provider,
            confidence_threshold=args.confidence_threshold,
            progress=update_progress,
        )
    finally:
        repository.close()

    with (output_dir / "findings.jsonl").open("w", encoding="utf-8") as handle:
        for finding in findings:
            handle.write(json.dumps(finding, default=str, sort_keys=True) + "\n")

    completed_at = datetime.now(UTC)
    summary = {
        "status": "completed_with_errors" if counters["error"] else "completed",
        "dry_run": True,
        "database_reads": True,
        "database_writes": False,
        "network_calls": args.provider == "claude",
        "scope": args.scope,
        "limit": args.limit,
        "provider": provider.provider_name,
        "model_name": provider.model_name,
        "confidence_threshold": args.confidence_threshold,
        "passes_per_mention": 2,
        "started_at": started_at,
        "completed_at": completed_at,
        "output_dir": str(output_dir.resolve()),
        **counters,
    }
    _write_json(output_dir / "summary.json", summary)
    _write_json(
        output_dir / "progress.json",
        {
            "status": summary["status"],
            "processed": counters["processed"],
            "total": counters["total"],
            "counts": {
                key: counters[key]
                for key in (
                    "confirmed",
                    "proposed_change",
                    "pass_disagreement",
                    "low_confidence",
                    "error",
                )
            },
            "started_at": started_at,
            "completed_at": completed_at,
        },
    )
    print(json.dumps(summary, default=str, indent=2, sort_keys=True))
    return 0 if counters["total"] == 0 or counters["error"] < counters["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
