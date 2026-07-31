"""Orchestration for one independently auditable enrichment."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from src.enrich_news.models import (
    EnrichmentOutput,
    EnrichmentResult,
    NewsRecord,
    ProviderUsage,
)
from src.enrich_news.prompt import PROMPT_VERSION
from src.enrich_news.provider import EnrichmentProvider
from src.enrich_news.sources import collect_evidence


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


def _normalize_source_refs(
    output: EnrichmentOutput,
    aliases: dict[str, str],
) -> None:
    """Replace exact known aliases while preserving order and rejecting all others later."""

    for item in [*output.tags, *output.entities, *output.claims]:
        normalized = (
            aliases.get(source_ref.strip(), source_ref.strip()) for source_ref in item.source_refs
        )
        item.source_refs = list(dict.fromkeys(normalized))


def enrich_record(
    record: NewsRecord,
    provider: EnrichmentProvider,
    *,
    enrichment_version: str = "v1",
    allow_network: bool = False,
) -> EnrichmentResult:
    started_at = datetime.now(UTC)
    evidence = collect_evidence(record, allow_network=allow_network)
    if len(evidence.images) > 20:
        evidence.images = evidence.images[:20]
        evidence.warnings.append("image input limit reached; only the first 20 were analyzed")
    input_manifest = {
        "prompt_version": PROMPT_VERSION,
        "network_enabled": allow_network,
        **evidence.manifest,
        "image_inputs": [
            {
                "source_ref": image.source_ref,
                "media_type": image.media_type,
                "content_sha256": image.sha256,
                "byte_size": len(image.data),
            }
            for image in evidence.images
        ],
    }
    input_fingerprint = hashlib.sha256(
        _canonical_bytes(
            {
                "record": record.model_dump(mode="json"),
                "input_manifest": input_manifest,
                "evidence_text": evidence.as_prompt_text(),
            }
        )
    ).hexdigest()
    response = None
    try:
        response = provider.enrich(evidence)
        source_aliases = {record.source_url: "tweet"} if record.source_url else {}
        _normalize_source_refs(response.output, source_aliases)
        valid_source_refs = set(evidence.source_refs())
        used_source_refs = {
            source_ref
            for item in (
                [*response.output.tags, *response.output.entities, *response.output.claims]
            )
            for source_ref in item.source_refs
        }
        unknown_refs = sorted(used_source_refs - valid_source_refs)
        if unknown_refs:
            raise ValueError(f"provider returned unknown source references: {unknown_refs}")
        return EnrichmentResult(
            news_id=record.news_id,
            enrichment_version=enrichment_version,
            provider=provider.provider_name,
            model_name=response.model_name,
            status="completed_with_warnings" if evidence.warnings else "completed",
            input_fingerprint=input_fingerprint,
            input_manifest=input_manifest,
            output=response.output,
            usage=response.usage,
            warnings=evidence.warnings,
            started_at=started_at,
            completed_at=datetime.now(UTC),
        )
    except Exception as exc:
        return EnrichmentResult(
            news_id=record.news_id,
            enrichment_version=enrichment_version,
            provider=provider.provider_name,
            model_name=response.model_name if response else provider.model_name,
            status="failed",
            input_fingerprint=input_fingerprint,
            input_manifest=input_manifest,
            usage=response.usage if response else ProviderUsage(),
            warnings=evidence.warnings,
            error=f"{type(exc).__name__}: {str(exc)[:400]}",
            started_at=started_at,
            completed_at=datetime.now(UTC),
        )
