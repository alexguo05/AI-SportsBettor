"""Pure candidate retrieval, validated resolution, and audit-row construction."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
from typing import Any

from src.common.gcs import canonical_json_bytes
from src.entity_bank.models import (
    AliasType,
    CandidateEntity,
    ExtractedMention,
    IdentityStatus,
    MatchMethod,
    ResolutionDecision,
    ResolutionStatus,
)
from src.entity_bank.nflverse_pipeline import ENTITY_NAMESPACE
from src.entity_bank.normalization import normalize_name
from src.entity_bank.prompt import EXTRACTOR_VERSION, RESOLVER_VERSION
from src.entity_bank.provider import EntityProvider, ProviderResult, ProviderUsage

PROVISIONAL_MAX_LEXICAL_SCORE = 0.84


def deterministic_alias_match_is_safe(
    text: str,
    *,
    entity_type: str,
    aliases: list[str] | set[str],
) -> bool:
    """Require case-sensitive team context for collision-prone short aliases."""

    compact = "".join(character for character in normalize_name(text) if character.isalnum())
    if len(compact) > 3:
        return True
    return (
        entity_type == "team"
        and text.isupper()
        and text in aliases
    )


@dataclass(frozen=True)
class SourceReference:
    source_kind: str
    source_id: str
    source_content_sha256: str
    event_id: str | None = None
    market_id: str | None = None
    news_id: str | None = None
    kalshi_market_ticker: str | None = None


class CandidateIndex:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self._aliases: list[list[str]] = []
        self._tokens: dict[str, set[int]] = {}
        self._prefixes: dict[str, set[int]] = {}
        for index, row in enumerate(rows):
            aliases = sorted(
                {
                    normalize_name(row["canonical_name"]),
                    *(
                        normalize_name(alias)
                        for alias in row.get("aliases", [])
                        if alias
                    ),
                }
            )
            self._aliases.append(aliases)
            for alias in aliases:
                for token in alias.split():
                    if len(token) >= 2:
                        self._tokens.setdefault(token, set()).add(index)
                if alias:
                    self._prefixes.setdefault(alias[:2], set()).add(index)

    def exact_matches(
        self,
        text: str,
        *,
        entity_type: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized = normalize_name(text)
        return [
            row
            for index, row in enumerate(self.rows)
            if normalized in self._aliases[index]
            and (entity_type is None or row["entity_type"] == entity_type)
        ]

    def retrieve(
        self,
        mention: ExtractedMention,
        *,
        limit: int = 8,
        minimum_score: float = 0.65,
    ) -> list[CandidateEntity]:
        needle = normalize_name(mention.text)
        candidate_indexes: set[int] = set()
        for token in needle.split():
            candidate_indexes.update(self._tokens.get(token, set()))
        candidate_indexes.update(self._prefixes.get(needle[:2], set()))
        if not candidate_indexes:
            candidate_indexes = set(range(len(self.rows)))
        ranked: list[CandidateEntity] = []
        for row_index in candidate_indexes:
            row = self.rows[row_index]
            if row["entity_type"] != mention.entity_type.value:
                continue
            normalized_aliases = self._aliases[row_index]
            score = max(
                SequenceMatcher(None, needle, alias).ratio()
                for alias in normalized_aliases
            )
            if score < minimum_score:
                continue
            aliases = sorted({row["canonical_name"], *row.get("aliases", [])})
            ranked.append(
                CandidateEntity(
                    entity_id=row["entity_id"],
                    canonical_name=row["canonical_name"],
                    entity_type=row["entity_type"],
                    identity_status=row["identity_status"],
                    aliases=aliases,
                    roles=sorted(set(row.get("roles", []))),
                    teams=sorted(set(row.get("teams", []))),
                    lexical_score=score,
                )
            )
        return sorted(
            ranked,
            key=lambda candidate: (-candidate.lexical_score, candidate.canonical_name),
        )[:limit]

    def get(self, entity_id: str) -> CandidateEntity | None:
        for row_index, row in enumerate(self.rows):
            if row["entity_id"] != entity_id:
                continue
            return CandidateEntity(
                entity_id=row["entity_id"],
                canonical_name=row["canonical_name"],
                entity_type=row["entity_type"],
                identity_status=row["identity_status"],
                aliases=sorted({row["canonical_name"], *row.get("aliases", [])}),
                roles=sorted(set(row.get("roles", []))),
                teams=sorted(set(row.get("teams", []))),
                lexical_score=max(
                    (
                        SequenceMatcher(
                            None,
                            normalize_name(row["canonical_name"]),
                            alias,
                        ).ratio()
                        for alias in self._aliases[row_index]
                    ),
                    default=1.0,
                ),
            )
        return None


def mention_id(
    source: SourceReference,
    mention: ExtractedMention,
    *,
    extractor_version: str = EXTRACTOR_VERSION,
) -> str:
    identity = {
        "source_kind": source.source_kind,
        "source_id": source.source_id,
        "source_content_sha256": source.source_content_sha256,
        "extractor_version": extractor_version,
        "text": normalize_name(mention.text),
        "mention_role": mention.mention_role.value,
        "evidence": normalize_name(mention.evidence),
    }
    digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
    return str(uuid.uuid5(ENTITY_NAMESPACE, f"mention:{digest}"))


def _deterministic_result(
    decision: ResolutionDecision,
    *,
    model_name: str,
) -> ProviderResult:
    return ProviderResult(
        output=decision,
        usage=ProviderUsage(),
        provider="deterministic",
        model_name=model_name,
    )


def _provisional_entity(
    mention: ExtractedMention,
    *,
    observed_at: datetime,
) -> tuple[dict[str, Any], dict[str, Any], ResolutionDecision]:
    normalized = normalize_name(mention.text)
    entity_id = str(uuid.uuid5(ENTITY_NAMESPACE, f"provisional:polymarket:{normalized}"))
    entity = {
        "entity_id": entity_id,
        "entity_type": mention.entity_type.value,
        "canonical_name": mention.text,
        "normalized_name": normalized,
        "identity_status": IdentityStatus.PROVISIONAL.value,
        "merged_into_entity_id": None,
        "latest_bank_version_id": None,
        "first_observed_at": observed_at,
        "last_observed_at": observed_at,
        "updated_at": observed_at,
    }
    alias = {
        "entity_id": entity_id,
        "normalized_alias": normalized,
        "source": "polymarket",
        "alias": mention.text,
        "alias_type": AliasType.PROVIDER_NAME.value,
        "confidence": mention.confidence,
        "first_observed_at": observed_at,
        "last_observed_at": observed_at,
        "source_metadata": {"provisional": True},
    }
    decision = ResolutionDecision(
        status=ResolutionStatus.RESOLVED,
        entity_id=entity_id,
        candidate_entity_ids=[],
        confidence=mention.confidence,
        reason="Structured Polymarket candidate created as a provisional identity.",
    )
    return entity, alias, decision


def resolve_mention(
    *,
    mention: ExtractedMention,
    source: SourceReference,
    source_context: str,
    index: CandidateIndex,
    provider: EntityProvider,
    bank_version_id: str | None,
    observed_at: datetime,
    allow_provisional: bool,
    extractor_version: str = EXTRACTOR_VERSION,
) -> dict[str, Any]:
    candidates = index.retrieve(mention)
    normalized = normalize_name(mention.text)
    exact = [
        candidate
        for candidate in candidates
        if normalized
        in {
            normalize_name(candidate.canonical_name),
            *map(normalize_name, candidate.aliases),
        }
    ]
    provisional_entity = None
    provisional_alias = None
    match_method: str | None = None
    exact_aliases = (
        {
            exact[0].canonical_name,
            *exact[0].aliases,
        }
        if len(exact) == 1
        else set()
    )
    if len(exact) == 1 and deterministic_alias_match_is_safe(
        mention.text,
        entity_type=exact[0].entity_type.value,
        aliases=exact_aliases,
    ):
        raw_alias_match = mention.text in exact_aliases
        match_method = (
            MatchMethod.EXACT_ALIAS.value
            if raw_alias_match
            else MatchMethod.NORMALIZED_ALIAS.value
        )
        provider_result = _deterministic_result(
            ResolutionDecision(
                status=ResolutionStatus.RESOLVED,
                entity_id=exact[0].entity_id,
                candidate_entity_ids=[exact[0].entity_id],
                confidence=1,
                reason="Unique entity-bank alias match.",
            ),
            model_name="exact-alias-v1",
        )
    elif allow_provisional and max(
        (candidate.lexical_score for candidate in candidates),
        default=0,
    ) <= PROVISIONAL_MAX_LEXICAL_SCORE:
        provisional_entity, provisional_alias, decision = _provisional_entity(
            mention,
            observed_at=observed_at,
        )
        match_method = MatchMethod.PROVISIONAL_CREATION.value
        provider_result = _deterministic_result(
            decision,
            model_name="provisional-policy-v1",
        )
    elif not candidates:
        provider_result = _deterministic_result(
            ResolutionDecision(
                status=ResolutionStatus.UNRESOLVED,
                candidate_entity_ids=[],
                confidence=0,
                reason="Candidate retrieval found no plausible entity.",
            ),
            model_name="no-candidates-v1",
        )
    else:
        provider_result = provider.adjudicate(
            mention=mention,
            candidates=candidates,
            source_context=source_context,
            as_of=observed_at,
        )
        if provider_result.output.status == ResolutionStatus.RESOLVED:
            match_method = MatchMethod.CONTEXT_ADJUDICATED.value

    decision = provider_result.output
    current_mention_id = mention_id(
        source,
        mention,
        extractor_version=extractor_version,
    )
    mention_row = {
        "mention_id": current_mention_id,
        "news_id": source.news_id,
        "polymarket_event_id": (
            source.event_id if source.source_kind == "polymarket_event" else None
        ),
        "polymarket_market_id": source.market_id,
        "kalshi_market_ticker": source.kalshi_market_ticker,
        "entity_id": decision.entity_id,
        "mention_text": mention.text,
        "normalized_text": normalized,
        "entity_type_hint": mention.entity_type.value,
        "person_role_hint": (
            mention.person_role_hint.value if mention.person_role_hint else None
        ),
        "mention_role": mention.mention_role.value,
        "evidence": mention.evidence,
        "source_refs": mention.source_refs,
        "source_content_sha256": source.source_content_sha256,
        "extractor_version": extractor_version,
        "resolver_version": RESOLVER_VERSION,
        "resolution_status": decision.status.value,
        "match_method": match_method,
        "confidence": decision.confidence,
        "last_bank_version_id": bank_version_id,
        "candidate_entity_ids": decision.candidate_entity_ids,
        "resolution_metadata": {
            "reason": decision.reason,
            "provider": provider_result.provider,
            "model_name": provider_result.model_name,
        },
        "first_observed_at": observed_at,
        "last_observed_at": observed_at,
        "updated_at": observed_at,
    }
    attempt_identity = {
        "mention_id": current_mention_id,
        "bank_version_id": bank_version_id,
        "resolver_version": RESOLVER_VERSION,
        "provider": provider_result.provider,
        "model_name": provider_result.model_name,
        "decision": decision.model_dump(mode="json"),
    }
    attempt_digest = hashlib.sha256(canonical_json_bytes(attempt_identity)).hexdigest()
    attempt_row = {
        "attempt_id": str(uuid.uuid5(ENTITY_NAMESPACE, f"attempt:{attempt_digest}")),
        "mention_id": current_mention_id,
        "bank_version_id": bank_version_id,
        "resolver_version": RESOLVER_VERSION,
        "provider": provider_result.provider,
        "model_name": provider_result.model_name,
        "status": "completed",
        "candidate_snapshot": [
            candidate.model_dump(mode="json") for candidate in candidates
        ],
        "decision": decision.model_dump(mode="json"),
        "usage": {
            "input_tokens": provider_result.usage.input_tokens,
            "output_tokens": provider_result.usage.output_tokens,
        },
        "error": None,
    }
    return {
        "mention": mention_row,
        "attempt": attempt_row,
        "provisional_entity": provisional_entity,
        "provisional_alias": provisional_alias,
        "provider_usage": {
            "input_tokens": provider_result.usage.input_tokens,
            "output_tokens": provider_result.usage.output_tokens,
        },
    }


def ignored_mention(
    *,
    text: str,
    reason: str,
    mention: ExtractedMention,
    source: SourceReference,
    bank_version_id: str | None,
    observed_at: datetime,
) -> dict[str, Any]:
    current_mention_id = mention_id(source, mention)
    return {
        "mention_id": current_mention_id,
        "news_id": source.news_id,
        "polymarket_event_id": None,
        "polymarket_market_id": source.market_id,
        "kalshi_market_ticker": source.kalshi_market_ticker,
        "entity_id": None,
        "mention_text": text,
        "normalized_text": normalize_name(text),
        "entity_type_hint": mention.entity_type.value,
        "person_role_hint": (
            mention.person_role_hint.value if mention.person_role_hint else None
        ),
        "mention_role": mention.mention_role.value,
        "evidence": mention.evidence,
        "source_refs": mention.source_refs,
        "source_content_sha256": source.source_content_sha256,
        "extractor_version": EXTRACTOR_VERSION,
        "resolver_version": RESOLVER_VERSION,
        "resolution_status": ResolutionStatus.IGNORED.value,
        "match_method": None,
        "confidence": 1,
        "last_bank_version_id": bank_version_id,
        "candidate_entity_ids": [],
        "resolution_metadata": {"reason": reason, "terminal": True},
        "first_observed_at": observed_at,
        "last_observed_at": observed_at,
        "updated_at": observed_at,
    }


def serialize_audit_record(value: dict[str, Any]) -> str:
    def default(item: Any) -> Any:
        if isinstance(item, datetime):
            return item.astimezone(UTC).isoformat()
        raise TypeError(type(item).__name__)

    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=default)
