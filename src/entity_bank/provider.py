"""Schema-constrained Claude provider and deterministic offline fixture."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from anthropic import Anthropic

from src.entity_bank.models import (
    AccuracySweepDecision,
    CandidateEntity,
    EntityType,
    ExtractedMention,
    MarketDisposition,
    MarketEventAnalysis,
    MarketTopic,
    MentionRole,
    PersonRoleHint,
    ResolutionDecision,
    ResolutionStatus,
)
from src.entity_bank.normalization import infer_contract_type, normalize_name, placeholder_reason
from src.entity_bank.prompt import (
    ACCURACY_SWEEP_SYSTEM_PROMPT,
    MARKET_SYSTEM_PROMPT,
    RESOLUTION_SYSTEM_PROMPT,
)


@dataclass(frozen=True)
class ProviderUsage:
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass(frozen=True)
class ProviderResult:
    output: Any
    usage: ProviderUsage
    provider: str
    model_name: str


class EntityProvider(Protocol):
    provider_name: str
    model_name: str

    def analyze_market_event(
        self,
        *,
        event_id: str,
        event_title: str,
        event_slug: str | None,
        markets: list[dict[str, Any]],
    ) -> ProviderResult: ...

    def adjudicate(
        self,
        *,
        mention: ExtractedMention,
        candidates: list[CandidateEntity],
        source_context: str,
        as_of: datetime,
    ) -> ProviderResult: ...

    def adjudicate_accuracy_sweep(
        self,
        *,
        mention: ExtractedMention,
        candidates: list[CandidateEntity],
        source_context: str,
        current_resolution: dict[str, Any],
        as_of: datetime,
        pass_number: int,
    ) -> ProviderResult: ...


class ClaudeEntityProvider:
    provider_name = "anthropic"

    def __init__(
        self,
        api_key: str,
        *,
        model_name: str,
        max_tokens: int = 8192,
        timeout_seconds: float = 90,
    ) -> None:
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is required")
        self.model_name = model_name
        self.max_tokens = max_tokens
        self.client = Anthropic(api_key=api_key, timeout=timeout_seconds, max_retries=2)

    def _parse(
        self,
        *,
        system: str,
        payload: dict[str, Any],
        output_format: type[Any],
    ) -> ProviderResult:
        response = self.client.messages.parse(
            model=self.model_name,
            max_tokens=self.max_tokens,
            system=system,
            messages=[
                {
                    "role": "user",
                    "content": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                }
            ],
            output_format=output_format,
        )
        if response.parsed_output is None:
            raise RuntimeError(f"Claude returned no structured output ({response.stop_reason})")
        return ProviderResult(
            output=response.parsed_output,
            usage=ProviderUsage(
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
            ),
            provider=self.provider_name,
            model_name=self.model_name,
        )

    def analyze_market_event(
        self,
        *,
        event_id: str,
        event_title: str,
        event_slug: str | None,
        markets: list[dict[str, Any]],
    ) -> ProviderResult:
        result = self._parse(
            system=MARKET_SYSTEM_PROMPT,
            payload={
                "event_id": event_id,
                "event_title": event_title,
                "event_slug": event_slug,
                "markets": markets,
            },
            output_format=MarketEventAnalysis,
        )
        expected = [str(market["market_id"]) for market in markets]
        actual = [market.market_id for market in result.output.markets]
        if sorted(expected) != sorted(actual) or len(actual) != len(expected):
            raise ValueError(
                f"market ID coverage mismatch: expected={expected!r}, actual={actual!r}"
            )
        if result.output.event_id != event_id:
            raise ValueError("event_id did not round-trip exactly")
        return result

    def adjudicate(
        self,
        *,
        mention: ExtractedMention,
        candidates: list[CandidateEntity],
        source_context: str,
        as_of: datetime,
    ) -> ProviderResult:
        result = self._parse(
            system=RESOLUTION_SYSTEM_PROMPT,
            payload={
                "as_of": as_of.isoformat(),
                "mention": mention.model_dump(mode="json"),
                "source_context": source_context,
                "allowed_candidates": [
                    candidate.model_dump(mode="json") for candidate in candidates
                ],
            },
            output_format=ResolutionDecision,
        )
        allowed = {candidate.entity_id for candidate in candidates}
        decision: ResolutionDecision = result.output
        if decision.entity_id is not None and decision.entity_id not in allowed:
            raise ValueError(f"resolver returned non-allowlisted entity ID {decision.entity_id}")
        if not set(decision.candidate_entity_ids).issubset(allowed):
            raise ValueError("resolver returned non-allowlisted candidate IDs")
        return result

    def adjudicate_accuracy_sweep(
        self,
        *,
        mention: ExtractedMention,
        candidates: list[CandidateEntity],
        source_context: str,
        current_resolution: dict[str, Any],
        as_of: datetime,
        pass_number: int,
    ) -> ProviderResult:
        result = self._parse(
            system=ACCURACY_SWEEP_SYSTEM_PROMPT,
            payload={
                "independent_pass": pass_number,
                "as_of": as_of.isoformat(),
                "mention": mention.model_dump(mode="json"),
                "source_context": source_context,
                "current_resolution": current_resolution,
                "allowed_candidates": [
                    candidate.model_dump(mode="json") for candidate in candidates
                ],
            },
            output_format=AccuracySweepDecision,
        )
        allowed = {candidate.entity_id for candidate in candidates}
        decision: AccuracySweepDecision = result.output
        if decision.entity_id is not None and decision.entity_id not in allowed:
            raise ValueError(f"sweep returned non-allowlisted entity ID {decision.entity_id}")
        if not set(decision.candidate_entity_ids).issubset(allowed):
            raise ValueError("sweep returned non-allowlisted candidate IDs")
        return result


class DeterministicEntityProvider:
    """Offline control-flow fixture; its semantic output is never persisted."""

    provider_name = "deterministic_dry_run"
    model_name = "entity-fixture-v1"

    def analyze_market_event(
        self,
        *,
        event_id: str,
        event_title: str,
        event_slug: str | None,
        markets: list[dict[str, Any]],
    ) -> ProviderResult:
        del event_slug
        lowered_event = event_title.casefold()
        output: list[MarketDisposition] = []
        for market in markets:
            question = str(market["question"]).casefold()
            sports_market_type = market.get("sports_market_type")
            outcomes = list(market.get("outcomes") or [])
            if sports_market_type in {"moneyline", "spreads", "totals"}:
                topic = MarketTopic.GAME
            elif "mvp" in lowered_event or "award" in question:
                topic = MarketTopic.AWARD
            elif "draft" in question:
                topic = MarketTopic.DRAFT
            elif "sign" in question or "team" in question:
                topic = MarketTopic.ROSTER_DESTINATION
            else:
                topic = MarketTopic.OTHER
            group_item = market.get("group_item_title")
            ignore_reason = placeholder_reason(group_item)
            output.append(
                MarketDisposition(
                    market_id=str(market["market_id"]),
                    market_topic=topic,
                    contract_type=infer_contract_type(sports_market_type, outcomes),
                    group_item_entity_type=(
                        EntityType.PERSON if group_item and not ignore_reason else None
                    ),
                    group_item_person_role_hint=(
                        PersonRoleHint.PLAYER if group_item and not ignore_reason else None
                    ),
                    group_item_mention_role=(
                        MentionRole.CANDIDATE if group_item and not ignore_reason else None
                    ),
                    standalone_mentions=[],
                    ignore_group_item=bool(ignore_reason),
                    ignore_reason=ignore_reason,
                    confidence=0.7,
                )
            )
        return ProviderResult(
            output=MarketEventAnalysis(event_id=event_id, markets=output),
            usage=ProviderUsage(),
            provider=self.provider_name,
            model_name=self.model_name,
        )

    def adjudicate(
        self,
        *,
        mention: ExtractedMention,
        candidates: list[CandidateEntity],
        source_context: str,
        as_of: datetime,
    ) -> ProviderResult:
        del source_context, as_of
        exact = [
            candidate
            for candidate in candidates
            if normalize_name(mention.text)
            in {normalize_name(candidate.canonical_name), *map(normalize_name, candidate.aliases)}
        ]
        if len(exact) == 1:
            decision = ResolutionDecision(
                status=ResolutionStatus.RESOLVED,
                entity_id=exact[0].entity_id,
                candidate_entity_ids=[exact[0].entity_id],
                confidence=1,
                reason="Unique exact alias in deterministic fixture.",
            )
        elif len(exact) > 1:
            decision = ResolutionDecision(
                status=ResolutionStatus.AMBIGUOUS,
                candidate_entity_ids=[candidate.entity_id for candidate in exact],
                confidence=0,
                reason="Exact alias maps to multiple entities.",
            )
        else:
            decision = ResolutionDecision(
                status=ResolutionStatus.UNRESOLVED,
                candidate_entity_ids=[candidate.entity_id for candidate in candidates],
                confidence=0,
                reason="No unique exact alias in deterministic fixture.",
            )
        return ProviderResult(
            output=decision,
            usage=ProviderUsage(),
            provider=self.provider_name,
            model_name=self.model_name,
        )

    def adjudicate_accuracy_sweep(
        self,
        *,
        mention: ExtractedMention,
        candidates: list[CandidateEntity],
        source_context: str,
        current_resolution: dict[str, Any],
        as_of: datetime,
        pass_number: int,
    ) -> ProviderResult:
        del pass_number
        base = self.adjudicate(
            mention=mention,
            candidates=candidates,
            source_context=source_context,
            as_of=as_of,
        )
        decision: ResolutionDecision = base.output
        same = (
            decision.status.value == current_resolution.get("resolution_status")
            and decision.entity_id == current_resolution.get("entity_id")
        )
        output = AccuracySweepDecision(
            **decision.model_dump(),
            current_decision_assessment="confirmed" if same else "change",
            evidence_quote=mention.evidence,
            risk_flags=[] if same else ["deterministic_fixture_disagreement"],
        )
        return ProviderResult(
            output=output,
            usage=base.usage,
            provider=base.provider,
            model_name=base.model_name,
        )
