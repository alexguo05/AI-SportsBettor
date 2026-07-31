"""Anthropic and deterministic dry-run enrichment providers."""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from typing import Protocol

from anthropic import Anthropic

from src.enrich_news.config import DEFAULT_MAX_OUTPUT_TOKENS, DEFAULT_MODEL_NAME
from src.enrich_news.models import (
    EnrichmentOutput,
    ExtractedClaim,
    InformationStatus,
    ProviderUsage,
    TagAssignment,
    TagCertainty,
    TopicTag,
    Usefulness,
)
from src.enrich_news.prompt import SYSTEM_PROMPT, build_user_prompt
from src.enrich_news.sources import CollectedEvidence


@dataclass(frozen=True)
class ProviderResponse:
    output: EnrichmentOutput
    usage: ProviderUsage
    model_name: str


class EnrichmentProvider(Protocol):
    provider_name: str
    model_name: str

    def enrich(self, evidence: CollectedEvidence) -> ProviderResponse: ...


class ClaudeProvider:
    provider_name = "anthropic"

    def __init__(
        self,
        api_key: str,
        *,
        model_name: str = DEFAULT_MODEL_NAME,
        max_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
        timeout_seconds: float = 90.0,
    ) -> None:
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is required for the Claude provider")
        self.model_name = model_name
        self.max_tokens = max_tokens
        self.client = Anthropic(
            api_key=api_key,
            timeout=timeout_seconds,
            max_retries=2,
        )

    def enrich(self, evidence: CollectedEvidence) -> ProviderResponse:
        content: list[dict[str, object]] = []
        for image in evidence.images[:20]:
            content.append(
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": image.media_type,
                        "data": base64.b64encode(image.data).decode("ascii"),
                    },
                }
            )
            content.append(
                {
                    "type": "text",
                    "text": f"The preceding image has source reference [{image.source_ref}].",
                }
            )
        content.append(
            {
                "type": "text",
                "text": build_user_prompt(evidence.as_prompt_text(), evidence.source_refs()),
            }
        )
        response = self.client.messages.parse(
            model=self.model_name,
            max_tokens=self.max_tokens,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": content}],
            output_format=EnrichmentOutput,
        )
        if response.parsed_output is None:
            raise RuntimeError(f"Claude returned no structured output ({response.stop_reason})")
        return ProviderResponse(
            output=response.parsed_output,
            usage=ProviderUsage(
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
                cache_creation_input_tokens=(response.usage.cache_creation_input_tokens or 0),
                cache_read_input_tokens=response.usage.cache_read_input_tokens or 0,
            ),
            model_name=self.model_name,
        )


_TAG_KEYWORDS: tuple[tuple[TopicTag, tuple[str, ...]], ...] = (
    (
        TopicTag.INJURY_AVAILABILITY,
        (
            "injury",
            "injured",
            "questionable",
            "doubtful",
            "ruled out",
            "did not practice",
            "limited practice",
            "left practice",
            "game-time decision",
        ),
    ),
    (
        TopicTag.LINEUP_DEPTH_CHART,
        ("depth chart", "starter", "starting", "benched", "backup", "snap count", "role"),
    ),
    (
        TopicTag.ROSTER_TRANSACTION,
        ("trade", "traded", "acquired", "signed", "released", "waived", "activated"),
    ),
    (
        TopicTag.COACHING_MANAGEMENT,
        ("head coach", "offensive coordinator", "defensive coordinator", "fired", "hired"),
    ),
    (
        TopicTag.CONTRACT,
        ("contract", "extension", "holdout", "salary", "guaranteed"),
    ),
    (
        TopicTag.DISCIPLINE_LEGAL,
        ("suspended", "suspension", "arrested", "investigation", "fine", "lawsuit"),
    ),
    (
        TopicTag.WEATHER_FIELD_CONDITIONS,
        ("weather", "rain", "snow", "wind", "roof", "field conditions", "turf"),
    ),
    (
        TopicTag.SCHEDULE_TRAVEL,
        ("rescheduled", "schedule", "travel", "flight", "short week", "bye week"),
    ),
    (
        TopicTag.GAME_STATUS_RESULT,
        ("final score", "final:", "postponed", "cancelled", "halftime", "overtime"),
    ),
    (
        TopicTag.PERFORMANCE_STATISTICS,
        ("yards", "touchdowns", "interceptions", "record", "completion percentage"),
    ),
    (
        TopicTag.MARKET_ODDS,
        ("betting odds", "betting line", "point spread", "moneyline", "line movement"),
    ),
    (
        TopicTag.LEAGUE_MANAGEMENT_RULE,
        ("rule change", "league policy", "officiating", "competition committee"),
    ),
)
_PROMOTIONAL_KEYWORDS = ("subscribe", "giveaway", "merch", "tickets on sale", "happy birthday")


def _contains_keyword(text: str, keyword: str) -> bool:
    return bool(re.search(rf"(?<!\w){re.escape(keyword)}(?!\w)", text))


class DeterministicDryRunProvider:
    """Offline control-flow test double; its output is never presented as model output."""

    provider_name = "deterministic_dry_run"
    model_name = "keyword-fixture-v1"

    def enrich(self, evidence: CollectedEvidence) -> ProviderResponse:
        text = evidence.as_prompt_text()
        lowered = text.lower()
        matched = [
            topic
            for topic, keywords in _TAG_KEYWORDS
            if any(_contains_keyword(lowered, keyword) for keyword in keywords)
        ]
        if not matched and any(
            _contains_keyword(lowered, keyword) for keyword in _PROMOTIONAL_KEYWORDS
        ):
            matched = [TopicTag.PROMOTIONAL_SOCIAL]
        if not matched:
            matched = [TopicTag.UNRELATED_OTHER]

        high_usefulness_tags = {
            TopicTag.INJURY_AVAILABILITY,
            TopicTag.LINEUP_DEPTH_CHART,
            TopicTag.ROSTER_TRANSACTION,
            TopicTag.COACHING_MANAGEMENT,
            TopicTag.DISCIPLINE_LEGAL,
            TopicTag.WEATHER_FIELD_CONDITIONS,
            TopicTag.SCHEDULE_TRAVEL,
        }
        medium_usefulness_tags = {
            TopicTag.CONTRACT,
            TopicTag.GAME_STATUS_RESULT,
            TopicTag.PERFORMANCE_STATISTICS,
            TopicTag.MARKET_ODDS,
            TopicTag.LEAGUE_MANAGEMENT_RULE,
        }
        if any(tag in high_usefulness_tags for tag in matched):
            usefulness = Usefulness.HIGH
        elif any(tag in medium_usefulness_tags for tag in matched):
            usefulness = Usefulness.MEDIUM
        else:
            usefulness = Usefulness.IRRELEVANT

        if any(value in lowered for value in ("rumor", "could be", "might be")):
            information_status = InformationStatus.RUMOR
        elif any(value in lowered for value in ("i think", "in my opinion", "analysis:")):
            information_status = InformationStatus.OPINION
        elif any(value in lowered for value in ("official", "team announced", "we have signed")):
            information_status = InformationStatus.OFFICIAL
        else:
            information_status = InformationStatus.REPORTED

        tweet_match = re.search(r"Tweet text:\s*(.+)", text)
        summary_source = tweet_match.group(1).strip() if tweet_match else text.strip()
        summary = summary_source[:300] or "No usable source text."
        output = EnrichmentOutput(
            tags=[
                TagAssignment(
                    tag=tag,
                    certainty=TagCertainty.NEUTRAL,
                    source_refs=["tweet"],
                )
                for tag in matched
            ],
            information_status=information_status,
            usefulness=usefulness,
            summary=summary,
            classification_reason=(
                "Deterministic keyword fixture used for an offline pipeline dry run; "
                "this is not an AI quality judgment."
            ),
            entities=[],
            claims=[
                ExtractedClaim(
                    statement=summary,
                    confidence=0.65,
                    source_refs=["tweet"],
                )
            ],
        )
        return ProviderResponse(
            output=output,
            usage=ProviderUsage(),
            model_name=self.model_name,
        )
