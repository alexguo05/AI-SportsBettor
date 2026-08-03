"""Deterministic normalization and provider-syntax guardrails."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any

from src.common.gcs import canonical_json_bytes
from src.entity_bank.models import ContractType

_PLACEHOLDER = re.compile(r"^(?:Player|Coach|Person|Team) [A-Z]{1,2}$")
_GENERIC_OPTIONS = {
    "another coach",
    "another player",
    "another team",
    "no listed player",
    "other",
    "someone else",
}


def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKC", value).casefold()
    text = text.replace("’", "'").replace("`", "'")
    text = re.sub(r"(?<=\b[A-Za-z])\.(?=[A-Za-z]\.?(?:\s|$))", "", text)
    text = re.sub(r"[^\w'\- ]+", " ", text)
    return " ".join(text.split())


def placeholder_reason(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if _PLACEHOLDER.fullmatch(text):
        return "polymarket_placeholder"
    if text.casefold() == "yes" or (text.casefold() == "no" and text != "NO"):
        return "generic_market_option"
    if normalize_name(text) in _GENERIC_OPTIONS:
        return "generic_market_option"
    return None


def infer_contract_type(
    sports_market_type: str | None,
    outcomes: list[str],
) -> ContractType:
    market_type = (sports_market_type or "").casefold()
    if market_type == "moneyline":
        return ContractType.MONEYLINE
    if market_type == "spreads":
        return ContractType.SPREAD
    if market_type == "totals":
        return ContractType.TOTAL
    if [outcome.casefold() for outcome in outcomes] == ["yes", "no"]:
        return ContractType.BINARY
    if len(outcomes) > 2:
        return ContractType.MULTI_CANDIDATE
    return ContractType.OTHER


def entity_input_projection(
    *,
    event_title: str,
    event_slug: str | None,
    market: dict[str, Any],
) -> dict[str, Any]:
    return {
        "event_title": event_title,
        "event_slug": event_slug,
        "market_question": market["question"],
        "market_slug": market.get("slug"),
        "group_item_title": market.get("group_item_title"),
        "outcomes": list(market.get("outcomes") or []),
        "sports_market_type": market.get("sports_market_type"),
    }


def entity_input_fingerprint(
    *,
    event_title: str,
    event_slug: str | None,
    market: dict[str, Any],
) -> str:
    projection = entity_input_projection(
        event_title=event_title,
        event_slug=event_slug,
        market=market,
    )
    return hashlib.sha256(canonical_json_bytes(projection)).hexdigest()
