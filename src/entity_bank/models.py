"""Closed taxonomies and validated entity extraction contracts."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class EntityType(StrEnum):
    TEAM = "team"
    PERSON = "person"


class PersonRoleHint(StrEnum):
    PLAYER = "player"
    COACH = "coach"
    PROSPECT = "prospect"
    OWNER = "owner"
    EXECUTIVE = "executive"
    OFFICIAL = "official"
    AGENT = "agent"
    MEDIA = "media"
    OTHER = "other"
    UNKNOWN = "unknown"


class MentionRole(StrEnum):
    SUBJECT = "subject"
    CANDIDATE = "candidate"
    COMPETITOR = "competitor"
    DESTINATION = "destination"
    AFFILIATED_TEAM = "affiliated_team"
    AFFECTED_TEAM = "affected_team"
    COUNTERPARTY = "counterparty"
    REFERENCED = "referenced"
    UNKNOWN = "unknown"


class MarketTopic(StrEnum):
    GAME = "game"
    SEASON_SERIES = "season_series"
    CHAMPIONSHIP = "championship"
    POSTSEASON_QUALIFICATION = "postseason_qualification"
    DIVISION_OR_CONFERENCE = "division_or_conference"
    AWARD = "award"
    STAT_LEADER = "stat_leader"
    DRAFT = "draft"
    ROSTER_DESTINATION = "roster_destination"
    STARTING_ROLE = "starting_role"
    TRANSACTION = "transaction"
    RETIREMENT = "retirement"
    COACHING_STATUS = "coaching_status"
    INJURY_AVAILABILITY = "injury_availability"
    CONTRACT = "contract"
    POLICY_RULE = "policy_rule"
    LABOR = "labor"
    RELOCATION = "relocation"
    ENTERTAINMENT = "entertainment"
    OTHER = "other"


class ContractType(StrEnum):
    BINARY = "binary"
    MONEYLINE = "moneyline"
    SPREAD = "spread"
    TOTAL = "total"
    MULTI_CANDIDATE = "multi_candidate"
    OTHER = "other"


class ResolutionStatus(StrEnum):
    RESOLVED = "resolved"
    AMBIGUOUS = "ambiguous"
    UNRESOLVED = "unresolved"
    IGNORED = "ignored"


class IdentityStatus(StrEnum):
    CANONICAL = "canonical"
    PROVISIONAL = "provisional"
    MERGED = "merged"
    REJECTED = "rejected"


class MatchMethod(StrEnum):
    PROVIDER_ID = "provider_id"
    EXACT_ALIAS = "exact_alias"
    NORMALIZED_ALIAS = "normalized_alias"
    CONTEXT_ADJUDICATED = "context_adjudicated"
    MANUAL = "manual"
    PROVISIONAL_CREATION = "provisional_creation"


class AliasType(StrEnum):
    CANONICAL_NAME = "canonical_name"
    PROVIDER_NAME = "provider_name"
    FULL_NAME = "full_name"
    FOOTBALL_NAME = "football_name"
    ABBREVIATION = "abbreviation"
    NICKNAME = "nickname"
    INITIALS = "initials"
    FORMER_NAME = "former_name"
    MANUAL = "manual"


class ExtractedMention(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=500)
    entity_type: EntityType
    person_role_hint: PersonRoleHint | None = None
    mention_role: MentionRole
    evidence: str = Field(min_length=1, max_length=2_000)
    confidence: float = Field(ge=0, le=1)
    source_refs: list[str] = Field(default_factory=list, max_length=12)

    @model_validator(mode="after")
    def validate_person_role(self) -> ExtractedMention:
        if self.entity_type == EntityType.TEAM and self.person_role_hint is not None:
            raise ValueError("team mentions cannot have person_role_hint")
        return self


class MarketDisposition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market_id: str
    market_topic: MarketTopic
    contract_type: ContractType
    group_item_entity_type: EntityType | None = None
    group_item_person_role_hint: PersonRoleHint | None = None
    group_item_mention_role: MentionRole | None = None
    standalone_mentions: list[ExtractedMention] = Field(default_factory=list, max_length=12)
    ignore_group_item: bool = False
    ignore_reason: str | None = Field(default=None, max_length=1_000)
    confidence: float = Field(ge=0, le=1)


class MarketEventAnalysis(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    markets: list[MarketDisposition] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_market_ids(self) -> MarketEventAnalysis:
        market_ids = [market.market_id for market in self.markets]
        if len(market_ids) != len(set(market_ids)):
            raise ValueError("market IDs must be unique")
        return self


class CandidateEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entity_id: str
    canonical_name: str
    entity_type: EntityType
    identity_status: IdentityStatus
    aliases: list[str] = Field(default_factory=list)
    roles: list[str] = Field(default_factory=list)
    teams: list[str] = Field(default_factory=list)
    lexical_score: float = Field(ge=0, le=1)


class ResolutionDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: ResolutionStatus
    entity_id: str | None = None
    candidate_entity_ids: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1)
    reason: str = Field(min_length=1, max_length=4_000)

    @model_validator(mode="after")
    def validate_decision(self) -> ResolutionDecision:
        if self.status == ResolutionStatus.RESOLVED and self.entity_id is None:
            raise ValueError("resolved decisions require entity_id")
        if self.status != ResolutionStatus.RESOLVED and self.entity_id is not None:
            raise ValueError("only resolved decisions may include entity_id")
        return self


class AccuracySweepDecision(ResolutionDecision):
    """Independent high-accuracy assessment of an existing resolution."""

    current_decision_assessment: Literal["confirmed", "change", "insufficient"]
    evidence_quote: str = Field(min_length=1, max_length=2_000)
    risk_flags: list[str] = Field(default_factory=list, max_length=12)
