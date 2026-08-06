from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any

import pytest

from src.enrich_news.prompt import ENTITY_EXTRACTOR_VERSION
from src.entity_bank.accuracy_sweep import run_sweep_records
from src.entity_bank.gamma_backfill import main as gamma_backfill_main
from src.entity_bank.models import (
    CandidateEntity,
    ContractType,
    EntityType,
    ExtractedMention,
    IdentityStatus,
    MarketEventAnalysis,
    MarketTopic,
    MentionRole,
    PersonRoleHint,
    ResolutionDecision,
    ResolutionStatus,
)
from src.entity_bank.nflverse_pipeline import (
    NflverseClient,
    SourceAsset,
    normalize_snapshot,
)
from src.entity_bank.nflverse_poll import inferred_nfl_season, run_cycle
from src.entity_bank.nflverse_sync import main as nflverse_sync_main
from src.entity_bank.normalization import (
    entity_input_fingerprint,
    normalize_name,
    placeholder_reason,
)
from src.entity_bank.prompt import EXTRACTOR_VERSION, MARKET_SYSTEM_PROMPT
from src.entity_bank.provider import (
    ClaudeEntityProvider,
    DeterministicEntityProvider,
    ProviderResult,
    ProviderUsage,
)
from src.entity_bank.resolver import CandidateIndex, SourceReference, resolve_mention
from src.entity_bank.worker import (
    Batch,
    process_market_events,
    process_news,
    process_pending_mentions,
)
from src.entity_bank.worker import (
    main as worker_main,
)

NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)


class FakeHttpResponse:
    def __init__(self, *, payload: Any = None, content: bytes = b"") -> None:
        self._payload = payload
        self.content = content
        self.headers = {"ETag": '"fixture"'}

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class FakeNflverseSession:
    def __init__(self, responses: dict[str, FakeHttpResponse]) -> None:
        self.responses = responses

    def get(self, url: str, **_kwargs: Any) -> FakeHttpResponse:
        return self.responses[url]


class FakeSnapshotClient:
    def __init__(self, assets: tuple[SourceAsset, ...]) -> None:
        self.assets = assets

    def fetch(self, _season: int) -> tuple[SourceAsset, ...]:
        return self.assets


class FakePollRepository:
    def __init__(self, latest_hash: str | None = None) -> None:
        self.latest_hash = latest_hash
        self.envelopes: list[dict[str, Any]] = []

    def latest_content_sha256(self, source: str = "nflverse") -> str | None:
        assert source == "nflverse"
        return self.latest_hash

    def persist_nflverse_snapshot(
        self,
        envelope: dict[str, Any],
    ) -> dict[str, int | str]:
        self.envelopes.append(envelope)
        self.latest_hash = envelope["content_sha256"]
        return {"version_id": envelope["content_sha256"][:32], "entities": 1}


class FakePollBlob:
    def __init__(self) -> None:
        self.metadata: dict[str, str] = {}
        self.content_encoding: str | None = None
        self.data: bytes | None = None

    def upload_from_string(self, data: bytes, **_kwargs: Any) -> None:
        self.data = data


class FakePollBucket:
    def __init__(self) -> None:
        self.paths: list[str] = []
        self.blobs: list[FakePollBlob] = []

    def blob(self, path: str) -> FakePollBlob:
        self.paths.append(path)
        blob = FakePollBlob()
        self.blobs.append(blob)
        return blob


def asset(name: str, csv_text: str) -> SourceAsset:
    return SourceAsset(
        name=name,
        url=f"https://example.test/{name}.csv",
        content=csv_text.encode(),
        etag=None,
        sha256=name * 8,
    )


def test_nflverse_asset_discovery_verifies_provider_digest() -> None:
    content = b"a,b\n1,2\n"
    digest = hashlib.sha256(content).hexdigest()
    api_url = "https://api.example.test/repo"
    download_url = "https://download.example.test/teams.csv"
    client = NflverseClient(
        session=FakeNflverseSession(
            {
                f"{api_url}/releases/tags/teams": FakeHttpResponse(
                    payload={
                        "assets": [
                            {
                                "id": 123,
                                "name": "teams.csv",
                                "browser_download_url": download_url,
                                "digest": f"sha256:{digest}",
                                "updated_at": "2026-07-31T00:00:00Z",
                            }
                        ]
                    }
                ),
                download_url: FakeHttpResponse(content=content),
            }
        ),
        api_url=api_url,
    )

    discovered = client._download("teams", "teams", "teams.csv")

    assert discovered.asset_id == 123
    assert discovered.sha256 == digest
    assert discovered.provider_digest == f"sha256:{digest}"


def test_nflverse_asset_discovery_rejects_digest_mismatch() -> None:
    api_url = "https://api.example.test/repo"
    download_url = "https://download.example.test/teams.csv"
    client = NflverseClient(
        session=FakeNflverseSession(
            {
                f"{api_url}/releases/tags/teams": FakeHttpResponse(
                    payload={
                        "assets": [
                            {
                                "id": 123,
                                "name": "teams.csv",
                                "browser_download_url": download_url,
                                "digest": f"sha256:{'0' * 64}",
                            }
                        ]
                    }
                ),
                download_url: FakeHttpResponse(content=b"different"),
            }
        ),
        api_url=api_url,
    )

    with pytest.raises(RuntimeError, match="digest mismatch"):
        client._download("teams", "teams", "teams.csv")


def test_name_normalization_and_placeholder_suppression_are_conservative() -> None:
    assert normalize_name("  D.J. Moore Jr. ") == "dj moore jr"
    assert normalize_name("Brian O’Neill") == "brian o'neill"
    assert placeholder_reason("Player A") == "polymarket_placeholder"
    assert placeholder_reason("Team A") == "polymarket_placeholder"
    assert placeholder_reason("Team AB") == "polymarket_placeholder"
    assert placeholder_reason("another player") == "generic_market_option"
    assert placeholder_reason("Yes") == "generic_market_option"
    assert placeholder_reason("No") == "generic_market_option"
    assert placeholder_reason("NO") is None
    assert placeholder_reason("A.J. Brown") is None
    assert '"Team D"' in MARKET_SYSTEM_PROMPT
    decision_schema = ResolutionDecision.model_json_schema()["properties"]
    assert decision_schema["reason"]["maxLength"] == 4_000


def test_entity_fingerprint_only_changes_for_entity_relevant_inputs() -> None:
    market = {
        "question": "Will Josh Allen win MVP?",
        "slug": "josh-allen-mvp",
        "group_item_title": "Josh Allen",
        "outcomes": ["Yes", "No"],
        "sports_market_type": None,
        "active": True,
    }
    first = entity_input_fingerprint(
        event_title="NFL MVP",
        event_slug="nfl-mvp",
        market=market,
    )
    changed_lifecycle = {**market, "active": False, "price": "0.42"}
    assert first == entity_input_fingerprint(
        event_title="NFL MVP",
        event_slug="nfl-mvp",
        market=changed_lifecycle,
    )
    assert first != entity_input_fingerprint(
        event_title="NFL MVP",
        event_slug="nfl-mvp",
        market={**market, "group_item_title": "Lamar Jackson"},
    )


def test_nflverse_snapshot_merges_franchise_aliases_and_preserves_duplicate_names() -> None:
    assets = (
        asset(
            "teams",
            "team_abbr,team_name,team_id,team_nick,team_conf,team_division\n"
            "LAR,Los Angeles Rams,2510,Rams,NFC,NFC West\n"
            "STL,St. Louis Rams,2510,Rams,NFC,NFC West\n"
            "BUF,Buffalo Bills,0610,Bills,AFC,AFC East\n",
        ),
        asset(
            "players",
            "gsis_id,display_name,football_name,short_name,esb_id,nfl_id,espn_id,"
            "pfr_id,pff_id,smart_id,position,position_group,rookie_season,last_season\n"
            "00-1,John Smith,John,J.Smith,,,,,,,,QB,QB,2020,2026\n"
            "00-2,John Smith,Johnny,J.Smith,,,,,,,,WR,WR,2024,2026\n"
            "99-9,Retired Player,Retired,R.Player,,,,,,smart-retired,QB,QB,1980,1990\n",
        ),
        asset(
            "rosters",
            "season,team,position,status,full_name,gsis_id,week,game_type,"
            "jersey_number\n"
            "2026,LAR,QB,ACT,John Smith,00-1,1,REG,12\n"
            "2026,BUF,WR,ACT,John Smith,00-2,1,REG,80\n",
        ),
    )
    snapshot = normalize_snapshot(assets, season=2026, observed_at=NOW)
    team_entities = [entity for entity in snapshot.entities if entity["entity_type"] == "team"]
    people = [entity for entity in snapshot.entities if entity["entity_type"] == "person"]

    assert len(team_entities) == 2
    rams = next(
        entity for entity in team_entities if entity["canonical_name"] == "Los Angeles Rams"
    )
    assert {"LAR", "STL", "St. Louis Rams"}.issubset({alias["alias"] for alias in rams["aliases"]})
    assert len(people) == 2
    assert people[0]["entity_id"] != people[1]["entity_id"]
    assert {entity["canonical_name"] for entity in snapshot.complete_player_history} == {
        "John Smith",
        "Retired Player",
    }
    assert len(snapshot.complete_player_history) == 3
    assert len(snapshot.relationships) == 2


def test_nflverse_merges_consistent_duplicates_and_quarantines_mixed_identities() -> None:
    assets = (
        asset(
            "teams",
            "team_abbr,team_name,team_id,team_nick,team_conf,team_division\n"
            "CLE,Cleveland Browns,1050,Browns,AFC,AFC North\n",
        ),
        asset(
            "players",
            "gsis_id,display_name,football_name,short_name,esb_id,nfl_id,espn_id,"
            "pfr_id,pff_id,smart_id,birth_date,position,position_group,"
            "rookie_season,last_season\n"
            "BAR591037,Dante Barnett,Dante,,BAR591037,,,,,smart-dante,"
            "2000-01-01,DL,DL,2025,2026\n"
            "00-0040784,Quinshon Judkins,Quinshon,,JUD359919,,,,,smart-quinshon,"
            "2003-10-29,RB,RB,2025,2026\n"
            "PRY456541,Layne Pryor,Layne,,PRY456541,,,,,smart-layne,"
            "2002-01-01,TE,TE,2025,2025\n"
            "00-0040792,Layne Pryor,Layne,,PRY456541,,,,,smart-layne,"
            "2002-01-01,TE,TE,2026,2026\n",
        ),
        asset(
            "rosters",
            "season,team,position,status,full_name,football_name,gsis_id,esb_id,"
            "smart_id,week,game_type,jersey_number\n"
            "2026,CLE,DL,ACT,Quinshon Judkins,Dante,00-0040789,BAR591037,"
            "smart-quinshon,1,REG,9\n"
            "2026,CLE,TE,ACT,Layne Pryor Jr.,Layne,00-0040792,PRY456541,"
            "smart-layne,1,REG,80\n"
            "2026,CLE,WR,ACT,Unknown Person,Unknown,,,,1,REG,81\n",
        ),
    )

    snapshot = normalize_snapshot(assets, season=2026, observed_at=NOW)
    people = [entity for entity in snapshot.entities if entity["entity_type"] == "person"]
    complete_history = list(snapshot.complete_player_history)
    layne = [entity for entity in people if entity["canonical_name"] == "Layne Pryor"]
    mapping_owners: dict[tuple[str, str], set[str]] = {}
    for entity in complete_history:
        for mapping in entity["source_mappings"]:
            key = (mapping["provider"], mapping["source_entity_id"])
            mapping_owners.setdefault(key, set()).add(entity["entity_id"])

    assert len(people) == 1
    assert len(complete_history) == 3
    assert {entity["canonical_name"] for entity in complete_history} == {
        "Dante Barnett",
        "Layne Pryor",
        "Quinshon Judkins",
    }
    assert len(layne) == 1
    assert {
        mapping["source_entity_id"]
        for mapping in layne[0]["source_mappings"]
        if mapping["provider"] == "gsis"
    } == {"PRY456541", "00-0040792"}
    assert all(len(owners) == 1 for owners in mapping_owners.values())
    assert len(snapshot.relationships) == 1
    assert "Layne Pryor Jr." in {alias["alias"] for alias in layne[0]["aliases"]}
    assert snapshot.quality["merged_player_rows"] == 1
    assert snapshot.quality["unsafe_source_mapping_collisions"] == 0
    assert {
        reason for record in snapshot.quarantined_records for reason in record["reason_codes"]
    } == {
        "identifiers_resolve_to_multiple_people",
        "missing_stable_person_identifier",
    }
    assert len(snapshot.source_mapping_conflicts) == 1


def poll_assets() -> tuple[SourceAsset, ...]:
    return (
        asset(
            "teams",
            "team_abbr,team_name,team_id,team_nick,team_conf,team_division\n"
            "BUF,Buffalo Bills,0610,Bills,AFC,AFC East\n",
        ),
        asset(
            "players",
            "gsis_id,display_name,football_name,short_name,esb_id,nfl_id,espn_id,"
            "pfr_id,pff_id,smart_id,birth_date,position,position_group,"
            "rookie_season,last_season\n"
            "00-1,Josh Allen,Josh,J.Allen,ALL123,,,,,smart-josh,1996-05-21,"
            "QB,QB,2018,2026\n",
        ),
        asset(
            "rosters",
            "season,team,position,status,full_name,football_name,gsis_id,esb_id,"
            "smart_id,week,game_type,jersey_number\n"
            "2026,BUF,QB,ACT,Josh Allen,Josh,00-1,ALL123,smart-josh,1,REG,17\n",
        ),
    )


def test_nflverse_poll_skips_all_storage_for_unchanged_snapshot() -> None:
    assets = poll_assets()
    snapshot = normalize_snapshot(assets, season=2026, observed_at=NOW)
    repository = FakePollRepository(snapshot.content_sha256)
    bucket_created = False

    def bucket_factory() -> FakePollBucket:
        nonlocal bucket_created
        bucket_created = True
        return FakePollBucket()

    report = run_cycle(
        client=FakeSnapshotClient(assets),  # type: ignore[arg-type]
        repository=repository,
        bucket_factory=bucket_factory,
        bucket_name="bucket",
        season=2026,
        now=NOW,
    )

    assert report["skipped_unchanged_snapshot"]
    assert not report["gcs_writes"]
    assert not report["database_writes"]
    assert not bucket_created
    assert repository.envelopes == []


def test_nflverse_poll_archives_then_persists_changed_snapshot() -> None:
    assets = poll_assets()
    repository = FakePollRepository()
    bucket = FakePollBucket()

    report = run_cycle(
        client=FakeSnapshotClient(assets),  # type: ignore[arg-type]
        repository=repository,
        bucket_factory=lambda: bucket,
        bucket_name="bucket",
        season=2026,
        now=NOW,
        run_id_factory=lambda: "a" * 32,
    )

    assert report["gcs_writes"]
    assert report["database_writes"]
    assert not report["skipped_unchanged_snapshot"]
    assert len(bucket.blobs) == 1
    assert bucket.blobs[0].data is not None
    assert len(repository.envelopes) == 1
    assert report["storage_uri"].startswith("gs://bucket/raw/provider=nflverse/")


def test_nflverse_poll_infers_season_from_league_year_boundary() -> None:
    assert inferred_nfl_season(datetime(2027, 1, 15, tzinfo=UTC)) == 2026
    assert inferred_nfl_season(datetime(2027, 3, 15, tzinfo=UTC)) == 2027


def candidate_row(entity_id: str, name: str, *, team: str = "Buffalo Bills") -> dict[str, Any]:
    return {
        "entity_id": entity_id,
        "canonical_name": name,
        "entity_type": "person",
        "identity_status": "canonical",
        "aliases": [name],
        "roles": ["player"],
        "teams": [team],
    }


def person_mention(name: str) -> ExtractedMention:
    return ExtractedMention(
        text=name,
        entity_type=EntityType.PERSON,
        person_role_hint=PersonRoleHint.PLAYER,
        mention_role=MentionRole.CANDIDATE,
        evidence=name,
        confidence=0.9,
    )


def test_news_resolution_consumes_persisted_mentions_without_extraction_call() -> None:
    batch = Batch()
    records = [
        {
            "news_id": "news-rich",
            "text": "Josh Allen practiced in Buffalo.",
            "summary": "Josh Allen practiced.",
            "input_fingerprint": "a" * 64,
            "enrichment_version": "v3",
            "entities": [
                {
                    "name": "Josh Allen",
                    "entity_type": "player",
                    "mention_role": "subject",
                    "evidence": "Josh Allen practiced in Buffalo.",
                    "confidence": 0.95,
                    "source_refs": ["tweet"],
                },
                {
                    "name": "Buffalo",
                    "entity_type": "location",
                    "mention_role": "referenced",
                    "evidence": "Buffalo",
                    "confidence": 0.7,
                    "source_refs": ["tweet"],
                },
            ],
        },
        {
            "news_id": "news-empty",
            "text": "No resolvable NFL entity.",
            "summary": "No material entity.",
            "input_fingerprint": "b" * 64,
            "enrichment_version": "v3",
            "entities": [],
        },
    ]

    process_news(
        records=records,
        provider=DeterministicEntityProvider(),
        index=CandidateIndex([candidate_row("entity-1", "Josh Allen")]),
        bank_version_id="bank-1",
        batch=batch,
        observed_at=NOW,
    )

    assert len(batch.mentions) == 1
    mention = next(iter(batch.mentions.values()))
    assert mention["entity_id"] == "entity-1"
    assert mention["extractor_version"] == ENTITY_EXTRACTOR_VERSION
    assert mention["source_refs"] == ["tweet"]
    assert len(batch.news_resolution_runs) == 2
    assert {row["mention_count"] for row in batch.news_resolution_runs.values()} == {0, 1}
    assert all(row["status"] == "completed" for row in batch.news_resolution_runs.values())
    assert batch.input_tokens == 0


def source(market_id: str = "market-1") -> SourceReference:
    return SourceReference(
        source_kind="polymarket_market",
        source_id=market_id,
        source_content_sha256="a" * 64,
        market_id=market_id,
    )


def test_unique_alias_resolves_without_model_memory_and_is_idempotent() -> None:
    index = CandidateIndex([candidate_row("entity-1", "Josh Allen")])
    kwargs = {
        "mention": person_mention("Josh Allen"),
        "source": source(),
        "source_context": "Will Josh Allen win NFL MVP?",
        "index": index,
        "provider": DeterministicEntityProvider(),
        "bank_version_id": "bank-1",
        "observed_at": NOW,
        "allow_provisional": True,
    }
    first = resolve_mention(**kwargs)
    second = resolve_mention(**kwargs)

    assert first["mention"]["entity_id"] == "entity-1"
    assert first["mention"]["match_method"] == "exact_alias"
    assert first["mention"]["mention_id"] == second["mention"]["mention_id"]
    assert first["attempt"]["attempt_id"] == second["attempt"]["attempt_id"]


def test_short_team_alias_requires_exact_uppercase_or_context_adjudication() -> None:
    class UnresolvedAdjudicator(DeterministicEntityProvider):
        def __init__(self) -> None:
            self.calls = 0

        def adjudicate(self, **_kwargs: Any) -> ProviderResult:
            self.calls += 1
            return ProviderResult(
                output=ResolutionDecision(
                    status=ResolutionStatus.UNRESOLVED,
                    candidate_entity_ids=["team-no"],
                    confidence=0,
                    reason="Surface form is not an uppercase team abbreviation.",
                ),
                usage=ProviderUsage(),
                provider=self.provider_name,
                model_name=self.model_name,
            )

    index = CandidateIndex(
        [
            {
                "entity_id": "team-no",
                "canonical_name": "New Orleans Saints",
                "entity_type": "team",
                "identity_status": "canonical",
                "aliases": ["New Orleans Saints", "Saints", "NO"],
                "roles": [],
                "teams": [],
            }
        ]
    )
    provider = UnresolvedAdjudicator()

    def team_mention(text: str) -> ExtractedMention:
        return ExtractedMention(
            text=text,
            entity_type=EntityType.TEAM,
            mention_role=MentionRole.COMPETITOR,
            evidence=text,
            confidence=1,
            source_refs=["outcomes"],
        )

    title_case = resolve_mention(
        mention=team_mention("No"),
        source=source(),
        source_context="Yes | No",
        index=index,
        provider=provider,
        bank_version_id="bank-1",
        observed_at=NOW,
        allow_provisional=False,
    )
    uppercase = resolve_mention(
        mention=team_mention("NO"),
        source=source("market-uppercase"),
        source_context="NO | ATL",
        index=index,
        provider=provider,
        bank_version_id="bank-1",
        observed_at=NOW,
        allow_provisional=False,
    )

    assert title_case["mention"]["resolution_status"] == "unresolved"
    assert title_case["mention"]["match_method"] is None
    assert uppercase["mention"]["entity_id"] == "team-no"
    assert uppercase["mention"]["match_method"] == "exact_alias"
    assert provider.calls == 1


def test_same_name_collision_stays_ambiguous() -> None:
    index = CandidateIndex(
        [
            candidate_row("entity-1", "John Smith", team="Buffalo Bills"),
            candidate_row("entity-2", "John Smith", team="Los Angeles Rams"),
        ]
    )
    result = resolve_mention(
        mention=person_mention("John Smith"),
        source=source(),
        source_context="Will John Smith lead the NFL?",
        index=index,
        provider=DeterministicEntityProvider(),
        bank_version_id="bank-1",
        observed_at=NOW,
        allow_provisional=True,
    )

    assert result["mention"]["resolution_status"] == "ambiguous"
    assert set(result["mention"]["candidate_entity_ids"]) == {"entity-1", "entity-2"}
    assert result["provisional_entity"] is None


def test_candidate_retrieval_keeps_typo_candidates_but_drops_weak_unrelated_names() -> None:
    index = CandidateIndex(
        [
            candidate_row("entity-1", "Josh Allen"),
            candidate_row("entity-2", "Patrick Mahomes"),
        ]
    )
    typo = index.retrieve(person_mention("Jsoh Allen"))
    unrelated = index.retrieve(person_mention("Kanye West"))

    assert [candidate.entity_id for candidate in typo] == ["entity-1"]
    assert unrelated == []


def test_structured_polymarket_person_can_become_provisional_but_x_cannot() -> None:
    index = CandidateIndex([])
    polymarket = resolve_mention(
        mention=person_mention("Future Prospect"),
        source=source(),
        source_context="Future Prospect",
        index=index,
        provider=DeterministicEntityProvider(),
        bank_version_id="bank-1",
        observed_at=NOW,
        allow_provisional=True,
    )
    x_result = resolve_mention(
        mention=person_mention("Future Prospect"),
        source=SourceReference(
            source_kind="news",
            source_id="news-1",
            source_content_sha256="b" * 64,
            news_id="news-1",
        ),
        source_context="Future Prospect",
        index=index,
        provider=DeterministicEntityProvider(),
        bank_version_id="bank-1",
        observed_at=NOW,
        allow_provisional=False,
    )

    assert polymarket["mention"]["match_method"] == "provisional_creation"
    assert polymarket["provisional_entity"]["identity_status"] == "provisional"
    assert x_result["mention"]["resolution_status"] == "unresolved"
    assert x_result["provisional_entity"] is None


def test_structured_prospect_becomes_provisional_after_only_weak_fuzzy_candidates() -> None:
    result = resolve_mention(
        mention=ExtractedMention(
            text="Arch Manning",
            entity_type=EntityType.PERSON,
            person_role_hint=PersonRoleHint.PROSPECT,
            mention_role=MentionRole.SUBJECT,
            evidence="Arch Manning",
            confidence=0.95,
            source_refs=["group_item_title"],
        ),
        source=source(),
        source_context="Arch Manning",
        index=CandidateIndex(
            [
                candidate_row("entity-dontae", "Dontae Manning"),
                candidate_row("entity-paul", "Paul Manning"),
            ]
        ),
        provider=DeterministicEntityProvider(),
        bank_version_id="bank-1",
        observed_at=NOW,
        allow_provisional=True,
    )

    assert result["mention"]["resolution_status"] == "resolved"
    assert result["mention"]["match_method"] == "provisional_creation"
    assert result["provisional_entity"]["canonical_name"] == "Arch Manning"


def test_high_similarity_candidate_blocks_provisional_duplicate() -> None:
    result = resolve_mention(
        mention=person_mention("Jsoh Allen"),
        source=source(),
        source_context="Jsoh Allen",
        index=CandidateIndex([candidate_row("entity-josh", "Josh Allen")]),
        provider=DeterministicEntityProvider(),
        bank_version_id="bank-1",
        observed_at=NOW,
        allow_provisional=True,
    )

    assert result["mention"]["resolution_status"] == "unresolved"
    assert result["provisional_entity"] is None


def test_claude_adjudication_rejects_hallucinated_entity_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = ClaudeEntityProvider.__new__(ClaudeEntityProvider)
    provider.model_name = "test-model"
    candidate = CandidateEntity(
        entity_id="allowed",
        canonical_name="Josh Allen",
        entity_type=EntityType.PERSON,
        identity_status=IdentityStatus.CANONICAL,
        aliases=["Josh Allen"],
        roles=["player"],
        teams=["Buffalo Bills"],
        lexical_score=1,
    )

    def fake_parse(**_kwargs: Any) -> ProviderResult:
        return ProviderResult(
            output=ResolutionDecision(
                status=ResolutionStatus.RESOLVED,
                entity_id="hallucinated",
                candidate_entity_ids=[],
                confidence=0.99,
                reason="Model memory.",
            ),
            usage=ProviderUsage(),
            provider="anthropic",
            model_name="test-model",
        )

    monkeypatch.setattr(provider, "_parse", fake_parse)
    with pytest.raises(ValueError, match="non-allowlisted"):
        provider.adjudicate(
            mention=person_mention("Josh Allen"),
            candidates=[candidate],
            source_context="Josh Allen",
            as_of=NOW,
        )


def test_claude_market_batch_rejects_omitted_duplicate_and_extra_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = ClaudeEntityProvider.__new__(ClaudeEntityProvider)
    provider.model_name = "test-model"

    def fake_parse(**_kwargs: Any) -> ProviderResult:
        return ProviderResult(
            output=MarketEventAnalysis(
                event_id="event-1",
                markets=[
                    DeterministicEntityProvider()
                    .analyze_market_event(
                        event_id="event-1",
                        event_title="NFL MVP",
                        event_slug=None,
                        markets=[
                            {
                                "market_id": "market-1",
                                "question": "Will Josh Allen win?",
                                "group_item_title": "Josh Allen",
                                "sports_market_type": None,
                                "outcomes": ["Yes", "No"],
                            }
                        ],
                    )
                    .output.markets[0]
                ],
            ),
            usage=ProviderUsage(),
            provider="anthropic",
            model_name="test-model",
        )

    monkeypatch.setattr(provider, "_parse", fake_parse)
    with pytest.raises(ValueError, match="coverage mismatch"):
        provider.analyze_market_event(
            event_id="event-1",
            event_title="NFL MVP",
            event_slug=None,
            markets=[
                {
                    "market_id": "market-1",
                    "question": "Will Josh Allen win?",
                    "group_item_title": "Josh Allen",
                    "sports_market_type": None,
                    "outcomes": ["Yes", "No"],
                },
                {
                    "market_id": "market-2",
                    "question": "Will Lamar Jackson win?",
                    "group_item_title": "Lamar Jackson",
                    "sports_market_type": None,
                    "outcomes": ["Yes", "No"],
                },
            ],
        )


def test_market_processing_suppresses_placeholders_and_skips_unchanged() -> None:
    base_market = {
        "question": "Will this player win MVP?",
        "slug": "candidate",
        "group_item_threshold": None,
        "sports_market_type": None,
        "source_content_sha256": "c" * 64,
        "outcomes": ["Yes", "No"],
        "prior_entity_input_sha256": None,
        "prior_extractor_version": None,
    }
    event = {
        "event_id": "event-1",
        "title": "NFL MVP",
        "slug": "nfl-mvp",
        "markets": [
            {**base_market, "market_id": "market-a", "group_item_title": "Player A"},
            {
                **base_market,
                "market_id": "market-josh",
                "group_item_title": "Josh Allen",
            },
            {
                **base_market,
                "market_id": "market-team",
                "group_item_title": "Cincinnati Bengals",
            },
        ],
    }
    batch = Batch()
    process_market_events(
        events=[event],
        provider=DeterministicEntityProvider(),
        index=CandidateIndex(
            [
                candidate_row("entity-1", "Josh Allen"),
                {
                    "entity_id": "team-1",
                    "canonical_name": "Cincinnati Bengals",
                    "entity_type": "team",
                    "identity_status": "canonical",
                    "aliases": ["Cincinnati Bengals", "Bengals", "CIN"],
                    "roles": [],
                    "teams": [],
                },
            ]
        ),
        bank_version_id="bank-1",
        batch=batch,
        observed_at=NOW,
    )

    assert {row["resolution_status"] for row in batch.mentions.values()} == {
        "ignored",
        "resolved",
    }
    assert len(batch.classifications) == 3
    assert batch.provisional_entities == {}
    team_mention = next(
        row for row in batch.mentions.values() if row["mention_text"] == "Cincinnati Bengals"
    )
    assert team_mention["entity_type_hint"] == "team"
    assert team_mention["entity_id"] == "team-1"
    assert team_mention["source_refs"] == ["group_item_title"]

    unchanged_event = {
        **event,
        "markets": [
            {
                **market,
                "prior_entity_input_sha256": entity_input_fingerprint(
                    event_title=event["title"],
                    event_slug=event["slug"],
                    market=market,
                ),
                "prior_extractor_version": EXTRACTOR_VERSION,
            }
            for market in event["markets"]
        ],
    }
    unchanged_batch = Batch()
    process_market_events(
        events=[unchanged_event],
        provider=DeterministicEntityProvider(),
        index=CandidateIndex([]),
        bank_version_id="bank-1",
        batch=unchanged_batch,
        observed_at=NOW,
    )
    assert unchanged_batch.skipped_unchanged_markets == 3
    assert unchanged_batch.classifications == {}


def test_market_processing_deduplicates_group_and_standalone_mentions() -> None:
    class DuplicateMentionProvider(DeterministicEntityProvider):
        def analyze_market_event(self, **kwargs: Any) -> ProviderResult:
            result = super().analyze_market_event(**kwargs)
            disposition = result.output.markets[0]
            disposition.group_item_mention_role = MentionRole.SUBJECT
            disposition.standalone_mentions = [
                ExtractedMention(
                    text="Arch Manning",
                    entity_type=EntityType.PERSON,
                    person_role_hint=PersonRoleHint.PROSPECT,
                    mention_role=MentionRole.SUBJECT,
                    evidence="Will Arch Manning be drafted first?",
                    confidence=0.9,
                    source_refs=["question"],
                )
            ]
            return result

    event = {
        "event_id": "event-draft",
        "title": "2027 NFL Draft",
        "slug": "2027-nfl-draft",
        "markets": [
            {
                "market_id": "market-arch",
                "question": "Will Arch Manning be drafted first?",
                "slug": "arch-manning-first",
                "group_item_title": "Arch Manning",
                "group_item_threshold": None,
                "sports_market_type": None,
                "source_content_sha256": "d" * 64,
                "outcomes": ["Yes", "No"],
                "prior_entity_input_sha256": None,
                "prior_extractor_version": None,
            }
        ],
    }
    batch = Batch()

    process_market_events(
        events=[event],
        provider=DuplicateMentionProvider(),
        index=CandidateIndex([]),
        bank_version_id="bank-1",
        batch=batch,
        observed_at=NOW,
    )

    arch_mentions = [
        row for row in batch.mentions.values() if row["normalized_text"] == "arch manning"
    ]
    assert len(arch_mentions) == 1


def test_market_processing_rejects_fabricated_evidence_and_unknown_source_refs() -> None:
    class InvalidEvidenceProvider(DeterministicEntityProvider):
        def analyze_market_event(self, **kwargs: Any) -> ProviderResult:
            result = super().analyze_market_event(**kwargs)
            result.output.markets[0].standalone_mentions = [
                ExtractedMention(
                    text="Josh Allen",
                    entity_type=EntityType.PERSON,
                    person_role_hint=PersonRoleHint.PLAYER,
                    mention_role=MentionRole.SUBJECT,
                    evidence="Josh Allen is listed as the winner",
                    confidence=0.9,
                    source_refs=["market-1"],
                )
            ]
            return result

    event = {
        "event_id": "event-invalid",
        "title": "NFL MVP",
        "slug": "nfl-mvp",
        "markets": [
            {
                "market_id": "market-1",
                "question": "Will Josh Allen win MVP?",
                "slug": "josh-allen-mvp",
                "group_item_title": None,
                "group_item_threshold": None,
                "sports_market_type": None,
                "source_content_sha256": "f" * 64,
                "outcomes": ["Yes", "No"],
                "prior_entity_input_sha256": None,
                "prior_extractor_version": None,
            }
        ],
    }
    batch = Batch()

    process_market_events(
        events=[event],
        provider=InvalidEvidenceProvider(),
        index=CandidateIndex([candidate_row("entity-josh", "Josh Allen")]),
        bank_version_id="bank-1",
        batch=batch,
        observed_at=NOW,
    )

    assert batch.mentions == {}
    assert len(batch.failures) == 1
    assert "not verbatim" in batch.failures[0]["error"]


def test_binary_outcomes_and_question_placeholders_never_resolve_as_entities() -> None:
    class PlaceholderGameProvider(DeterministicEntityProvider):
        def analyze_market_event(self, **kwargs: Any) -> ProviderResult:
            result = super().analyze_market_event(**kwargs)
            disposition = result.output.markets[0]
            disposition.market_topic = MarketTopic.GAME
            disposition.contract_type = ContractType.BINARY
            disposition.standalone_mentions = [
                ExtractedMention(
                    text="Person E",
                    entity_type=EntityType.PERSON,
                    person_role_hint=PersonRoleHint.UNKNOWN,
                    mention_role=MentionRole.SUBJECT,
                    evidence="Will Person E start Week 1 for the Chiefs?",
                    confidence=0.9,
                    source_refs=["question"],
                )
            ]
            return result

    event = {
        "event_id": "event-placeholder",
        "title": "Chiefs Week 1 starting QB?",
        "slug": "chiefs-week-1-qb",
        "markets": [
            {
                "market_id": "market-placeholder",
                "question": "Will Person E start Week 1 for the Chiefs?",
                "slug": "person-e-start",
                "group_item_title": None,
                "group_item_threshold": None,
                "sports_market_type": None,
                "source_content_sha256": "e" * 64,
                "outcomes": ["Yes", "No"],
                "prior_entity_input_sha256": None,
                "prior_extractor_version": None,
            }
        ],
    }
    batch = Batch()
    process_market_events(
        events=[event],
        provider=PlaceholderGameProvider(),
        index=CandidateIndex(
            [
                {
                    "entity_id": "team-no",
                    "canonical_name": "New Orleans Saints",
                    "entity_type": "team",
                    "identity_status": "canonical",
                    "aliases": ["New Orleans Saints", "Saints", "NO"],
                    "roles": [],
                    "teams": [],
                }
            ]
        ),
        bank_version_id="bank-1",
        batch=batch,
        observed_at=NOW,
    )

    assert [row["mention_text"] for row in batch.mentions.values()] == ["Person E"]
    mention = next(iter(batch.mentions.values()))
    assert mention["resolution_status"] == "ignored"
    assert mention["resolution_metadata"]["reason"] == "polymarket_placeholder"
    assert batch.attempts == {}


def test_game_market_recovers_canonical_team_outcomes_without_model_mentions() -> None:
    event = {
        "event_id": "event-game",
        "title": "Buffalo Bills vs. Cincinnati Bengals",
        "slug": "buf-cin",
        "markets": [
            {
                "market_id": "market-game",
                "question": "Bills or Bengals?",
                "slug": "buf-cin-moneyline",
                "group_item_title": None,
                "group_item_threshold": None,
                "sports_market_type": "moneyline",
                "source_content_sha256": "e" * 64,
                "outcomes": ["Bills", "Bengals"],
                "prior_entity_input_sha256": None,
                "prior_extractor_version": None,
            }
        ],
    }
    team_rows = [
        {
            "entity_id": "team-buf",
            "canonical_name": "Buffalo Bills",
            "entity_type": "team",
            "identity_status": "canonical",
            "aliases": ["Buffalo Bills", "Bills", "BUF"],
            "roles": [],
            "teams": [],
        },
        {
            "entity_id": "team-cin",
            "canonical_name": "Cincinnati Bengals",
            "entity_type": "team",
            "identity_status": "canonical",
            "aliases": ["Cincinnati Bengals", "Bengals", "CIN"],
            "roles": [],
            "teams": [],
        },
    ]
    batch = Batch()

    process_market_events(
        events=[event],
        provider=DeterministicEntityProvider(),
        index=CandidateIndex(team_rows),
        bank_version_id="bank-1",
        batch=batch,
        observed_at=NOW,
    )

    assert {row["mention_text"]: row["entity_id"] for row in batch.mentions.values()} == {
        "Bills": "team-buf",
        "Bengals": "team-cin",
    }
    assert all(row["source_refs"] == ["outcomes"] for row in batch.mentions.values())


def test_moneyline_recovery_preserves_uppercase_short_team_abbreviations() -> None:
    event = {
        "event_id": "event-short-alias",
        "title": "New Orleans Saints vs. Atlanta Falcons",
        "slug": "no-atl",
        "markets": [
            {
                "market_id": "market-short-alias",
                "question": "NO or ATL?",
                "slug": "no-atl-moneyline",
                "group_item_title": None,
                "group_item_threshold": None,
                "sports_market_type": "moneyline",
                "source_content_sha256": "f" * 64,
                "outcomes": ["NO", "ATL"],
                "prior_entity_input_sha256": None,
                "prior_extractor_version": None,
            }
        ],
    }
    teams = [
        {
            "entity_id": "team-no",
            "canonical_name": "New Orleans Saints",
            "entity_type": "team",
            "identity_status": "canonical",
            "aliases": ["New Orleans Saints", "Saints", "NO"],
            "roles": [],
            "teams": [],
        },
        {
            "entity_id": "team-atl",
            "canonical_name": "Atlanta Falcons",
            "entity_type": "team",
            "identity_status": "canonical",
            "aliases": ["Atlanta Falcons", "Falcons", "ATL"],
            "roles": [],
            "teams": [],
        },
    ]
    batch = Batch()
    process_market_events(
        events=[event],
        provider=DeterministicEntityProvider(),
        index=CandidateIndex(teams),
        bank_version_id="bank-1",
        batch=batch,
        observed_at=NOW,
    )

    assert {row["mention_text"]: row["entity_id"] for row in batch.mentions.values()} == {
        "NO": "team-no",
        "ATL": "team-atl",
    }


def test_pending_unresolved_mention_reresolves_after_bank_change() -> None:
    row = {
        "mention_id": "old-mention-id",
        "news_id": "news-1",
        "polymarket_event_id": None,
        "polymarket_market_id": None,
        "entity_id": None,
        "mention_text": "Josh Allen",
        "normalized_text": "josh allen",
        "entity_type_hint": "person",
        "person_role_hint": "player",
        "mention_role": "referenced",
        "evidence": "Josh Allen",
        "source_content_sha256": "a" * 64,
        "extractor_version": "entity-extractor-v1",
        "resolver_version": "entity-resolver-v1",
        "resolution_status": "unresolved",
        "match_method": None,
        "confidence": 0,
        "last_bank_version_id": "old-bank",
        "candidate_entity_ids": [],
        "resolution_metadata": {},
        "first_observed_at": NOW,
        "last_observed_at": NOW,
        "created_at": NOW,
        "updated_at": NOW,
    }
    batch = Batch()
    process_pending_mentions(
        rows=[row],
        provider=DeterministicEntityProvider(),
        index=CandidateIndex([candidate_row("entity-1", "Josh Allen")]),
        bank_version_id="new-bank",
        batch=batch,
        observed_at=NOW,
    )

    assert len(batch.mentions) == 1
    resolved = next(iter(batch.mentions.values()))
    assert resolved["entity_id"] == "entity-1"
    assert resolved["resolution_status"] == "resolved"
    assert resolved["last_bank_version_id"] == "new-bank"


def kalshi_event(**market_overrides: Any) -> dict[str, Any]:
    """A Kalshi event shaped as ResolutionRepository.load_kalshi_market_events
    emits it: ticker in market_id/slug, market title in question,
    yes_sub_title in group_item_title."""
    market: dict[str, Any] = {
        "market_id": "KXNFLGAME-26AUG15DALSEA-SEA",
        "question": "Will Seattle win the Dallas vs Seattle Pro Football game?",
        "slug": "KXNFLGAME-26AUG15DALSEA-SEA",
        "group_item_title": "Seattle",
        "group_item_threshold": None,
        "sports_market_type": None,
        "source_content_sha256": "d" * 64,
        "prior_entity_input_sha256": None,
        "prior_extractor_version": None,
        "outcomes": ["Seattle"],
    }
    market.update(market_overrides)
    return {
        "event_id": "KXNFLGAME-26AUG15DALSEA",
        "title": "Dallas at Seattle Winner",
        "slug": "KXNFLGAME-26AUG15DALSEA",
        "markets": [market],
    }


def test_kalshi_markets_produce_ticker_mentions_and_classifications() -> None:
    batch = Batch()
    process_market_events(
        events=[kalshi_event()],
        provider=DeterministicEntityProvider(),
        index=CandidateIndex(
            [
                {
                    "entity_id": "team-sea",
                    "canonical_name": "Seattle Seahawks",
                    "entity_type": "team",
                    "identity_status": "canonical",
                    "aliases": ["Seattle Seahawks", "Seattle", "SEA"],
                    "roles": [],
                    "teams": [],
                }
            ]
        ),
        bank_version_id="bank-1",
        batch=batch,
        observed_at=NOW,
        source_kind="kalshi_market",
    )

    assert batch.failures == []
    classification = batch.classifications["KXNFLGAME-26AUG15DALSEA-SEA"]
    assert classification["market_ticker"] == "KXNFLGAME-26AUG15DALSEA-SEA"
    assert "market_id" not in classification
    assert batch.mentions
    for mention in batch.mentions.values():
        assert mention["kalshi_market_ticker"] == "KXNFLGAME-26AUG15DALSEA-SEA"
        assert mention["polymarket_market_id"] is None
        assert mention["polymarket_event_id"] is None
    resolved = next(
        row for row in batch.mentions.values() if row["mention_text"] == "Seattle"
    )
    assert resolved["entity_id"] == "team-sea"
    assert resolved["resolution_status"] == "resolved"


def test_kalshi_ignored_group_item_is_recorded_not_failed() -> None:
    class IgnoringProvider(DeterministicEntityProvider):
        def analyze_market_event(self, **kwargs: Any) -> ProviderResult:
            result = super().analyze_market_event(**kwargs)
            disposition = result.output.markets[0]
            disposition.group_item_entity_type = None
            disposition.group_item_person_role_hint = None
            disposition.group_item_mention_role = None
            disposition.ignore_group_item = True
            disposition.ignore_reason = "ladder strike label, not an entity"
            return result

    batch = Batch()
    process_market_events(
        events=[
            kalshi_event(
                market_id="KXNFL1QTOTAL-26AUG06CARARI-11",
                slug="KXNFL1QTOTAL-26AUG06CARARI-11",
                question="Will there be over 10.5 1Q points scored?",
                group_item_title="Over 10.5 1Q points scored",
                group_item_threshold="10.5",
                outcomes=["Over 10.5 1Q points scored"],
            )
        ],
        provider=IgnoringProvider(),
        index=CandidateIndex([]),
        bank_version_id="bank-1",
        batch=batch,
        observed_at=NOW,
        source_kind="kalshi_market",
    )

    assert batch.failures == []
    ignored = next(iter(batch.mentions.values()))
    assert ignored["resolution_status"] == "ignored"
    assert ignored["mention_text"] == "Over 10.5 1Q points scored"
    assert ignored["kalshi_market_ticker"] == "KXNFL1QTOTAL-26AUG06CARARI-11"
    assert (
        ignored["resolution_metadata"]["reason"]
        == "ladder strike label, not an entity"
    )


def test_pending_kalshi_mention_keeps_its_ticker_source() -> None:
    row = {
        "mention_id": "kalshi-mention-id",
        "news_id": None,
        "polymarket_event_id": None,
        "polymarket_market_id": None,
        "kalshi_market_ticker": "KXNFLGAME-26AUG15DALSEA-SEA",
        "entity_id": None,
        "mention_text": "Josh Allen",
        "normalized_text": "josh allen",
        "entity_type_hint": "person",
        "person_role_hint": "player",
        "mention_role": "referenced",
        "evidence": "Josh Allen",
        "source_content_sha256": "a" * 64,
        "extractor_version": "entity-extractor-v1",
        "resolver_version": "entity-resolver-v1",
        "resolution_status": "unresolved",
        "match_method": None,
        "confidence": 0,
        "last_bank_version_id": "old-bank",
        "candidate_entity_ids": [],
        "resolution_metadata": {},
        "first_observed_at": NOW,
        "last_observed_at": NOW,
        "created_at": NOW,
        "updated_at": NOW,
    }
    batch = Batch()
    process_pending_mentions(
        rows=[row],
        provider=DeterministicEntityProvider(),
        index=CandidateIndex([candidate_row("entity-1", "Josh Allen")]),
        bank_version_id="new-bank",
        batch=batch,
        observed_at=NOW,
    )

    resolved = next(iter(batch.mentions.values()))
    assert resolved["entity_id"] == "entity-1"
    assert resolved["kalshi_market_ticker"] == "KXNFLGAME-26AUG15DALSEA-SEA"
    assert resolved["polymarket_market_id"] is None


def test_live_write_commands_require_explicit_confirmation() -> None:
    assert nflverse_sync_main(["--apply"]) == 2
    assert gamma_backfill_main(["--apply"]) == 2
    assert worker_main(["--apply", "--provider", "mock"]) == 2


def test_accuracy_sweep_uses_two_passes_and_proposes_consensus_change() -> None:
    row = {
        "mention_id": "mention-1",
        "news_id": "news-1",
        "polymarket_market_id": None,
        "polymarket_event_id": None,
        "mention_text": "Josh Allen",
        "entity_type_hint": "person",
        "person_role_hint": "player",
        "mention_role": "subject",
        "evidence": "Bills quarterback Josh Allen practiced in full.",
        "source_refs": ["tweet"],
        "resolution_status": "ambiguous",
        "entity_id": None,
        "match_method": None,
        "confidence": 0.4,
        "candidate_entity_ids": [],
        "resolver_version": "entity-resolver-v4",
        "resolution_metadata": {},
        "last_observed_at": NOW,
        "updated_at": NOW,
        "news_text": "Bills quarterback Josh Allen practiced in full.",
        "market_question": None,
        "market_slug": None,
        "market_event_title": None,
        "direct_event_title": None,
    }

    findings, summary = run_sweep_records(
        [row],
        candidate_rows=[candidate_row("entity-1", "Josh Allen")],
        provider=DeterministicEntityProvider(),
    )

    assert summary["processed"] == 1
    assert summary["proposed_change"] == 1
    assert findings[0]["outcome"] == "proposed_change"
    assert findings[0]["recommended_resolution"]["entity_id"] == "entity-1"
    assert len(findings[0]["pass_decisions"]) == 2
    assert findings[0]["expected_updated_at"] == NOW
