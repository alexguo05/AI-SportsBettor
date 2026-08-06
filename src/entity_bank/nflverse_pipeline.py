"""Fetch and normalize versioned nflverse entity snapshots."""

from __future__ import annotations

import base64
import csv
import gzip
import hashlib
import io
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
from typing import Any

import requests

from src.common.gcs import canonical_json_bytes
from src.entity_bank.models import AliasType, EntityType, PersonRoleHint
from src.entity_bank.normalization import normalize_name

SCHEMA_NAME = "nflverse_entity_snapshot"
SCHEMA_VERSION = 1
STORAGE_PROVIDER = "nflverse"
STORAGE_SOURCE = "github-releases"
STORAGE_OBJECT = "entity-snapshot"
NFLVERSE_NORMALIZER_VERSION = "nflverse-normalizer-v3"
ENTITY_NAMESPACE = uuid.UUID("7fbcb3dd-6f5c-47f0-989d-731c3fb45085")
DEFAULT_BASE_URL = "https://github.com/nflverse/nflverse-data/releases/download"
DEFAULT_API_URL = "https://api.github.com/repos/nflverse/nflverse-data"


@dataclass(frozen=True)
class SourceAsset:
    name: str
    url: str
    content: bytes
    etag: str | None
    sha256: str
    asset_id: int | None = None
    provider_digest: str | None = None
    source_updated_at: str | None = None


@dataclass(frozen=True)
class NflverseSnapshot:
    season: int
    observed_at: datetime
    assets: tuple[SourceAsset, ...]
    entities: tuple[dict[str, Any], ...]
    complete_player_history: tuple[dict[str, Any], ...]
    relationships: tuple[dict[str, Any], ...]
    quarantined_records: tuple[dict[str, Any], ...]
    source_mapping_conflicts: tuple[dict[str, Any], ...]
    quality: dict[str, int]
    content_sha256: str


class NflverseClient:
    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout_seconds: float = 60,
        max_attempts: int = 4,
        sleep: Any = time.sleep,
        base_url: str = DEFAULT_BASE_URL,
        api_url: str = DEFAULT_API_URL,
    ) -> None:
        self.session = session or requests.Session()
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.sleep = sleep
        self.base_url = base_url.rstrip("/")
        self.api_url = api_url.rstrip("/")

    def _get(self, url: str, **kwargs: Any) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.get(
                    url,
                    timeout=self.timeout_seconds,
                    **kwargs,
                )
                response.raise_for_status()
                return response
            except (requests.RequestException, RuntimeError) as exc:
                last_error = exc
                if attempt < self.max_attempts:
                    self.sleep(min(2 ** (attempt - 1), 8))
        raise RuntimeError(f"nflverse request failed: {url}") from last_error

    def _discover_asset(self, tag: str, filename: str) -> dict[str, Any]:
        release = self._get(f"{self.api_url}/releases/tags/{tag}").json()
        assets = list(release.get("assets") or [])
        matching = [asset for asset in assets if asset.get("name") == filename]
        if not matching and release.get("assets_url"):
            page = 1
            while True:
                page_assets = self._get(
                    release["assets_url"],
                    params={"per_page": 100, "page": page},
                ).json()
                if not page_assets:
                    break
                matching.extend(
                    asset for asset in page_assets if asset.get("name") == filename
                )
                if matching or len(page_assets) < 100:
                    break
                page += 1
        if len(matching) != 1:
            raise RuntimeError(
                f"expected one nflverse {tag}/{filename} asset; found {len(matching)}"
            )
        return matching[0]

    def _download(self, name: str, tag: str, filename: str) -> SourceAsset:
        asset = self._discover_asset(tag, filename)
        url = asset.get("browser_download_url") or (
            f"{self.base_url}/{tag}/{filename}"
        )
        response = self._get(url)
        content = response.content
        computed_sha256 = hashlib.sha256(content).hexdigest()
        provider_digest = asset.get("digest")
        if provider_digest:
            algorithm, separator, expected_digest = provider_digest.partition(":")
            if separator != ":" or algorithm.casefold() != "sha256":
                raise RuntimeError(f"unsupported nflverse digest {provider_digest!r}")
            if computed_sha256 != expected_digest.casefold():
                raise RuntimeError(
                    f"nflverse digest mismatch for {filename}: "
                    f"expected {expected_digest}, got {computed_sha256}"
                )
        return SourceAsset(
            name=name,
            url=url,
            content=content,
            etag=response.headers.get("ETag"),
            sha256=computed_sha256,
            asset_id=asset.get("id"),
            provider_digest=provider_digest,
            source_updated_at=asset.get("updated_at"),
        )

    def fetch(self, season: int) -> tuple[SourceAsset, ...]:
        specs = (
            ("teams", "teams", "teams_colors_logos.csv"),
            ("players", "players", "players.csv"),
            (
                "rosters",
                "rosters",
                f"roster_{season}.csv",
            ),
        )
        return tuple(
            self._download(name, tag, filename)
            for name, tag, filename in specs
        )


def _csv_rows(asset: SourceAsset) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(asset.content.decode("utf-8-sig"))))


def _entity_id(source_type: str, source_id: str) -> str:
    return str(uuid.uuid5(ENTITY_NAMESPACE, f"nflverse:{source_type}:{source_id}"))


def _nonempty(row: dict[str, str], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key, "").strip()
        if value:
            return value
    return None


def _alias(value: str, alias_type: AliasType) -> dict[str, str]:
    return {
        "alias": value,
        "normalized_alias": normalize_name(value),
        "alias_type": alias_type.value,
    }


def _normalize_teams(
    rows: list[dict[str, str]],
    current_abbreviations: set[str],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        team_id = row.get("team_id", "").strip()
        if team_id:
            grouped.setdefault(team_id, []).append(row)

    entities: list[dict[str, Any]] = []
    abbreviation_to_entity: dict[str, str] = {}
    for team_id, variants in sorted(grouped.items()):
        current = next(
            (row for row in variants if row.get("team_abbr") in current_abbreviations),
            variants[0],
        )
        canonical_name = current["team_name"].strip()
        entity_id = _entity_id("team", team_id)
        aliases: dict[tuple[str, str], dict[str, str]] = {}
        for row in variants:
            team_name = row.get("team_name", "").strip()
            team_nick = row.get("team_nick", "").strip()
            # Markets and tweets routinely name a team by bare location
            # ("Tampa Bay", "Arizona"), which is never an nflverse column;
            # derive it as the full name minus the nickname suffix.
            location = (
                team_name[: -len(team_nick)].strip()
                if team_nick and team_name.endswith(team_nick)
                else ""
            )
            for value, alias_type in (
                (team_name, AliasType.FULL_NAME),
                (team_nick, AliasType.NICKNAME),
                (row.get("team_abbr", "").strip(), AliasType.ABBREVIATION),
                (location, AliasType.LOCATION),
            ):
                if value:
                    item = _alias(value, alias_type)
                    aliases[(item["normalized_alias"], item["alias_type"])] = item
            abbreviation = row.get("team_abbr", "").strip()
            if abbreviation:
                abbreviation_to_entity[abbreviation] = entity_id
        entities.append(
            {
                "entity_id": entity_id,
                "entity_type": EntityType.TEAM.value,
                "canonical_name": canonical_name,
                "normalized_name": normalize_name(canonical_name),
                "source_mappings": [
                    {
                        "provider": "nflverse",
                        "source_entity_type": "team",
                        "source_entity_id": team_id,
                        "metadata": {
                            "current_abbreviation": current.get("team_abbr"),
                            "conference": current.get("team_conf"),
                            "division": current.get("team_division"),
                        },
                    }
                ],
                "aliases": list(aliases.values()),
                "roles": [],
            }
        )
    return entities, abbreviation_to_entity


PLAYER_ID_FIELDS: tuple[tuple[str, str], ...] = (
    ("gsis_id", "gsis"),
    ("esb_id", "nfl_esb"),
    ("smart_id", "nflverse_smart"),
    ("nfl_id", "nfl"),
    ("espn_id", "espn"),
    ("sportradar_id", "sportradar"),
    ("pfr_id", "pfr"),
    ("pff_id", "pff"),
    ("rotowire_id", "rotowire"),
    ("yahoo_id", "yahoo"),
    ("fantasy_data_id", "fantasy_data"),
    ("sleeper_id", "sleeper"),
    ("gsis_it_id", "gsis_it"),
)


def _player_identifiers(row: dict[str, str]) -> list[tuple[str, str]]:
    return [
        (provider, value)
        for field, provider in PLAYER_ID_FIELDS
        if (value := row.get(field, "").strip())
    ]


def _player_source_key(row: dict[str, str]) -> str | None:
    identifiers = _player_identifiers(row)
    return identifiers[0][1] if identifiers else None


def _person_names_compatible(left: str, right: str) -> bool:
    suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}

    def tokens(value: str) -> list[str]:
        result = normalize_name(value).split()
        while result and result[-1] in suffixes:
            result.pop()
        return result

    left_tokens = tokens(left)
    right_tokens = tokens(right)
    if not left_tokens or not right_tokens:
        return False
    if left_tokens == right_tokens:
        return True
    if left_tokens[-1] == right_tokens[-1]:
        return True
    return (
        left_tokens[0] == right_tokens[0]
        and SequenceMatcher(
            None,
            " ".join(left_tokens),
            " ".join(right_tokens),
        ).ratio()
        >= 0.85
    )


def _compatible_player_rows(left: dict[str, str], right: dict[str, str]) -> bool:
    if not _person_names_compatible(
        left.get("display_name", ""),
        right.get("display_name", ""),
    ):
        return False
    left_birth_date = left.get("birth_date", "").strip()
    right_birth_date = right.get("birth_date", "").strip()
    return not (
        left_birth_date
        and right_birth_date
        and left_birth_date != right_birth_date
    )


def _preferred_identity_key(rows: list[dict[str, str]]) -> str:
    modern_gsis = sorted(
        {
            row.get("gsis_id", "").strip()
            for row in rows
            if row.get("gsis_id", "").strip().startswith("00-")
        }
    )
    if modern_gsis:
        return modern_gsis[0]
    all_gsis = sorted(
        {
            row.get("gsis_id", "").strip()
            for row in rows
            if row.get("gsis_id", "").strip()
        }
    )
    if all_gsis:
        return all_gsis[0]
    source_keys = sorted(
        source_key
        for row in rows
        if (source_key := _player_source_key(row))
    )
    if not source_keys:
        raise ValueError("player identity group has no source key")
    return source_keys[0]


def _build_player_entity(rows: list[dict[str, str]]) -> dict[str, Any]:
    representative = max(
        rows,
        key=lambda row: (
            row.get("gsis_id", "").startswith("00-"),
            bool(row.get("birth_date", "").strip()),
            row.get("last_season", ""),
            len(_player_identifiers(row)),
        ),
    )
    canonical_name = representative["display_name"].strip()
    aliases: dict[tuple[str, str], dict[str, str]] = {}
    mappings: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        for field, alias_type in (
            ("display_name", AliasType.CANONICAL_NAME),
            ("football_name", AliasType.FOOTBALL_NAME),
            ("short_name", AliasType.INITIALS),
        ):
            value = row.get(field, "").strip()
            if value:
                item = _alias(value, alias_type)
                aliases[(item["normalized_alias"], item["alias_type"])] = item
        for provider, source_id in _player_identifiers(row):
            mappings[(provider, source_id)] = {
                "provider": provider,
                "source_entity_type": "person",
                "source_entity_id": source_id,
                "metadata": {},
            }
    positions = sorted(
        {
            row.get("position", "").strip()
            for row in rows
            if row.get("position", "").strip()
        }
    )
    position_groups = sorted(
        {
            row.get("position_group", "").strip()
            for row in rows
            if row.get("position_group", "").strip()
        }
    )
    return {
        "entity_id": _entity_id("person", _preferred_identity_key(rows)),
        "entity_type": EntityType.PERSON.value,
        "canonical_name": canonical_name,
        "normalized_name": normalize_name(canonical_name),
        "source_mappings": list(mappings.values()),
        "aliases": list(aliases.values()),
        "roles": [
            {
                "role": PersonRoleHint.PLAYER.value,
                "source": "nflverse",
                "evidence": {
                    "positions": positions,
                    "position_groups": position_groups,
                    "rookie_season": representative.get("rookie_season"),
                    "last_season": max(
                        (
                            row.get("last_season", "")
                            for row in rows
                            if row.get("last_season", "")
                        ),
                        default=None,
                    ),
                    "merged_source_rows": len(rows),
                },
            }
        ],
    }


def _normalize_players(
    rows: list[dict[str, str]],
    *,
    source: str,
) -> tuple[
    list[dict[str, Any]],
    dict[tuple[str, str], str],
    list[dict[str, Any]],
    list[dict[str, Any]],
    int,
]:
    valid_rows: list[dict[str, str]] = []
    quarantined: list[dict[str, Any]] = []
    for row in rows:
        reasons = []
        if not _player_identifiers(row):
            reasons.append("missing_stable_person_identifier")
        if not row.get("display_name", "").strip():
            reasons.append("missing_person_name")
        if reasons:
            quarantined.append(
                {
                    "source": source,
                    "status": "quarantined",
                    "reason_codes": reasons,
                    "record": row,
                }
            )
        else:
            valid_rows.append(row)

    parents = list(range(len(valid_rows)))

    def root(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = root(left)
        right_root = root(right)
        if left_root != right_root:
            parents[right_root] = left_root

    identifier_rows: dict[tuple[str, str], list[int]] = {}
    for index, row in enumerate(valid_rows):
        for identifier in _player_identifiers(row):
            for prior_index in identifier_rows.get(identifier, []):
                if _compatible_player_rows(valid_rows[prior_index], row):
                    union(prior_index, index)
            identifier_rows.setdefault(identifier, []).append(index)

    grouped: dict[int, list[dict[str, str]]] = {}
    for index, row in enumerate(valid_rows):
        grouped.setdefault(root(index), []).append(row)
    entities = [_build_player_entity(group) for group in grouped.values()]

    mapping_owners: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for entity in entities:
        for mapping in entity["source_mappings"]:
            key = (mapping["provider"], mapping["source_entity_id"])
            mapping_owners.setdefault(key, []).append(entity)
    conflicts: list[dict[str, Any]] = []
    conflicting_keys = {
        key for key, owners in mapping_owners.items() if len(owners) > 1
    }
    for provider, source_id in sorted(conflicting_keys):
        owners = mapping_owners[(provider, source_id)]
        conflicts.append(
            {
                "source": source,
                "provider": provider,
                "source_entity_type": "person",
                "source_entity_id": source_id,
                "status": "excluded_from_bank",
                "reason": "identifier_maps_to_multiple_incompatible_people",
                "entities": [
                    {
                        "entity_id": entity["entity_id"],
                        "canonical_name": entity["canonical_name"],
                    }
                    for entity in owners
                ],
            }
        )
    for entity in entities:
        entity["source_mappings"] = [
            mapping
            for mapping in entity["source_mappings"]
            if (mapping["provider"], mapping["source_entity_id"])
            not in conflicting_keys
        ]
    entities_with_mappings = []
    for entity in entities:
        if entity["source_mappings"]:
            entities_with_mappings.append(entity)
        else:
            quarantined.append(
                {
                    "source": source,
                    "status": "quarantined",
                    "reason_codes": ["all_person_identifiers_conflicted"],
                    "record": {
                        "canonical_name": entity["canonical_name"],
                        "entity_id": entity["entity_id"],
                    },
                }
            )
    source_to_entity = {
        (mapping["provider"], mapping["source_entity_id"]): entity["entity_id"]
        for entity in entities_with_mappings
        for mapping in entity["source_mappings"]
    }
    merged_rows = len(valid_rows) - len(entities)
    return (
        entities_with_mappings,
        source_to_entity,
        conflicts,
        quarantined,
        merged_rows,
    )


def normalize_snapshot(
    assets: tuple[SourceAsset, ...],
    *,
    season: int,
    observed_at: datetime,
) -> NflverseSnapshot:
    by_name = {asset.name: asset for asset in assets}
    roster_rows = _csv_rows(by_name["rosters"])
    current_abbreviations = {
        row["team"].strip() for row in roster_rows if row.get("team", "").strip()
    }
    team_entities, team_ids = _normalize_teams(
        _csv_rows(by_name["teams"]),
        current_abbreviations,
    )
    player_rows = _csv_rows(by_name["players"])
    (
        player_entities,
        person_ids,
        source_mapping_conflicts,
        quarantined_records,
        merged_player_rows,
    ) = _normalize_players(
        player_rows,
        source="players",
    )
    entity_by_id = {
        entity["entity_id"]: entity
        for entity in team_entities + player_entities
    }

    relationships: list[dict[str, Any]] = []
    missing_person_source_id = 0
    unknown_person_mapping = 0
    unknown_team_mapping = 0
    roster_only_people = 0
    for row in roster_rows:
        roster_player_row = {
            **row,
            "display_name": row.get("full_name", ""),
            "position_group": row.get("position", ""),
            "rookie_season": row.get("rookie_year", ""),
            "last_season": str(season),
        }
        identifiers = _player_identifiers(roster_player_row)
        if not identifiers:
            missing_person_source_id += 1
            quarantined_records.append(
                {
                    "source": "rosters",
                    "status": "quarantined",
                    "reason_codes": ["missing_stable_person_identifier"],
                    "record": row,
                }
            )
            continue
        team_entity_id = team_ids.get(row.get("team", "").strip())
        if not team_entity_id:
            unknown_team_mapping += 1
            quarantined_records.append(
                {
                    "source": "rosters",
                    "status": "quarantined",
                    "reason_codes": ["unknown_team_abbreviation"],
                    "record": row,
                }
            )
            continue

        matching_entity_ids = {
            person_ids[identifier]
            for identifier in identifiers
            if identifier in person_ids
        }
        if len(matching_entity_ids) > 1:
            matched_entities = [
                {
                    "entity_id": entity_id,
                    "canonical_name": entity_by_id[entity_id]["canonical_name"],
                }
                for entity_id in sorted(matching_entity_ids)
            ]
            quarantined_records.append(
                {
                    "source": "rosters",
                    "status": "quarantined",
                    "reason_codes": ["identifiers_resolve_to_multiple_people"],
                    "identifier_matches": matched_entities,
                    "record": row,
                }
            )
            source_mapping_conflicts.append(
                {
                    "source": "rosters",
                    "status": "record_quarantined",
                    "reason": "identifiers_resolve_to_multiple_people",
                    "identifiers": [
                        {"provider": provider, "source_entity_id": source_id}
                        for provider, source_id in identifiers
                    ],
                    "entities": matched_entities,
                }
            )
            continue

        normalized_full_name = normalize_name(row.get("full_name", ""))
        if matching_entity_ids:
            person_entity_id = next(iter(matching_entity_ids))
            matched_entity = entity_by_id[person_entity_id]
            accepted_name_values = {
                matched_entity["canonical_name"],
                *(alias["alias"] for alias in matched_entity["aliases"]),
            }
            if not normalized_full_name or not any(
                _person_names_compatible(row.get("full_name", ""), accepted_name)
                for accepted_name in accepted_name_values
            ):
                quarantined_records.append(
                    {
                        "source": "rosters",
                        "status": "quarantined",
                        "reason_codes": ["name_conflicts_with_identifier_owner"],
                        "identifier_matches": [
                            {
                                "entity_id": person_entity_id,
                                "canonical_name": matched_entity["canonical_name"],
                            }
                        ],
                        "record": row,
                    }
                )
                source_mapping_conflicts.append(
                    {
                        "source": "rosters",
                        "status": "record_quarantined",
                        "reason": "name_conflicts_with_identifier_owner",
                        "identifiers": [
                            {"provider": provider, "source_entity_id": source_id}
                            for provider, source_id in identifiers
                        ],
                        "entities": [
                            {
                                "entity_id": person_entity_id,
                                "canonical_name": matched_entity["canonical_name"],
                            }
                        ],
                    }
                )
                continue
            if normalized_full_name not in {
                alias["normalized_alias"] for alias in matched_entity["aliases"]
            }:
                matched_entity["aliases"].append(
                    _alias(row["full_name"].strip(), AliasType.PROVIDER_NAME)
                )
        else:
            football_name = normalize_name(row.get("football_name", ""))
            full_name_tokens = set(normalized_full_name.split())
            if (
                not normalized_full_name
                or (football_name and football_name not in full_name_tokens)
            ):
                quarantined_records.append(
                    {
                        "source": "rosters",
                        "status": "quarantined",
                        "reason_codes": [
                            (
                                "missing_person_name"
                                if not normalized_full_name
                                else "football_name_conflicts_with_full_name"
                            )
                        ],
                        "record": row,
                    }
                )
                continue
            (
                roster_entities,
                roster_person_ids,
                roster_conflicts,
                roster_quarantined,
                _,
            ) = _normalize_players(
                [roster_player_row],
                source="rosters",
            )
            source_mapping_conflicts.extend(roster_conflicts)
            quarantined_records.extend(roster_quarantined)
            if len(roster_entities) != 1:
                unknown_person_mapping += 1
                continue
            roster_entity = roster_entities[0]
            player_entities.append(roster_entity)
            entity_by_id[roster_entity["entity_id"]] = roster_entity
            person_ids.update(roster_person_ids)
            person_entity_id = roster_entity["entity_id"]
            roster_only_people += 1

        person_source_id = _player_source_key(roster_player_row)
        if person_source_id is None:
            raise AssertionError("validated roster row lost its source key")
        source_key = (
            f"nflverse:roster:{season}:{person_source_id}:{row.get('team', '')}"
        )
        relationships.append(
            {
                "relationship_id": str(
                    uuid.uuid5(ENTITY_NAMESPACE, f"relationship:{source_key}")
                ),
                "subject_entity_id": person_entity_id,
                "predicate": "rostered_by",
                "object_entity_id": team_entity_id,
                "source": "nflverse",
                "source_key": source_key,
                "evidence": {
                    "season": season,
                    "week": row.get("week"),
                    "game_type": row.get("game_type"),
                    "status": row.get("status"),
                    "position": row.get("position"),
                    "jersey_number": row.get("jersey_number"),
                },
            }
        )

    mapping_owners: dict[tuple[str, str], set[str]] = {}
    for entity in player_entities:
        for mapping in entity["source_mappings"]:
            key = (mapping["provider"], mapping["source_entity_id"])
            mapping_owners.setdefault(key, set()).add(entity["entity_id"])
    unsafe_mapping_collisions = sum(
        len(owner_ids) > 1 for owner_ids in mapping_owners.values()
    )
    if unsafe_mapping_collisions:
        raise ValueError(
            f"normalized nflverse bank has {unsafe_mapping_collisions} "
            "unresolved source mapping collisions"
        )

    current_person_ids = {
        relationship["subject_entity_id"] for relationship in relationships
    }
    current_player_entities = [
        entity
        for entity in player_entities
        if entity["entity_id"] in current_person_ids
    ]
    alias_owners: dict[str, set[str]] = {}
    for entity in team_entities + current_player_entities:
        for alias in entity["aliases"]:
            alias_owners.setdefault(alias["normalized_alias"], set()).add(
                entity["entity_id"]
            )
    content_sha256 = hashlib.sha256(
        canonical_json_bytes(
            {
                "normalizer_version": NFLVERSE_NORMALIZER_VERSION,
                "assets": {
                    asset.name: asset.sha256 for asset in assets
                },
            }
        )
    ).hexdigest()
    return NflverseSnapshot(
        season=season,
        observed_at=observed_at.astimezone(UTC),
        assets=assets,
        entities=tuple(team_entities + current_player_entities),
        complete_player_history=tuple(player_entities),
        relationships=tuple(relationships),
        quarantined_records=tuple(quarantined_records),
        source_mapping_conflicts=tuple(source_mapping_conflicts),
        quality={
            "source_player_rows": len(player_rows),
            "merged_player_rows": merged_player_rows,
            "complete_player_history": len(player_entities),
            "current_rostered_people": len(current_player_entities),
            "roster_only_people": roster_only_people,
            "source_roster_rows": len(roster_rows),
            "roster_missing_person_source_id": missing_person_source_id,
            "roster_unknown_person_mapping": unknown_person_mapping,
            "roster_unknown_team_mapping": unknown_team_mapping,
            "quarantined_records": len(quarantined_records),
            "source_mapping_conflicts": len(source_mapping_conflicts),
            "unsafe_source_mapping_collisions": unsafe_mapping_collisions,
            "ambiguous_normalized_aliases": sum(
                len(owner_ids) > 1 for owner_ids in alias_owners.values()
            ),
        },
        content_sha256=content_sha256,
    )


def fetch_snapshot(
    client: NflverseClient,
    *,
    season: int,
    now: datetime | None = None,
) -> NflverseSnapshot:
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    return normalize_snapshot(
        client.fetch(season),
        season=season,
        observed_at=observed_at,
    )


def build_object_path(observed_at: datetime, ingest_run_id: str) -> str:
    utc = observed_at.astimezone(UTC)
    return (
        f"raw/provider={STORAGE_PROVIDER}/source={STORAGE_SOURCE}/"
        f"object={STORAGE_OBJECT}/schema=v{SCHEMA_VERSION}/"
        f"date={utc:%Y-%m-%d}/hour={utc:%H}/"
        f"nflverse_entities_{ingest_run_id}.json.gz"
    )


def build_envelope(
    snapshot: NflverseSnapshot,
    *,
    ingest_run_id: str,
    storage_uri: str,
) -> dict[str, Any]:
    return {
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "provider": STORAGE_PROVIDER,
        "source": STORAGE_SOURCE,
        "object_type": STORAGE_OBJECT,
        "ingest_run_id": ingest_run_id,
        "ingested_at": snapshot.observed_at.isoformat(),
        "storage_uri": storage_uri,
        "content_sha256": snapshot.content_sha256,
        "record_count": len(snapshot.entities) + len(snapshot.relationships),
        "request": {
            "season": snapshot.season,
            "normalizer_version": NFLVERSE_NORMALIZER_VERSION,
            "assets": [
                {
                    "name": asset.name,
                    "url": asset.url,
                    "etag": asset.etag,
                    "sha256": asset.sha256,
                    "asset_id": asset.asset_id,
                    "provider_digest": asset.provider_digest,
                    "source_updated_at": asset.source_updated_at,
                    "byte_size": len(asset.content),
                }
                for asset in snapshot.assets
            ],
        },
        "snapshot": {
            "season": snapshot.season,
            "entity_count": len(snapshot.entities),
            "relationship_count": len(snapshot.relationships),
            "normalization_audit": {
                "quality": snapshot.quality,
                "quarantined_records": list(snapshot.quarantined_records),
                "source_mapping_conflicts": list(snapshot.source_mapping_conflicts),
            },
            "assets": [
                {
                    "name": asset.name,
                    "url": asset.url,
                    "etag": asset.etag,
                    "sha256": asset.sha256,
                    "asset_id": asset.asset_id,
                    "provider_digest": asset.provider_digest,
                    "source_updated_at": asset.source_updated_at,
                    "content_base64": base64.b64encode(asset.content).decode("ascii"),
                }
                for asset in snapshot.assets
            ],
        },
        "_normalized_entities": list(snapshot.entities),
        "_normalized_relationships": list(snapshot.relationships),
    }


def archive_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in envelope.items()
        if not key.startswith("_normalized_")
    }


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return gzip.compress(
        canonical_json_bytes(archive_envelope(envelope)),
        compresslevel=6,
        mtime=0,
    )


def summary(snapshot: NflverseSnapshot) -> dict[str, Any]:
    teams = sum(entity["entity_type"] == EntityType.TEAM for entity in snapshot.entities)
    people = sum(entity["entity_type"] == EntityType.PERSON for entity in snapshot.entities)
    return {
        "season": snapshot.season,
        "normalizer_version": NFLVERSE_NORMALIZER_VERSION,
        "content_sha256": snapshot.content_sha256,
        "teams": teams,
        "people": people,
        "complete_player_history": len(snapshot.complete_player_history),
        "relationships": len(snapshot.relationships),
        "quality": snapshot.quality,
        "assets": [
            {
                "name": asset.name,
                "sha256": asset.sha256,
                "asset_id": asset.asset_id,
                "provider_digest": asset.provider_digest,
                "source_updated_at": asset.source_updated_at,
                "byte_size": len(asset.content),
            }
            for asset in snapshot.assets
        ],
    }
