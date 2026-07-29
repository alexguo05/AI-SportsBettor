"""Current X API recent-search ingestion with replayable GCS output."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import random
import sys
import tempfile
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

import requests
from dotenv import dotenv_values
from google.cloud import storage
from google.oauth2 import service_account

X_RECENT_SEARCH_URL = "https://api.x.com/2/tweets/search/recent"
SCHEMA_NAME = "x_posts"
SCHEMA_VERSION = 3
MAX_ARCHIVED_MEDIA_BYTES = 25 * 1024 * 1024

TWEET_FIELDS = ",".join(
    (
        "id",
        "text",
        "author_id",
        "created_at",
        "conversation_id",
        "lang",
        "possibly_sensitive",
        "public_metrics",
        "entities",
        "attachments",
        "referenced_tweets",
        "edit_history_tweet_ids",
    )
)
EXPANSIONS = ",".join(
    (
        "author_id",
        "attachments.media_keys",
        "referenced_tweets.id",
        "referenced_tweets.id.author_id",
    )
)
USER_FIELDS = "id,name,username,verified,profile_image_url"
MEDIA_FIELDS = ",".join(
    (
        "media_key",
        "type",
        "url",
        "preview_image_url",
        "width",
        "height",
        "alt_text",
        "variants",
        "duration_ms",
    )
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_x_timestamp(value: str | datetime | None) -> datetime | None:
    if not value:
        return None
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(value.replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format_x_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


@dataclass(frozen=True)
class XConfig:
    handles: tuple[str, ...]
    max_results: int = 100
    poll_interval_seconds: int = 30
    bucket_name: str = "ai-sports-bettor"
    query_max_characters: int = 512
    max_pages_per_batch: int = 10
    recovery_lookback_hours: int = 24
    checkpoint_max_age_hours: int = 144
    download_media: bool = True

    def __post_init__(self) -> None:
        if not self.handles:
            raise ValueError("x_base_handles must contain at least one handle")
        if any(not handle.strip().lstrip("@") for handle in self.handles):
            raise ValueError("x_base_handles cannot contain blank handles")
        if not 10 <= self.max_results <= 100:
            raise ValueError("tweet_max_results must be between 10 and 100")
        if self.poll_interval_seconds < 1:
            raise ValueError("x_poll_interval_sec must be positive")
        if self.query_max_characters < 64:
            raise ValueError("x_query_max_characters is too small")
        if self.max_pages_per_batch < 1:
            raise ValueError("x_max_pages_per_batch must be positive")
        if not 1 <= self.recovery_lookback_hours <= 168:
            raise ValueError("x_recovery_lookback_hours must be between 1 and 168")
        if not 1 <= self.checkpoint_max_age_hours <= 168:
            raise ValueError("x_checkpoint_max_age_hours must be between 1 and 168")


@dataclass(frozen=True)
class SearchCursor:
    since_id: str | None = None
    start_time: str | None = None
    recovery_reason: str | None = None

    def as_params(self) -> dict[str, str]:
        if self.since_id:
            return {"since_id": self.since_id}
        if self.start_time:
            return {"start_time": self.start_time}
        return {}


@dataclass(frozen=True)
class Checkpoint:
    since_id: str | None = None
    query_fingerprint: str | None = None
    updated_at: datetime | None = None
    last_successful_poll_at: datetime | None = None

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> Checkpoint:
        return cls(
            since_id=str(payload["since_id"]) if payload.get("since_id") else None,
            query_fingerprint=payload.get("query_fingerprint"),
            updated_at=parse_x_timestamp(payload.get("updated_at")),
            last_successful_poll_at=parse_x_timestamp(payload.get("last_successful_poll_at")),
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_name": "x_recent_search_checkpoint",
            "schema_version": 1,
            "since_id": self.since_id,
            "query_fingerprint": self.query_fingerprint,
            "updated_at": (
                self.updated_at.astimezone(UTC).isoformat() if self.updated_at else None
            ),
            "last_successful_poll_at": (
                self.last_successful_poll_at.astimezone(UTC).isoformat()
                if self.last_successful_poll_at
                else None
            ),
        }


class CycleRepository(Protocol):
    def persist_records(self, envelope: dict[str, Any]) -> None: ...

    def finalize_cycle(
        self,
        *,
        records: list[dict[str, Any]],
        checkpoint: dict[str, Any],
    ) -> None: ...


@dataclass
class SearchResult:
    query: str
    api_pages: list[dict[str, Any]] = field(default_factory=list)
    request_pages: list[dict[str, Any]] = field(default_factory=list)

    @property
    def posts(self) -> list[dict[str, Any]]:
        by_id: dict[str, dict[str, Any]] = {}
        for page in self.api_pages:
            for post in page.get("data", []) or []:
                post_id = post.get("id")
                if post_id:
                    by_id[str(post_id)] = post
        return list(by_id.values())

    @property
    def newest_id(self) -> str | None:
        ids = [post["id"] for post in self.posts if str(post.get("id", "")).isdigit()]
        return max(ids, key=int) if ids else None


def load_config(path: Path) -> XConfig:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    return XConfig(
        handles=tuple(dict.fromkeys(payload.get("x_base_handles", []))),
        max_results=int(payload.get("tweet_max_results", 100)),
        poll_interval_seconds=int(payload.get("x_poll_interval_sec", 10)),
        bucket_name=payload.get("gcs_bucket", "ai-sports-bettor"),
        query_max_characters=int(payload.get("x_query_max_characters", 512)),
        max_pages_per_batch=int(payload.get("x_max_pages_per_batch", 10)),
        recovery_lookback_hours=int(payload.get("x_recovery_lookback_hours", 24)),
        checkpoint_max_age_hours=int(payload.get("x_checkpoint_max_age_hours", 144)),
        download_media=bool(payload.get("x_download_media", True)),
    )


def load_bearer_token(src_dir: Path) -> str:
    dotenv_path = src_dir / ".env"
    dotenv = dotenv_values(dotenv_path) if dotenv_path.exists() else {}
    token = (
        os.getenv("X_BEARER_TOKEN")
        or os.getenv("BEARER_TOKEN")
        or dotenv.get("X_BEARER_TOKEN")
        or dotenv.get("BEARER_TOKEN")
    )
    if not token:
        raise ValueError("X_BEARER_TOKEN is not configured")
    return str(token)


def create_gcs_client(src_dir: Path) -> storage.Client:
    credentials_path = src_dir / "ai-sports-bettor-559e8837739f.json"
    configured_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if configured_path:
        return storage.Client()
    if credentials_path.exists():
        credentials = service_account.Credentials.from_service_account_file(str(credentials_path))
        return storage.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
    return storage.Client()


def build_query_batches(config: XConfig) -> list[str]:
    suffix = " -is:reply"
    clauses = [f"from:{handle.lstrip('@')}" for handle in config.handles]
    queries: list[str] = []
    current: list[str] = []
    for clause in clauses:
        tentative = " OR ".join([*current, clause]) + suffix
        if len(tentative) <= config.query_max_characters:
            current.append(clause)
            continue
        if not current:
            raise ValueError(f"handle clause exceeds query limit: {clause}")
        queries.append(" OR ".join(current) + suffix)
        current = [clause]
    if current:
        queries.append(" OR ".join(current) + suffix)
    return queries


def fingerprint_queries(queries: Iterable[str]) -> str:
    value = "\n".join((TWEET_FIELDS, EXPANSIONS, USER_FIELDS, MEDIA_FIELDS, *queries))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def choose_cursor(
    checkpoint: Checkpoint,
    query_fingerprint: str,
    config: XConfig,
    now: datetime,
) -> SearchCursor:
    checkpoint_is_current = (
        checkpoint.since_id
        and checkpoint.query_fingerprint == query_fingerprint
        and checkpoint.updated_at is not None
        and checkpoint.updated_at
        >= now.astimezone(UTC) - timedelta(hours=config.checkpoint_max_age_hours)
    )
    if checkpoint_is_current:
        return SearchCursor(since_id=checkpoint.since_id)
    if checkpoint.since_id and checkpoint.query_fingerprint != query_fingerprint:
        reason = "query_changed"
    elif checkpoint.since_id:
        reason = "checkpoint_stale_or_legacy"
    else:
        reason = "checkpoint_missing"
    start = now.astimezone(UTC) - timedelta(hours=config.recovery_lookback_hours)
    return SearchCursor(
        start_time=format_x_timestamp(start),
        recovery_reason=reason,
    )


class XRecentSearchClient:
    def __init__(
        self,
        bearer_token: str,
        *,
        session: requests.Session | None = None,
        timeout_seconds: float = 30,
        max_attempts: int = 5,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[float, float], float] | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.session = session or requests.Session()
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.sleep = sleep
        self.jitter = jitter or random.uniform
        self.headers = {
            "Authorization": f"Bearer {bearer_token}",
            "User-Agent": "ai-sports-bettor-x-ingest/1.0",
        }

    def _retry_delay(self, response: requests.Response | None, attempt: int) -> float:
        exponential_delay = min(30.0, float(2**attempt))
        header_delay = 0.0
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    header_delay = max(0.0, float(retry_after))
                except ValueError:
                    pass
            if response.status_code == 429 and not header_delay:
                reset = response.headers.get("x-rate-limit-reset")
                if reset:
                    try:
                        header_delay = max(0.0, float(reset) - time.time())
                    except ValueError:
                        pass
        return max(header_delay, exponential_delay + self.jitter(0.0, 1.0))

    def _request_page(
        self,
        params: dict[str, str],
        page_number: int,
    ) -> tuple[requests.Response, int]:
        retryable_statuses = {429, 500, 502, 503, 504}
        for attempt in range(1, self.max_attempts + 1):
            response: requests.Response | None = None
            try:
                response = self.session.get(
                    X_RECENT_SEARCH_URL,
                    params=params,
                    headers=self.headers,
                    timeout=self.timeout_seconds,
                )
                if response.status_code not in retryable_statuses:
                    response.raise_for_status()
                    return response, attempt
                if attempt == self.max_attempts:
                    response.raise_for_status()
            except (requests.ConnectionError, requests.Timeout):
                if attempt == self.max_attempts:
                    raise
            delay = self._retry_delay(response, attempt)
            status = response.status_code if response is not None else "network error"
            print(
                f"WARNING: X page {page_number} returned {status}; "
                f"retrying attempt {attempt + 1}/{self.max_attempts} in {delay:.1f}s",
                file=sys.stderr,
            )
            self.sleep(delay)
        raise RuntimeError("unreachable X retry state")

    def search(
        self,
        query: str,
        cursor: SearchCursor,
        *,
        max_results: int,
        max_pages: int,
    ) -> SearchResult:
        base_params = {
            "query": query,
            "max_results": str(max_results),
            "sort_order": "recency",
            "tweet.fields": TWEET_FIELDS,
            "expansions": EXPANSIONS,
            "user.fields": USER_FIELDS,
            "media.fields": MEDIA_FIELDS,
            **cursor.as_params(),
        }
        result = SearchResult(query=query)
        pagination_token: str | None = None
        for page_number in range(1, max_pages + 1):
            params = dict(base_params)
            if pagination_token:
                params["pagination_token"] = pagination_token
            response, attempts = self._request_page(params, page_number)
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("X API returned a non-object response")
            page_post_count = len(payload.get("data", []) or [])
            print(
                f"Fetched X page {page_number}: {page_post_count} posts "
                f"(request attempts: {attempts})"
            )
            result.api_pages.append(payload)
            result.request_pages.append(
                {
                    "page_number": page_number,
                    "params": {
                        key: value for key, value in params.items() if key != "pagination_token"
                    },
                    "used_pagination_token": bool(pagination_token),
                    "attempts": attempts,
                    "rate_limit": {
                        "limit": response.headers.get("x-rate-limit-limit"),
                        "remaining": response.headers.get("x-rate-limit-remaining"),
                        "reset": response.headers.get("x-rate-limit-reset"),
                    },
                }
            )
            pagination_token = (payload.get("meta") or {}).get("next_token")
            if not pagination_token:
                break
        if pagination_token:
            raise RuntimeError(
                f"X result exceeded x_max_pages_per_batch={max_pages}; checkpoint was not advanced"
            )
        return result


def _index_includes(
    search_results: Iterable[SearchResult],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    users: dict[str, dict[str, Any]] = {}
    media: dict[str, dict[str, Any]] = {}
    referenced_posts: dict[str, dict[str, Any]] = {}
    for result in search_results:
        for page in result.api_pages:
            includes = page.get("includes") or {}
            for user in includes.get("users", []) or []:
                if user.get("id"):
                    users[str(user["id"])] = user
            for item in includes.get("media", []) or []:
                if item.get("media_key"):
                    media[str(item["media_key"])] = item
            for post in includes.get("tweets", []) or []:
                if post.get("id"):
                    referenced_posts[str(post["id"])] = post
    return users, media, referenced_posts


def _original_media_url(media: dict[str, Any]) -> str | None:
    if media.get("type") == "photo":
        return media.get("url")
    variants = [
        variant
        for variant in media.get("variants", []) or []
        if variant.get("content_type") == "video/mp4" and variant.get("url")
    ]
    if variants:
        return max(variants, key=lambda item: item.get("bit_rate", 0)).get("url")
    return media.get("preview_image_url") or media.get("url")


def _selected_media_asset(media: dict[str, Any]) -> tuple[str | None, str]:
    media_type = media.get("type")
    if media_type == "photo":
        return media.get("url"), "original_image"
    if media_type in {"video", "animated_gif"}:
        return media.get("preview_image_url"), "video_preview"
    return (
        media.get("preview_image_url") or media.get("url"),
        "preview" if media.get("preview_image_url") else "original",
    )


def _media_extension(url: str, content_type: str | None) -> str:
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix and len(suffix) <= 8:
        return suffix
    return {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "video/mp4": ".mp4",
    }.get((content_type or "").split(";")[0].lower(), ".bin")


def upload_media(
    bucket: Any,
    bucket_name: str,
    post_id: str,
    media: dict[str, Any],
    *,
    session: requests.Session,
    max_bytes: int = MAX_ARCHIVED_MEDIA_BYTES,
) -> dict[str, Any]:
    selected_url = media.get("selected_source_url")
    media_key = media.get("media_key")
    base_result = {
        "gcs_uri": None,
        "content_type": None,
        "content_sha256": None,
        "byte_size": None,
        "upload_status": "not_available",
        "upload_error": None,
    }
    if not selected_url or not media_key:
        return {
            **base_result,
            "upload_status": (
                "skipped_no_preview"
                if media.get("media_type") in {"video", "animated_gif"}
                else "not_available"
            ),
        }
    expected_content_type = (
        "image/jpeg" if media.get("stored_asset_kind") == "video_preview" else None
    )
    extension = _media_extension(selected_url, expected_content_type)
    object_path = f"raw/media-schema=v1/source=x/post_id={post_id}/{media_key}{extension}"
    blob = bucket.blob(object_path)
    if blob.exists():
        print(f"Media {media_key} for X post {post_id} already exists; skipping download")
        return {
            **base_result,
            "gcs_uri": f"gs://{bucket_name}/{object_path}",
            "content_type": getattr(blob, "content_type", None),
            "byte_size": getattr(blob, "size", None),
            "upload_status": "already_exists",
        }
    try:
        response = session.get(selected_url, stream=True, timeout=60)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type")
        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > max_bytes:
            return {
                **base_result,
                "content_type": content_type,
                "byte_size": int(content_length),
                "upload_status": "skipped_too_large",
            }
        digest = hashlib.sha256()
        byte_size = 0
        with tempfile.NamedTemporaryFile(prefix="x_media_", delete=True) as temp:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                byte_size += len(chunk)
                if byte_size > max_bytes:
                    return {
                        **base_result,
                        "content_type": content_type,
                        "byte_size": byte_size,
                        "upload_status": "skipped_too_large",
                    }
                digest.update(chunk)
                temp.write(chunk)
            temp.flush()
            temp.seek(0)
            blob.upload_from_file(temp, content_type=content_type)
        print(f"Stored media {media_key} for X post {post_id}")
        return {
            **base_result,
            "gcs_uri": f"gs://{bucket_name}/{object_path}",
            "content_type": content_type,
            "content_sha256": digest.hexdigest(),
            "byte_size": byte_size,
            "upload_status": "uploaded",
        }
    except Exception as exc:
        print(
            f"WARNING: failed to store media {media.get('media_key')} for X post {post_id}: {exc}",
            file=sys.stderr,
        )
        return {
            **base_result,
            "upload_status": "failed",
            "upload_error": f"{type(exc).__name__}: {exc}",
        }


def normalize_posts(
    search_results: list[SearchResult],
    observed_at: datetime,
) -> list[dict[str, Any]]:
    users, media_index, referenced_posts = _index_includes(search_results)
    source_posts = dict(referenced_posts)
    for result in search_results:
        for post in result.posts:
            source_posts[str(post["id"])] = post

    posts_by_id: dict[str, dict[str, Any]] = {}
    for post_id, post in source_posts.items():
        author = users.get(str(post.get("author_id")), {})
        username = author.get("username")
        attachments = post.get("attachments") or {}
        media_records = []
        for media_key in attachments.get("media_keys", []) or []:
            media = media_index.get(str(media_key), {"media_key": media_key})
            selected_url, stored_asset_kind = _selected_media_asset(media)
            media_record = {
                "media_key": media.get("media_key"),
                "media_type": media.get("type"),
                "source_url": _original_media_url(media),
                "preview_image_url": media.get("preview_image_url"),
                "selected_source_url": selected_url,
                "stored_asset_kind": stored_asset_kind,
                "width": media.get("width"),
                "height": media.get("height"),
                "duration_ms": media.get("duration_ms"),
                "alt_text": media.get("alt_text"),
                "gcs_uri": None,
                "content_type": None,
                "content_sha256": None,
                "byte_size": None,
                "upload_status": "pending",
                "upload_error": None,
            }
            media_records.append(media_record)
        published_at = parse_x_timestamp(post.get("created_at"))
        relationships = [
            {
                "relationship_type": reference.get("type"),
                "target_news_id": f"x:{reference['id']}",
                "target_source_post_id": str(reference["id"]),
                "target_available": str(reference["id"]) in source_posts,
            }
            for reference in post.get("referenced_tweets", []) or []
            if reference.get("id") and reference.get("type")
        ]
        posts_by_id[post_id] = {
            "news_id": f"x:{post_id}",
            "source": "x",
            "source_post_id": post_id,
            "source_url": (
                f"https://x.com/{username}/status/{post_id}"
                if username
                else f"https://x.com/i/web/status/{post_id}"
            ),
            "author": {
                "source_user_id": post.get("author_id"),
                "username": username,
                "display_name": author.get("name"),
                "verified": author.get("verified"),
                "profile_image_url": author.get("profile_image_url"),
            },
            "text": post.get("text", ""),
            "language": post.get("lang"),
            "conversation_id": post.get("conversation_id"),
            "published_at": published_at.isoformat() if published_at else None,
            "ingested_at": observed_at.astimezone(UTC).isoformat(),
            "possibly_sensitive": post.get("possibly_sensitive"),
            "public_metrics": post.get("public_metrics") or {},
            "source_entities": post.get("entities") or {},
            "edit_history_post_ids": post.get("edit_history_tweet_ids") or [],
            "relationships": relationships,
            "media": media_records,
        }
    return sorted(
        posts_by_id.values(),
        key=lambda item: (item["published_at"] or "", item["source_post_id"]),
    )


def max_post_id(values: Iterable[str | None]) -> str | None:
    ids = [str(value) for value in values if value and str(value).isdigit()]
    return max(ids, key=int) if ids else None


def build_object_path(ingested_at: datetime, ingest_run_id: str) -> str:
    utc = ingested_at.astimezone(UTC)
    return (
        f"raw/schema=v{SCHEMA_VERSION}/source=x/posts/"
        f"date={utc:%Y-%m-%d}/hour={utc:%H}/"
        f"x_posts_{ingest_run_id}.json.gz"
    )


def build_envelope(
    *,
    ingest_run_id: str,
    ingested_at: datetime,
    storage_uri: str,
    query_fingerprint: str,
    cursor: SearchCursor,
    search_results: list[SearchResult],
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    raw_pages = [
        {
            "query": result.query,
            "pages": result.api_pages,
            "requests": result.request_pages,
        }
        for result in search_results
    ]
    payload_hash = hashlib.sha256(canonical_json_bytes(raw_pages)).hexdigest()
    return {
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "source": "x",
        "object_type": "news_posts",
        "ingest_run_id": ingest_run_id,
        "ingested_at": ingested_at.astimezone(UTC).isoformat(),
        "storage_uri": storage_uri,
        "content_sha256": payload_hash,
        "record_count": len(records),
        "request": {
            "endpoint": X_RECENT_SEARCH_URL,
            "query_fingerprint": query_fingerprint,
            "queries": [result.query for result in search_results],
            "cursor": {
                "since_id": cursor.since_id,
                "start_time": cursor.start_time,
                "recovery_reason": cursor.recovery_reason,
            },
            "page_count": sum(len(result.api_pages) for result in search_results),
        },
        "checkpoint_candidate": {
            "newest_id": max_post_id(result.newest_id for result in search_results),
        },
        "records": records,
        "raw_api_responses": raw_pages,
    }


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return gzip.compress(canonical_json_bytes(envelope), compresslevel=6, mtime=0)


def decode_envelope(data: bytes) -> dict[str, Any]:
    payload = json.loads(gzip.decompress(data))
    if payload.get("schema_name") != SCHEMA_NAME or payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported X envelope schema")
    return payload


def run_cycle(
    *,
    config: XConfig,
    client: XRecentSearchClient,
    bucket: Any,
    repository: CycleRepository,
    checkpoint: Checkpoint,
    now: datetime | None = None,
) -> Checkpoint:
    cycle_started_at = (now or utc_now()).astimezone(UTC)
    queries = build_query_batches(config)
    query_fingerprint = fingerprint_queries(queries)
    cursor = choose_cursor(checkpoint, query_fingerprint, config, cycle_started_at)
    cursor_description = (
        f"since_id={cursor.since_id}"
        if cursor.since_id
        else f"start_time={cursor.start_time} ({cursor.recovery_reason})"
    )
    print(f"Starting X poll cycle with {cursor_description}")
    search_results: list[SearchResult] = []
    for index, query in enumerate(queries):
        batch_number = index + 1
        print(f"Fetching X batch {batch_number}/{len(queries)}")
        result = client.search(
            query,
            cursor,
            max_results=config.max_results,
            max_pages=config.max_pages_per_batch,
        )
        search_results.append(result)
        print(
            f"Completed X batch {batch_number}/{len(queries)}: "
            f"{len(result.posts)} unique posts across {len(result.api_pages)} pages"
        )

    root_post_count = len({str(post["id"]) for result in search_results for post in result.posts})
    records = normalize_posts(search_results, cycle_started_at)
    total_media = sum(len(record.get("media", [])) for record in records)
    print(
        f"Normalized {root_post_count} matched and "
        f"{len(records) - root_post_count} expanded X posts with "
        f"{total_media} media metadata records"
    )
    newest_id = max_post_id(
        [
            checkpoint.since_id,
            *(result.newest_id for result in search_results),
        ]
    )
    if records:
        ingest_run_id = uuid.uuid4().hex
        object_path = build_object_path(cycle_started_at, ingest_run_id)
        storage_uri = f"gs://{config.bucket_name}/{object_path}"
        for record in records:
            record["raw_gcs_uri"] = storage_uri
        envelope = build_envelope(
            ingest_run_id=ingest_run_id,
            ingested_at=cycle_started_at,
            storage_uri=storage_uri,
            query_fingerprint=query_fingerprint,
            cursor=cursor,
            search_results=search_results,
            records=records,
        )
        blob = bucket.blob(object_path)
        blob.metadata = {
            "schema_name": SCHEMA_NAME,
            "schema_version": str(SCHEMA_VERSION),
            "content_sha256": envelope["content_sha256"],
            "record_count": str(len(records)),
        }
        blob.content_encoding = "gzip"
        blob.upload_from_string(
            encode_envelope(envelope),
            content_type="application/json",
        )
        print(f"Uploaded {len(records)} raw X posts to {storage_uri}")
        repository.persist_records(envelope)
        print(
            f"Committed {len(records)} X posts and pending media metadata "
            "to PostgreSQL; cursor remains unchanged"
        )

        media_session = requests.Session()
        print(f"Processing {total_media} media attachments")
        for record in records:
            for media in record.get("media", []):
                if config.download_media:
                    result = upload_media(
                        bucket,
                        config.bucket_name,
                        record["source_post_id"],
                        media,
                        session=media_session,
                    )
                else:
                    result = {
                        "gcs_uri": None,
                        "content_type": None,
                        "content_sha256": None,
                        "byte_size": None,
                        "upload_status": "disabled",
                        "upload_error": None,
                    }
                media.update(result)
    else:
        print("X poll cycle complete. No new posts found.")

    updated_checkpoint = Checkpoint(
        since_id=newest_id,
        query_fingerprint=query_fingerprint,
        updated_at=cycle_started_at,
        last_successful_poll_at=cycle_started_at,
    )
    repository.finalize_cycle(
        records=records,
        checkpoint=updated_checkpoint.to_json(),
    )
    if records:
        print("Finalized media statuses and advanced the PostgreSQL cursor")
    return updated_checkpoint


def main() -> int:
    from src.db.repository import NewsRepository

    src_dir = Path(__file__).resolve().parents[1]
    config_path = src_dir / "config" / "x_config.json"
    repository: NewsRepository | None = None
    try:
        config = load_config(config_path)
        token = load_bearer_token(src_dir)
        gcs_client = create_gcs_client(src_dir)
        bucket = gcs_client.bucket(config.bucket_name)
        repository = NewsRepository.from_environment(src_dir)
        checkpoint = Checkpoint.from_json(repository.load_checkpoint())
    except Exception as exc:
        if repository:
            repository.close()
        print(f"ERROR: failed to initialize X ingestion: {exc}", file=sys.stderr)
        return 1

    queries = build_query_batches(config)
    print(
        "Starting X recent-search poller. "
        f"Batches: {len(queries)}. Cycle interval: "
        f"{config.poll_interval_seconds}s. Bucket: {config.bucket_name}"
    )
    client = XRecentSearchClient(token)
    while True:
        try:
            checkpoint = run_cycle(
                config=config,
                client=client,
                bucket=bucket,
                repository=repository,
                checkpoint=checkpoint,
            )
        except KeyboardInterrupt:
            repository.close()
            print("Stopping X ingestion.")
            return 0
        except Exception as exc:
            print(
                f"ERROR: X poll cycle failed without advancing checkpoint: {exc}",
                file=sys.stderr,
            )
        time.sleep(config.poll_interval_seconds)
