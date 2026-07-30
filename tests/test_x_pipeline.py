from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import requests

from src.ingest_news.x_pipeline import (
    Checkpoint,
    SearchCursor,
    SearchResult,
    XConfig,
    XRecentSearchClient,
    build_media_object_path,
    build_object_path,
    build_query_batches,
    choose_cursor,
    decode_envelope,
    fingerprint_queries,
    normalize_posts,
    run_cycle,
    upload_media,
)

NOW = datetime(2026, 7, 29, 18, 0, tzinfo=UTC)


class FakeResponse:
    def __init__(self, payload: dict[str, Any], *, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers = {
            "x-rate-limit-limit": "450",
            "x-rate-limit-remaining": "449",
        }

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} response")

    def json(self) -> dict[str, Any]:
        return self.payload


class FakeSession:
    def __init__(self, responses: list[dict[str, Any] | FakeResponse]) -> None:
        self.responses = iter(responses)
        self.calls: list[dict[str, Any]] = []

    def get(self, _url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append(kwargs)
        response = next(self.responses)
        return response if isinstance(response, FakeResponse) else FakeResponse(response)


class FakeBlob:
    def __init__(self, name: str, *, fail_upload: bool = False) -> None:
        self.name = name
        self.fail_upload = fail_upload
        self.data: bytes | str | None = None
        self.content_encoding: str | None = None
        self.metadata: dict[str, str] | None = None

    def exists(self) -> bool:
        return self.data is not None

    def upload_from_string(
        self,
        data: bytes | str,
        *,
        content_type: str,
        content_encoding: str | None = None,
    ) -> None:
        del content_type
        if self.fail_upload:
            raise OSError("simulated GCS failure")
        self.data = data
        self.content_encoding = content_encoding

    def download_as_text(self) -> str:
        assert isinstance(self.data, str)
        return self.data


class FakeBucket:
    def __init__(self, *, fail_envelope_upload: bool = False) -> None:
        self.fail_envelope_upload = fail_envelope_upload
        self.blobs: dict[str, FakeBlob] = {}

    def blob(self, name: str) -> FakeBlob:
        if name not in self.blobs:
            self.blobs[name] = FakeBlob(
                name,
                fail_upload=self.fail_envelope_upload and name.endswith(".json.gz"),
            )
        return self.blobs[name]


class FakeClient:
    def __init__(self, result: SearchResult) -> None:
        self.result = result
        self.cursors: list[SearchCursor] = []

    def search(
        self,
        _query: str,
        cursor: SearchCursor,
        *,
        max_results: int,
        max_pages: int,
    ) -> SearchResult:
        del max_results, max_pages
        self.cursors.append(cursor)
        return self.result


class FakeRepository:
    def __init__(self, *, fail_records: bool = False, fail_finalize: bool = False) -> None:
        self.fail_records = fail_records
        self.fail_finalize = fail_finalize
        self.envelopes: list[dict[str, Any]] = []
        self.finalized: list[dict[str, Any]] = []
        self.calls: list[str] = []

    def persist_records(self, envelope: dict[str, Any]) -> None:
        if self.fail_records:
            raise OSError("simulated PostgreSQL failure")
        self.calls.append("records")
        self.envelopes.append(envelope)

    def finalize_cycle(
        self,
        *,
        records: list[dict[str, Any]],
        checkpoint: dict[str, Any],
    ) -> None:
        if self.fail_finalize:
            raise OSError("simulated PostgreSQL finalization failure")
        self.calls.append("cursor")
        self.finalized.append({"records": records, "checkpoint": checkpoint})


def sample_result() -> SearchResult:
    return SearchResult(
        query="from:Reporter -is:reply",
        api_pages=[
            {
                "data": [
                    {
                        "id": "200",
                        "text": "Player left practice.",
                        "author_id": "10",
                        "created_at": "2026-07-29T17:59:00Z",
                        "lang": "en",
                    }
                ],
                "includes": {
                    "users": [
                        {
                            "id": "10",
                            "username": "Reporter",
                            "name": "Reporter Name",
                        }
                    ]
                },
                "meta": {"newest_id": "200", "result_count": 1},
            }
        ],
    )


def sample_video_result() -> SearchResult:
    return SearchResult(
        query="from:Reporter",
        api_pages=[
            {
                "data": [
                    {
                        "id": "200",
                        "text": "Practice update.",
                        "author_id": "10",
                        "created_at": "2026-07-29T17:59:00Z",
                        "attachments": {"media_keys": ["13_video"]},
                    }
                ],
                "includes": {
                    "users": [{"id": "10", "username": "Reporter"}],
                    "media": [
                        {
                            "media_key": "13_video",
                            "type": "video",
                            "preview_image_url": "https://example.test/preview.jpg",
                            "variants": [
                                {
                                    "content_type": "video/mp4",
                                    "bit_rate": 1000000,
                                    "url": "https://example.test/full-video.mp4",
                                }
                            ],
                            "duration_ms": 120000,
                        }
                    ],
                },
                "meta": {"newest_id": "200"},
            }
        ],
    )


def sample_repost_result(*, include_original: bool = True) -> SearchResult:
    includes: dict[str, Any] = {
        "users": [
            {"id": "10", "username": "Reporter"},
            {"id": "20", "username": "OriginalReporter"},
        ]
    }
    if include_original:
        includes["tweets"] = [
            {
                "id": "100",
                "text": "Player has been ruled out.",
                "author_id": "20",
                "created_at": "2026-07-29T17:58:00Z",
            }
        ]
    return SearchResult(
        query="from:Reporter -is:reply",
        api_pages=[
            {
                "data": [
                    {
                        "id": "200",
                        "text": "RT @OriginalReporter: Player has been ruled out.",
                        "author_id": "10",
                        "created_at": "2026-07-29T17:59:00Z",
                        "referenced_tweets": [{"type": "retweeted", "id": "100"}],
                    }
                ],
                "includes": includes,
                "meta": {"newest_id": "200"},
            }
        ],
    )


def test_repost_normalizes_expanded_original_and_relationship() -> None:
    records = normalize_posts([sample_repost_result()], NOW)
    by_id = {record["source_post_id"]: record for record in records}

    assert set(by_id) == {"100", "200"}
    assert by_id["100"]["author"]["username"] == "OriginalReporter"
    assert by_id["100"]["text"] == "Player has been ruled out."
    assert by_id["200"]["relationships"] == [
        {
            "relationship_type": "retweeted",
            "target_news_id": "x:100",
            "target_source_post_id": "100",
            "target_available": True,
        }
    ]


def test_repost_preserves_unavailable_original_id() -> None:
    records = normalize_posts([sample_repost_result(include_original=False)], NOW)

    assert len(records) == 1
    assert records[0]["relationships"][0]["target_news_id"] == "x:100"
    assert records[0]["relationships"][0]["target_available"] is False


def test_video_metadata_selects_preview_instead_of_full_video() -> None:
    records = normalize_posts([sample_video_result()], NOW)
    media = records[0]["media"][0]

    assert media["media_type"] == "video"
    assert media["source_url"] == "https://example.test/full-video.mp4"
    assert media["selected_source_url"] == "https://example.test/preview.jpg"
    assert media["stored_asset_kind"] == "video_preview"
    assert media["upload_status"] == "pending"


def test_existing_media_is_skipped_before_download() -> None:
    bucket = FakeBucket()
    object_path = (
        "raw/provider=x/source=recent-search/object=media/schema=v1/"
        "post_id=200/13_video.jpg"
    )
    bucket.blob(object_path).data = b"existing"
    session = FakeSession([])
    media = normalize_posts([sample_video_result()], NOW)[0]["media"][0]

    result = upload_media(
        bucket,
        "bucket",
        "200",
        media,
        session=session,  # type: ignore[arg-type]
    )

    assert result["upload_status"] == "already_exists"
    assert result["gcs_uri"] == f"gs://bucket/{object_path}"
    assert session.calls == []


def test_existing_legacy_media_is_not_uploaded_again() -> None:
    bucket = FakeBucket()
    legacy_path = "raw/media-schema=v1/source=x/post_id=200/13_video.jpg"
    bucket.blob(legacy_path).data = b"existing"
    session = FakeSession([])
    media = normalize_posts([sample_video_result()], NOW)[0]["media"][0]

    result = upload_media(
        bucket,
        "bucket",
        "200",
        media,
        session=session,  # type: ignore[arg-type]
    )

    assert result["upload_status"] == "already_exists"
    assert result["gcs_uri"] == f"gs://bucket/{legacy_path}"
    assert session.calls == []


def test_storage_paths_group_by_provider_source_object_and_schema() -> None:
    assert build_object_path(NOW, "run-id") == (
        "raw/provider=x/source=recent-search/object=posts/schema=v3/"
        "date=2026-07-29/hour=18/x_posts_run-id.json.gz"
    )
    assert build_media_object_path("200", "13_video", ".jpg") == (
        "raw/provider=x/source=recent-search/object=media/schema=v1/"
        "post_id=200/13_video.jpg"
    )


def test_stale_or_changed_checkpoint_uses_bounded_recovery_window() -> None:
    config = XConfig(handles=("Reporter",), recovery_lookback_hours=24)
    queries = build_query_batches(config)
    fingerprint = fingerprint_queries(queries)

    stale = Checkpoint(
        since_id="100",
        query_fingerprint=fingerprint,
        updated_at=NOW - timedelta(days=7),
    )
    cursor = choose_cursor(stale, fingerprint, config, NOW)

    assert cursor.since_id is None
    assert cursor.start_time == "2026-07-28T18:00:00Z"
    assert cursor.recovery_reason == "checkpoint_stale_or_legacy"


def test_current_checkpoint_uses_since_id() -> None:
    config = XConfig(handles=("Reporter",))
    fingerprint = fingerprint_queries(build_query_batches(config))
    checkpoint = Checkpoint(
        since_id="100",
        query_fingerprint=fingerprint,
        updated_at=NOW,
    )

    assert choose_cursor(checkpoint, fingerprint, config, NOW) == SearchCursor(since_id="100")


def test_checkpoint_accepts_postgresql_datetime_values() -> None:
    checkpoint = Checkpoint.from_json(
        {
            "since_id": "100",
            "query_fingerprint": "fingerprint",
            "updated_at": NOW,
            "last_successful_poll_at": NOW,
        }
    )

    assert checkpoint.updated_at == NOW
    assert checkpoint.last_successful_poll_at == NOW


def test_recent_search_paginates_with_same_since_id() -> None:
    session = FakeSession(
        [
            {"data": [{"id": "200"}], "meta": {"next_token": "page-2"}},
            {"data": [{"id": "199"}], "meta": {}},
        ]
    )
    client = XRecentSearchClient("token", session=session)  # type: ignore[arg-type]

    result = client.search(
        "from:Reporter",
        SearchCursor(since_id="100"),
        max_results=100,
        max_pages=2,
    )

    assert {post["id"] for post in result.posts} == {"199", "200"}
    assert session.calls[0]["params"]["since_id"] == "100"
    assert "referenced_tweets" in session.calls[0]["params"]["tweet.fields"]
    assert "referenced_tweets.id" in session.calls[0]["params"]["expansions"]
    assert "pagination_token" not in session.calls[0]["params"]
    assert session.calls[1]["params"]["since_id"] == "100"
    assert session.calls[1]["params"]["pagination_token"] == "page-2"


def test_transient_page_failure_retries_same_pagination_token() -> None:
    session = FakeSession(
        [
            {"data": [{"id": "200"}], "meta": {"next_token": "page-2"}},
            FakeResponse({}, status_code=503),
            {"data": [{"id": "199"}], "meta": {}},
        ]
    )
    delays: list[float] = []
    client = XRecentSearchClient(
        "token",
        session=session,  # type: ignore[arg-type]
        sleep=delays.append,
        jitter=lambda _minimum, _maximum: 0.0,
    )

    result = client.search(
        "from:Reporter",
        SearchCursor(since_id="100"),
        max_results=100,
        max_pages=2,
    )

    assert {post["id"] for post in result.posts} == {"199", "200"}
    assert len(session.calls) == 3
    assert session.calls[1]["params"]["pagination_token"] == "page-2"
    assert session.calls[2]["params"]["pagination_token"] == "page-2"
    assert delays == [2.0]
    assert result.request_pages[1]["attempts"] == 2


def test_page_cap_fails_instead_of_advancing_checkpoint() -> None:
    session = FakeSession([{"data": [{"id": "200"}], "meta": {"next_token": "more"}}])
    client = XRecentSearchClient("token", session=session)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="checkpoint was not advanced"):
        client.search(
            "from:Reporter",
            SearchCursor(since_id="100"),
            max_results=100,
            max_pages=1,
        )


def test_successful_cycle_uploads_envelope_then_checkpoint() -> None:
    config = XConfig(handles=("Reporter",), download_media=False)
    bucket = FakeBucket()
    repository = FakeRepository()

    checkpoint = run_cycle(
        config=config,
        client=FakeClient(sample_result()),  # type: ignore[arg-type]
        bucket=bucket,
        repository=repository,
        checkpoint=Checkpoint(),
        now=NOW,
    )

    envelope_blob = next(blob for name, blob in bucket.blobs.items() if name.endswith(".json.gz"))
    assert isinstance(envelope_blob.data, bytes)
    envelope = decode_envelope(envelope_blob.data)
    record = envelope["records"][0]
    assert envelope["schema_name"] == "x_posts"
    assert envelope["record_count"] == 1
    assert record["news_id"] == "x:200"
    assert record["source_url"] == "https://x.com/Reporter/status/200"
    assert record["raw_gcs_uri"] == envelope["storage_uri"]
    assert checkpoint.since_id == "200"
    assert repository.envelopes[0] == envelope
    assert repository.finalized[0]["checkpoint"]["since_id"] == "200"
    assert repository.calls == ["records", "cursor"]


def test_failed_envelope_upload_does_not_write_checkpoint() -> None:
    config = XConfig(handles=("Reporter",), download_media=False)
    bucket = FakeBucket(fail_envelope_upload=True)

    with pytest.raises(OSError, match="simulated GCS failure"):
        run_cycle(
            config=config,
            client=FakeClient(sample_result()),  # type: ignore[arg-type]
            bucket=bucket,
            repository=FakeRepository(),
            checkpoint=Checkpoint(),
            now=NOW,
        )


def test_failed_database_transaction_does_not_return_advanced_checkpoint() -> None:
    config = XConfig(handles=("Reporter",), download_media=False)
    bucket = FakeBucket()

    with pytest.raises(OSError, match="simulated PostgreSQL failure"):
        run_cycle(
            config=config,
            client=FakeClient(sample_result()),  # type: ignore[arg-type]
            bucket=bucket,
            repository=FakeRepository(fail_records=True),
            checkpoint=Checkpoint(since_id="100"),
            now=NOW,
        )

    assert any(name.endswith(".json.gz") for name in bucket.blobs)


def test_finalization_failure_keeps_persisted_text_without_advancing_cursor() -> None:
    config = XConfig(handles=("Reporter",), download_media=False)
    bucket = FakeBucket()
    repository = FakeRepository(fail_finalize=True)

    with pytest.raises(OSError, match="simulated PostgreSQL finalization failure"):
        run_cycle(
            config=config,
            client=FakeClient(sample_result()),  # type: ignore[arg-type]
            bucket=bucket,
            repository=repository,
            checkpoint=Checkpoint(since_id="100"),
            now=NOW,
        )

    assert repository.calls == ["records"]
    assert repository.envelopes[0]["records"][0]["text"] == "Player left practice."
