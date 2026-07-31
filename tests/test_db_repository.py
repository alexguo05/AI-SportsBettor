from datetime import UTC, datetime

from src.db.models import metadata
from src.db.repository import (
    media_values,
    news_event_values,
    raw_object_values,
    relationship_values,
)

NOW = datetime(2026, 7, 29, 18, 0, tzinfo=UTC)


def test_ingestion_schema_contains_expected_tables() -> None:
    assert set(metadata.tables) == {
        "raw_ingest_objects",
        "news_events",
        "news_event_relationships",
        "news_media",
        "news_enrichments",
        "news_enrichment_tags",
        "ingest_cursors",
        "polymarket_events",
        "polymarket_event_versions",
        "polymarket_markets",
        "polymarket_market_versions",
        "polymarket_tokens",
        "polymarket_price_points",
        "polymarket_price_point_versions",
        "polymarket_price_cursors",
        "polymarket_current_order_books",
    }
    assert metadata.tables["news_events"].primary_key.columns.keys() == ["news_id"]
    assert metadata.tables["news_media"].primary_key.columns.keys() == [
        "news_id",
        "media_key",
    ]
    assert "missing_since" in metadata.tables["polymarket_events"].columns
    assert "missing_since" in metadata.tables["polymarket_markets"].columns
    assert "last_structural_sha256" in metadata.tables["ingest_cursors"].columns
    assert metadata.tables["news_enrichments"].primary_key.columns.keys() == [
        "news_id",
        "enrichment_version",
    ]
    enrichment_columns = metadata.tables["news_enrichments"].columns.keys()
    tag_columns = metadata.tables["news_enrichment_tags"].columns.keys()
    assert "primary_tag" not in enrichment_columns
    assert "confidence" not in enrichment_columns
    assert "certainty" in tag_columns
    assert "confidence" not in tag_columns
    assert "polymarket_order_book_snapshots" not in metadata.tables
    assert "bids" in metadata.tables["polymarket_current_order_books"].columns
    assert "asks" in metadata.tables["polymarket_current_order_books"].columns


def test_envelope_and_record_map_to_relational_rows() -> None:
    record = {
        "news_id": "x:200",
        "source": "x",
        "source_post_id": "200",
        "source_url": "https://x.com/Reporter/status/200",
        "author": {
            "source_user_id": "10",
            "username": "Reporter",
            "display_name": "Reporter Name",
            "verified": True,
            "profile_image_url": "https://example.test/profile.jpg",
        },
        "text": "Player left practice.",
        "language": "en",
        "conversation_id": "200",
        "published_at": "2026-07-29T17:59:00+00:00",
        "ingested_at": NOW.isoformat(),
        "possibly_sensitive": False,
        "public_metrics": {"like_count": 10},
        "source_entities": {},
        "edit_history_post_ids": ["200"],
        "relationships": [
            {
                "relationship_type": "retweeted",
                "target_news_id": "x:100",
                "target_source_post_id": "100",
                "target_available": True,
            }
        ],
        "media": [
            {
                "media_key": "3_abc",
                "media_type": "photo",
                "source_url": "https://example.test/image.jpg",
                "preview_image_url": None,
                "selected_source_url": "https://example.test/image.jpg",
                "stored_asset_kind": "original_image",
                "gcs_uri": "gs://bucket/image.jpg",
                "content_type": "image/jpeg",
                "content_sha256": "a" * 64,
                "byte_size": 1000,
                "width": 100,
                "height": 50,
                "duration_ms": None,
                "alt_text": "Practice report",
                "upload_status": "uploaded",
                "upload_error": None,
            }
        ],
    }
    envelope = {
        "ingest_run_id": "a" * 32,
        "provider": "x",
        "source": "recent-search",
        "object_type": "posts",
        "schema_name": "x_posts",
        "schema_version": 1,
        "storage_uri": "gs://bucket/envelope.json.gz",
        "content_sha256": "b" * 64,
        "record_count": 1,
        "ingested_at": NOW.isoformat(),
        "request": {"endpoint": "https://api.x.com/2/tweets/search/recent"},
        "checkpoint_candidate": {"newest_id": "200"},
    }

    raw_row = raw_object_values(envelope)
    event_row = news_event_values(record, envelope["ingest_run_id"])
    media_row = media_values(record, record["media"][0])
    relationship_row = relationship_values(record, record["relationships"][0])

    assert raw_row["request_metadata"]["checkpoint_candidate"]["newest_id"] == "200"
    assert event_row["news_id"] == "x:200"
    assert event_row["author_username"] == "Reporter"
    assert event_row["published_at"].tzinfo == UTC
    assert media_row["gcs_uri"] == "gs://bucket/image.jpg"
    assert media_row["stored_asset_kind"] == "original_image"
    assert media_row["byte_size"] == 1000
    assert relationship_row["source_news_id"] == "x:200"
    assert relationship_row["target_news_id"] == "x:100"
    assert relationship_row["relationship_type"] == "retweeted"


def test_legacy_x_envelope_maps_to_new_raw_dimensions() -> None:
    raw_row = raw_object_values(
        {
            "ingest_run_id": "a" * 32,
            "source": "x",
            "object_type": "news_posts",
            "schema_name": "x_posts",
            "schema_version": 3,
            "storage_uri": "gs://bucket/legacy.json.gz",
            "content_sha256": "b" * 64,
            "record_count": 1,
            "ingested_at": NOW.isoformat(),
            "request": {},
        }
    )

    assert raw_row["provider"] == "x"
    assert raw_row["source"] == "recent-search"
    assert raw_row["object_type"] == "posts"
