# X ingestion and storage contract

## Collection behavior

The collector uses X API v2 recent search:

`GET https://api.x.com/2/tweets/search/recent`

Recent search covers at most the previous seven days. Each request asks for 100
posts and follows `meta.next_token` with the `pagination_token` request
parameter. Every page in one polling cycle keeps the same `since_id` or
`start_time`. A failed page is retried in place up to five times with
exponential backoff and jitter for network errors, rate limits, and HTTP
500/502/503/504 responses. Earlier pages remain in memory during those retries.

The configured handles are split into query batches. A single checkpoint is
safe because every batch in a completed cycle uses the same cursor. The
checkpoint advances only after:

1. every query batch and page succeeds;
2. the compressed raw envelope is uploaded to GCS;
3. post text and pending media metadata commit to PostgreSQL without changing
   the cursor;
4. attached media has been attempted and its status recorded;
5. the cursor commits in a final PostgreSQL transaction.

If an API page, GCS upload, or PostgreSQL transaction fails, the in-memory
checkpoint is not advanced. If media processing is interrupted, text and media
metadata remain stored while the unchanged cursor causes a safe retry.
PostgreSQL upserts prevent duplicate news or media rows.

## Stale checkpoint recovery

The authoritative checkpoint is the `ingest_cursors` PostgreSQL row identified
by `(source='x', stream='recent_search')`. It includes the latest post ID,
update time, and a fingerprint of the complete query set. Legacy checkpoint
objects under `gs://<bucket>/ref/` are no longer read.

The collector uses `since_id` only when the checkpoint:

- belongs to the current query set; and
- is no older than `x_checkpoint_max_age_hours`.

Otherwise it uses a bounded `start_time` recovery window. The default is the
previous 24 hours (`x_recovery_lookback_hours`). Increase it up to 168 hours
before the first restart if more recent-search backfill is required.

## GCS layout

Raw post envelopes:

```text
raw/provider=x/source=recent-search/object=posts/schema=v3/date=YYYY-MM-DD/hour=HH/x_posts_<run_id>.json.gz
```

Downloaded media:

```text
raw/provider=x/source=recent-search/object=media/schema=v1/post_id=<post_id>/<media_key>.<ext>
```

The path hierarchy groups objects by provider, ingestion source, object type,
and then that object's schema version. Post-envelope schema `v3` and media
storage schema `v1` are independent contracts; the latter versions media path
and archival behavior rather than the X post envelope.

All post-envelope partitions and timestamps use UTC. Media object names are
globally deterministic, so retries check GCS before downloading. The collector
also checks the legacy
`raw/media-schema=v1/source=x/post_id=<post_id>/<media_key>.<ext>` location to
avoid uploading an existing asset twice. Photos are stored in full. Videos and
animated GIFs store only their preview image while retaining original type,
duration, source URL, and variants in the raw envelope. Selected assets larger
than 25 MiB are marked `skipped_too_large`.

## Envelope schema

Each gzip-compressed JSON object contains:

- `schema_name` and `schema_version` for migrations;
- `ingest_run_id`, `ingested_at`, and the envelope's own `storage_uri`;
- `content_sha256` over the canonical raw API responses;
- request queries, cursor, query fingerprint, page count, and rate-limit
  response metadata;
- normalized `records` for deterministic PostgreSQL loading;
- exact `raw_api_responses` for replay, audit, and future reprocessing.

Each normalized news record contains:

- stable `news_id` (`x:<post_id>`);
- provider identity and URL (`source`, `source_post_id`, `source_url`);
- nested author identity;
- source and ingestion timestamps;
- text, language, conversation ID, source entities, edit history, sensitivity,
  and observed public metrics;
- normalized repost, quote, and reply relationships; expanded originals are
  included as normal news records without another X API request;
- media metadata, original and selected source URLs, stored asset kind, byte
  size, GCS URI, content hash, and processing status;
- `raw_gcs_uri` linking the row back to its immutable source envelope.

## PostgreSQL mapping

The Alembic migrations create five core tables:

- `raw_ingest_objects`: one row per envelope, keyed by `ingest_run_id`, with
  `storage_uri`, schema version, content hash, source, timestamps, and status;
- `news_events`: one row per `news_id`, with X identity, author identity,
  timestamps, text, source metadata JSON, and a foreign key to
  `raw_ingest_objects`;
- `news_event_relationships`: repost, quote, and reply edges to original post
  IDs, including whether the target was available in the expanded response;
- `news_media`: one row per `(news_id, media_key)`, with source URL, GCS URI,
  MIME type, dimensions, hash, upload status, and processing status;
- `ingest_cursors`: one row per source stream, containing its query
  fingerprint, latest source ID, and successful-poll timestamps.

Use `news_id` and `(news_id, media_key)` as idempotent upsert keys. Keep the GCS
envelope immutable; corrected parsing or later LLM/OCR output belongs in
versioned PostgreSQL analysis tables rather than overwriting raw source data.
