# News enrichment contract

News enrichment is downstream of X ingestion. It does not change the meaning or
write path of `raw_ingest_objects`, `news_events`, `news_media`,
`news_event_relationships`, or `ingest_cursors`.

## Output dimensions

Each `(news_id, enrichment_version)` result has:

- one or more unordered, queryable topic tags, with every materially applicable
  tag included;
- independent information status (`official`, `reported`, `rumor`, `opinion`,
  or `unknown`);
- independent usefulness (`high`, `medium`, `low`, or `irrelevant`);
- a factual summary, classification reason, entities, and claims;
- source references that connect tags, entities, and claims to tweet, article,
  image, or video-frame evidence;
- provider, exact model ID, prompt version, input fingerprint, source manifest,
  token usage, warnings, timestamps, and any failure.

The initial topic taxonomy is:

- `injury_availability`
- `lineup_depth_chart`
- `roster_transaction`
- `coaching_management`
- `contract`
- `discipline_legal`
- `weather_field_conditions`
- `schedule_travel`
- `game_status_result`
- `performance_statistics`
- `market_odds`
- `league_management_rule`
- `promotional_social`
- `unrelated_other`

Tags do not have primary/secondary ranking or numeric confidence percentages.
Each tag instead has an evidence-strength category: `confident`, `neutral`, or
`unconfident`. `irrelevant` is a usefulness value rather than a topic. A post
can therefore be about promotional content while also being explicitly marked
irrelevant.

## Storage

Migration `20260731_05` adds:

- `news_enrichments`: one versioned result and its audit metadata;
- `news_enrichment_tags`: normalized tags for filtering and aggregation.

Raw source rows remain untouched. Reprocessing uses a new
`enrichment_version` or deliberately replaces the same version.

## Source processing

Article processing follows only URLs directly represented in the X source
entities. Requests:

- allow only public HTTP/HTTPS addresses on standard ports;
- reject credentials and private, loopback, link-local, reserved, multicast,
  and unspecified network targets;
- revalidate every redirect;
- enforce redirect, timeout, byte, content-type, and extracted-text limits;
- do not bypass authentication or paywalls.

Images are normalized to bounded JPEG inputs for Claude Vision.

Videos are downloaded to a temporary file, sampled into at most eight bounded
frames from the first five minutes, and deleted. Complete video is not added to
the raw GCS media prefix. Claude does not accept raw video or audio, so audio is
reported as `not_transcribed` unless a transcript is supplied by another
component. The schema retains provided transcripts and makes that limitation
visible rather than claiming unsupported audio analysis.

## Safe local dry run

Configure `ANTHROPIC_API_KEY`, `NEWS_ENRICHMENT_MODEL`, and
`NEWS_ENRICHMENT_VERSION` in the process environment or in the ignored
`src/.env` file. Never commit `src/.env`. The pinned default model is
`claude-haiku-4-5-20251001`, which retains structured text and vision support
at a lower token price than Sonnet. Output is capped at 1,536 tokens by default;
override that only after evaluation with `NEWS_ENRICHMENT_MAX_OUTPUT_TOKENS`.

Fully offline control-flow test:

```powershell
python -m src.enrich_news.dry_run `
  --input tests/fixtures/enrichment_records.jsonl `
  --provider mock
```

Live Claude with local output and optional public source reads:

```powershell
$env:ANTHROPIC_API_KEY = "<secret>"
python -m src.enrich_news.dry_run `
  --input tests/fixtures/enrichment_records.jsonl `
  --provider claude `
  --allow-network
```

The dry-run module never initializes Cloud SQL or GCS.
Mock-provider results cannot be applied to PostgreSQL.

Read existing PostgreSQL candidates but keep results local:

```powershell
python -m src.enrich_news.worker --limit 20 --provider claude --allow-network
```

Database persistence requires both `--apply` and the explicit confirmation:

```powershell
python -m src.enrich_news.worker `
  --limit 20 `
  --provider claude `
  --allow-network `
  --apply `
  --confirm-live-writes APPLY_ENRICHMENTS
```

Do not use the applying form until migration `20260731_05` has been reviewed and
applied to the intended database.
