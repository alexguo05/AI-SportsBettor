# Tweet-Market Linking and Label Construction

This layer turns the collected raw data into training examples. It joins
tweets to the Polymarket markets they may affect, measures how prices moved
after each tweet, and exports the result as a flat dataset a training run
can consume directly.

Everything here is derived and rebuildable: the inputs (tweets, mentions,
order-book envelopes, trades, resolutions) are durably collected by the
ingestion services, so links and labels can be recomputed from scratch at
any time with no data loss.

## Flow

```
entity_mentions (news side)  ─┐
                              ├─> linker ──> news_market_links
entity_mentions (market side) ┘                    │
                                                   v
raw_ingest_objects + GCS order-book envelopes ─> reaction builder ──> news_market_reactions
polymarket_trades ────────────────────────────┘                            │
                                                                           v
news_events + news_enrichments + polymarket_markets ────────────> dataset export (parquet/JSONL)
```

## 1. Linker (`python -m src.linking.link_pull`)

Joins resolved news mentions to resolved market mentions on canonical
`entity_id`. The join is deterministic and mechanical — no LLM calls — and
deliberately inclusive: any tweet sharing an entity with a market is linked,
and quality features are recorded so weak links can be filtered at training
time instead of being dropped at build time.

Per `(news_id, market_id)` pair, `news_market_links` stores:

- `shared_entity_ids` / `shared_entity_count` — which canonical entities the
  tweet and market have in common.
- `news_mention_roles` / `market_mention_roles` — per-entity mention roles
  and person-role hints from each side.
- `market_topic` / `contract_type` — from the market classification when one
  exists.
- `market_open_at_publish` — false when the market was first observed only
  after the tweet published (the market may not have existed yet). The link
  is kept, flagged.
- `linker_version` — currently `entity_overlap_v1`.

The only exclusion: markets that were provably final before the tweet
published (`closed_time` or `resolution_observed_at` earlier than
`published_at`), because no price reaction can exist for them.

Each run recomputes the full link set and upserts it, then prunes rows that
were not regenerated (their mention resolution changed or the market became
final). Re-runs are idempotent and retroactive over every tweet already
collected. The run checkpoint lives in `ingest_cursors` under
`(linking, news_market_links)`.

`--dry-run` computes and reports links without writing.

## 2. Reaction builder (`python -m src.linking.reactions_pull`)

Builds price-reaction labels for links whose post-publish window has fully
elapsed and which have no rows at the current `label_version`
(`midpoint_reaction_v1`).

For each linked tweet it selects order-book envelopes from the
`raw_ingest_objects` index:

- **Baseline** — the newest snapshot at or before publish, within a
  10-minute lookback.
- **Horizons** — the oldest snapshot at or after publish +1m, +5m, +30m,
  and +2h, accepted only within a 120-second tolerance so a collector gap
  cannot masquerade as a later price.

Only those chosen envelopes are downloaded from GCS (at most five per
tweet, LRU-cached across the run), never the full window. Midpoints are
extracted per outcome token and stored with deltas against the baseline in
`news_market_reactions`.

Trust signals per row instead of failures:

- `snapshot_count` / `max_gap_seconds` — coverage of the 2-hour post window
  computed from the envelope index (including window edges). A row with a
  large gap is visible, not silently wrong.
- Missing snapshots or tokens absent from an envelope leave the midpoint
  NULL rather than inventing a price.
- `trade_count` / `trade_notional` — executed volume in the 2-hour window
  from `polymarket_trades`. NULL when the window predates trade collection
  (before 2026-08-04); zero means trades were collected and none occurred.

`--limit N` caps the number of links labeled in one run.

## 3. Dataset export (`python -m src.linking.export_dataset`)

Joins tweet text, the latest completed enrichment (summary, claims,
usefulness), link features, reaction labels, and the market's final
resolution into one row per (tweet, market, outcome token). Writes JSONL
and parquet under `data/local/datasets/` by default.

- `--since` / `--until` bound the tweet publish time.
- `--format jsonl|parquet|both` (default both).
- Nested values (claims, role maps, entity lists) are JSON strings so the
  schema stays flat.
- `outcome_won` is derived from `winning_outcome_index` when the market has
  resolved; NULL otherwise.

## Versioning and rebuilds

- `linker_version` and `label_version` are stamped on every row. Changing
  linking or labeling logic means bumping the version constant; the next
  run rebuilds under the new version without destroying prior labels
  (reactions key on `label_version`; links are recomputed in place).
- To force a full label rebuild at the same version, delete the affected
  `news_market_reactions` rows and rerun the builder.
- Order of operations after a schema migration: `alembic upgrade head`,
  then linker, then reaction builder, then export.

## Running

Both builders are on-demand batches, run locally or on the VM:

```
python -m src.linking.link_pull            # or: link-news-markets
python -m src.linking.reactions_pull       # or: build-reaction-labels
python -m src.linking.export_dataset       # or: export-training-dataset
```

They need the same environment as the ingestion services (Cloud SQL access
and, for the reaction builder, GCS read access).

On the VM, `news-market-linking.timer` runs both builders hourly as a
oneshot `news-market-linking.service` (linker first, then the reaction
builder). Because both are idempotent, manual runs and the timer can
coexist safely. The export remains on-demand only.

Prefer running the reaction builder on the VM rather than locally: it
downloads order-book envelopes from GCS, and the VM sits in the same region
as the bucket and Cloud SQL, so a build that takes ~an hour over a home
connection completes in minutes there.
