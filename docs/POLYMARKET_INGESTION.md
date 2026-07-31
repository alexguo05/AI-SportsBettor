# Polymarket ingestion and storage contract

## Scope

The first Polymarket collector discovers the currently open event graph
returned by the public Gamma keyset endpoint for `tag_slug=nfl` and
`closed=false`. It writes queryable events, markets, outcome tokens, and state
versions to PostgreSQL.

Collection history begins when the service starts. It does not repeatedly
download Polymarket's historical closed NFL catalog. The open feed can still
include a small number of cultural or otherwise non-game events; relevance
classification belongs in a later normalization layer, not raw collection.

Gamma hashes only structural and lifecycle fields. Prices, volume, liquidity,
bid/ask, spread, and price-change metrics do not create market versions.
Complete Gamma response pages are archived only when the structural event graph
changes; unchanged successful polls update the checkpoint without another GCS
object.

The separate CLOB collector uses the outcome token IDs discovered by Gamma and
stores incremental public price history. A second CLOB collector takes bounded
one-minute order-book snapshots for every open token that accepts orders.

## Running

Apply migrations before starting the collector:

```bash
alembic upgrade head
python -m src.ingest_odds.polymarket_pull
python -m src.ingest_odds.clob_price_pull
python -m src.ingest_odds.clob_order_book_pull
```

To inspect one real Gamma cycle locally without initializing GCS or PostgreSQL:

```bash
python -m src.ingest_odds.polymarket_pull --dry-run
```

Dry-run mode fetches and normalizes the complete configured keyset traversal,
then prints the proposed GCS URI, raw page count, compressed envelope size and
hash, database row counts, and up to five sample events. It performs no GCS,
PostgreSQL, migration, reconciliation, or checkpoint writes.

By default, an order-book dry run reads ten eligible outcome token IDs from
PostgreSQL. Use `--limit` to change the sample size, or repeat `--token-id` to
override the database selection. Repeat `--depth-usdc` to compare bounded
payload sizes from one set of API responses:

```bash
python -m src.ingest_odds.clob_order_book_pull --dry-run \
  --limit 10 \
  --depth-usdc 10000 --depth-usdc 20000 --depth-usdc 50000
```

Standard output is canonical JSON, one exact proposed cloud envelope per depth.
Comparison statistics, including retained level counts and compressed and
uncompressed bytes, are written to standard error.

The public Gamma endpoint does not require an API key. The process reuses the
same GCS and Cloud SQL credentials as X ingestion.

Configuration is in `src/config/polymarket_config.json`:

- `polymarket_tag_slug`: provider tag, currently `nfl`
- `polymarket_closed`: `true`, `false`, or `null` for no status filter
- `polymarket_page_size`: keyset page size, maximum 500
- `polymarket_max_pages`: safety limit; exceeding it fails the cycle
- `polymarket_poll_interval_seconds`: delay between complete discovery cycles
- `polymarket_timeout_seconds` and `polymarket_max_attempts`: HTTP controls
- `clob_price_poll_interval_seconds`: delay between incremental price cycles
- `clob_price_fidelity_minutes`: requested CLOB price sampling interval
- `clob_price_initial_lookback_minutes`: first-cycle history window
- `clob_price_batch_size`: token IDs per request, maximum 20
- `clob_price_timeout_seconds` and `clob_price_max_attempts`: HTTP controls
- `clob_order_book_poll_interval_seconds`: snapshot interval, default 60 seconds
- `clob_order_book_depth_usdc`: cumulative executable notional retained per side
- `clob_order_book_batch_size`: token IDs per `/books` request, maximum 500
- `clob_order_book_timeout_seconds` and `clob_order_book_max_attempts`: HTTP controls
- `gcs_bucket`: raw archive bucket

## GCS layout

```text
raw/provider=polymarket/source=gamma/object=events/schema=v1/date=YYYY-MM-DD/hour=HH/polymarket_events_<run_id>.json.gz
raw/provider=polymarket/source=clob/object=price-history/schema=v1/date=YYYY-MM-DD/hour=HH/polymarket_prices_<run_id>.json.gz
raw/provider=polymarket/source=clob/object=order-books/schema=v1/date=YYYY-MM-DD/hour=HH/polymarket_order_books_<run_id>.json.gz
```

`provider` identifies Polymarket, `source` identifies the Gamma API surface,
`object` identifies the payload family, and `schema` versions our envelope.
Date and hour are UTC ingestion partitions.

Each gzip JSON envelope contains:

- envelope schema name and version
- provider, source, and object type
- ingestion run ID and UTC ingestion time
- immutable GCS URI and SHA-256 of the exact API pages
- endpoint, filter, pagination, retry, and query-fingerprint metadata
- exact provider response pages

Normalized records are intentionally not duplicated inside GCS. They are
derived in memory for PostgreSQL, and replay can regenerate them from the exact
provider responses.

Order-book envelopes are the exception: they contain normalized bid and ask
levels from best price outward until each side reaches the configured cumulative
USDC depth. The boundary level is retained in full, and thin books retain every
level. Per-side captured shares/notional, full-book notional, and truncation
flags make the bounded payload auditable without archiving discarded levels.

## PostgreSQL mapping

- `raw_ingest_objects`: one row per archived Gamma cycle
- `polymarket_events`: latest observed event state
- `polymarket_event_versions`: append-only changed event states
- `polymarket_markets`: latest observed market state
- `polymarket_market_versions`: append-only changed market states
- `polymarket_tokens`: market outcome to CLOB token mapping
- `polymarket_price_points`: latest CLOB value keyed by token and source timestamp
- `polymarket_price_point_versions`: append-only corrected price values
- `polymarket_price_cursors`: per-token incremental CLOB watermarks
- `polymarket_current_order_books`: latest bounded bid/ask levels, one row per token
- `ingest_cursors`: last fully successful Gamma and CLOB cycles

Historical full books remain only in GCS. `raw_ingest_objects` is the compact
PostgreSQL index from ingestion time to immutable GCS URI; training jobs query
that table for the relevant time window, then batch-download and extract books
from the corresponding compressed envelopes. PostgreSQL intentionally does not
duplicate global historical order-book summaries.

Every current and versioned event or market row carries raw-ingest lineage.
Current-state updates are guarded by `last_observed_at`, so replaying an older
envelope cannot regress current state.

After every complete keyset traversal, known events and markets absent from the
response receive `missing_since` equal to that cycle's observation time. The
timestamp remains unchanged while they stay absent and is cleared if they
reappear. Missing records are not automatically labeled closed or deleted,
because provider omissions can be temporary.

Version rows contain hashes, observation time, and raw-object lineage rather
than another copy of provider JSON. Version deduplication compares a new state
only with the immediately preceding state. A sequence such as `A → B → A`
therefore writes all three versions.

## Failure and restart behavior

The cycle order is:

1. fetch every keyset page
2. compute the structural graph fingerprint
3. if changed, upload raw pages and transactionally persist the graph
4. advance the PostgreSQL checkpoint

A failed or truncated fetch does not upload or advance the checkpoint. A failed
GCS upload does not write PostgreSQL. If database persistence succeeds but
checkpoint persistence fails, the next complete cycle safely re-observes the
same graph. An unchanged structural fingerprint skips both the Gamma GCS upload
and graph write.

CLOB watermarks are per token, so newly discovered markets receive their own
configured initial lookback instead of inheriting an older global cursor. A
batch that omits any requested token fails the cycle and advances no token
watermarks. The collector overlaps one fidelity interval on subsequent polls;
unchanged points update current observation metadata without creating duplicate
versions, while provider corrections append a price version.

Run this collector as a separate `systemd` service from `x-ingestion.service`.

Example VM unit:

```ini
[Unit]
Description=AI Sports Bettor Polymarket Gamma Ingestion
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=sportsbettor
Group=sportsbettor
WorkingDirectory=/opt/ai-sports-bettor
EnvironmentFile=/etc/ai-sports-bettor/x-ingestion.env
Environment=PYTHONPATH=/opt/ai-sports-bettor
ExecStart=/opt/ai-sports-bettor/.venv/bin/python -m src.ingest_odds.polymarket_pull
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Use a second unit with the same user, working directory, and environment for
CLOB, changing the description and command to:

```ini
Description=AI Sports Bettor Polymarket CLOB Price Ingestion
ExecStart=/opt/ai-sports-bettor/.venv/bin/python -m src.ingest_odds.clob_price_pull
```

The one-minute order-book collector should run as a third unit:

```ini
Description=AI Sports Bettor Polymarket CLOB Order Book Ingestion
ExecStart=/opt/ai-sports-bettor/.venv/bin/python -m src.ingest_odds.clob_order_book_pull
```

The environment file name can be shared because Gamma needs no secret of its
own and public CLOB reads are unauthenticated. Both services only need the
existing Cloud SQL and GCS environment variables.
