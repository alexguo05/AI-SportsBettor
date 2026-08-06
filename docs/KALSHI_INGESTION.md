# Kalshi Ingestion

Three collectors mirror the Polymarket raw-first architecture against the
Kalshi Trade API v2 (`https://api.elections.kalshi.com`). Every cycle archives
a gzip JSON envelope to GCS under
`raw/provider=kalshi/source=trade-api/object=.../` and upserts normalized rows
into PostgreSQL inside one transaction, keyed by `ingest_run_id` in
`raw_ingest_objects`.

| Collector | Command | Interval | Cursor stream |
| --- | --- | --- | --- |
| Structure + settlement | `kalshi-markets` | 15 min | `kalshi` / `structure` |
| Order books | `kalshi-order-books` | 15 s | `kalshi` / `order_books` |
| Trades | `kalshi-trades` | 60 s | `kalshi` / `trades` |

All three accept `--dry-run` (no writes) and `--once` (single live cycle).
Config lives in `src/config/kalshi_config.json`.

## Authentication

Every request is signed with RSA-PSS (SHA-256, digest-length salt) over
`timestamp_ms + METHOD + path` and sent with `KALSHI-ACCESS-KEY`,
`KALSHI-ACCESS-TIMESTAMP`, and `KALSHI-ACCESS-SIGNATURE` headers
(`src/ingest_odds/kalshi_client.py`). Credentials come from the environment
(or `src/.env` locally):

- `KALSHI_API_KEY_ID` — the API key UUID.
- `KALSHI_PRIVATE_KEY_PATH` — path to the PEM private-key file.

Rate limits are token buckets. The Basic tier refills 200 read tokens/second
and most GETs cost 10 tokens, so the client paces itself
(`kalshi_min_request_interval_seconds`, default 0.07 s ≈ 14 req/s) and applies
exponential backoff on 429/5xx; Kalshi 429s carry no `Retry-After` header.

## API schema notes (current, verified live August 2026)

- Numeric fields are fixed-point **strings**: `*_dollars` (prices, up to six
  decimals) and `*_fp` (contract counts, two decimals; fractional contracts
  like `2.50` exist). The integer-cent fields shown in older docs are gone.
- Order books return **bids only** for both sides (`yes_dollars`,
  `no_dollars`), sorted ascending with the best bid last. A NO bid at `p` is a
  YES ask at `1 - p`.
- The batch orderbook endpoint takes up to 100 tickers as **repeated**
  `tickers` query parameters. A comma-separated value is silently treated as
  one ticker.
- Trades carry a canonical `trade_id`, `yes_price_dollars` and
  `no_price_dollars`, `count_fp`, `taker_outcome_side` (`taker_side` is
  deprecated), `taker_book_side`, and `is_block_trade`.

## Structure + settlement collector (`kalshi-markets`)

One poller covers discovery **and** resolution, so there is no separate
resolutions service:

1. `GET /series?category=Sports` (single unpaginated response, ~3.1k series),
   filtered by the `kalshi_series_patterns` regexes (default `^KXNFL`,
   `^KXSB`; 264 series match).
2. Per matching series, `GET /events?series_ticker=...&status=open&`
   `with_nested_markets=true`, cursor-paginated. Nested market objects carry
   the full live state: prices, `volume_fp`, `volume_24h_fp`,
   `open_interest_fp`, status, rules, strike terms.
3. Per matching series, `GET /markets?series_ticker=...&status=settled&`
   `min_settled_ts=<watermark - 1h overlap>` captures each market's final
   `result` (`yes`/`no`/`scalar`), `settlement_value_dollars`, and
   `settlement_ts` after it leaves the open feed. The sweep is per-series on
   purpose: the exchange-wide settled feed churns tens of thousands of crypto
   markets per hour. The watermark is stored in the cursor's `since_id`;
   first run looks back `kalshi_settled_initial_lookback_hours` (168).

Rows land in `kalshi_series`, `kalshi_events`, and `kalshi_markets`.
Version rows (`kalshi_event_versions`, `kalshi_market_versions`) are appended
only when a **structural** hash changes; volatile trading fields (prices,
sizes, volume, open interest, `updated_time`) are stored on the market row
but excluded from the hash, so version history tracks lifecycle transitions:
listing edits, status changes (`active → closed → determined → finalized`),
and settlement. `missing_since` marks events/markets that left the feed, as
with Polymarket. There is intentionally no FK from `kalshi_markets` to
`kalshi_events` because the settled sweep can return markets whose event was
never observed open.

The market row keeps every field the API exposes, including the
settlement/closing data: `result`, `settlement_value`, `settlement_ts`,
`expiration_value`, `last_price`, `previous_price`, plus liveness prices
(`yes_bid/ask`, `no_bid/ask`, best-quote sizes), `volume`, `volume_24h`,
`open_interest`, strike terms, rules text, and `price_ranges` (tick
structure). The raw event/market JSON is archived verbatim in the GCS
envelope (`raw_api_responses`).

### Entity extraction hookup

When an event's structural hash (or any of its markets') changes, the
repository enqueues a `resolve_kalshi_market` job in the same transaction,
mirroring Polymarket's `resolve_market`. The `job-worker.service` loads the
event with its nested markets, shapes them into the shared classifier
input (market `title` → question, `yes_sub_title` → group-item label,
tickers → slugs, strikes → threshold), and runs the same LLM
classification + entity resolution. Mentions land in `entity_mentions`
with `kalshi_market_ticker` set; classifications land in
`kalshi_market_classifications`. From there the linker and reaction
builder pick Kalshi markets up like any other (see `docs/LINKING.md`).
Existing events that predate this hookup are backfilled once with the seed
command (`python -m src.jobs.seed --apply ...`), which enqueues every
non-missing Kalshi event.

## Order-book collector (`kalshi-order-books`)

Snapshots every market in `kalshi_markets` with `status = 'active'` and
`missing_since IS NULL` via `GET /markets/orderbooks` in batches of 100.
Books are normalized to yes-side bid/ask ladders (asks derived from NO bids).
`kalshi_order_book_depth_usdc` defaults to $1B — effectively the full ladder,
matching the Polymarket CLOB collector as deployed — with
`best_bid`, `best_ask`, `midpoint`, `spread`, captured/total notional and
shares, and truncation flags — the same shape as
`polymarket_current_order_books`, keyed by market `ticker` (Kalshi has no
per-outcome tokens; the yes side is the book). Current state goes to
`kalshi_current_order_books`; history accumulates in the GCS envelopes.

Offseason scale: ~6k active markets → 60 requests per cycle (~40% of a
15-second cadence at Basic-tier pacing).

## Trades collector (`kalshi-trades`)

Polls the exchange-wide feed `GET /markets/trades?min_ts=<floor>` newest
first with cursor pagination, then filters client-side to tickers present in
`kalshi_markets` (with a 1-day grace window after `missing_since`). Identity
is the provider `trade_id`; inserts are `ON CONFLICT DO NOTHING`, and the
120-second overlap window makes re-reads idempotent. Rows land in
`kalshi_trades` (`count`, `yes_price`, `no_price`, `taker_outcome_side`,
`taker_book_side`, `is_block_trade`, `traded_at`).

The exchange prints ~5–10k trades/minute (measured ~21k per 3 minutes,
dominated by 15-minute crypto ladders), i.e. ~20–30 pages per 60-second
cycle — a few percent of the read budget. Deep backfill through this feed is
not feasible, so the first run looks back only
`kalshi_trades_initial_lookback_minutes` (15). If a cycle hits
`kalshi_trades_max_pages` (200), it warns and advances the watermark rather
than wedging.

## Deployment

Local migration first: `uv run alembic upgrade head`
(`20260805_17_kalshi_ingestion`).

Three systemd units, following the Polymarket pattern
(`docs/POLYMARKET_INGESTION.md`), with the Kalshi credentials added to the
shared environment file (`/etc/ai-sports-bettor/x.env`):

```
KALSHI_API_KEY_ID=<key uuid>
KALSHI_PRIVATE_KEY_PATH=/etc/ai-sports-bettor/kalshi-key.pem
```

Copy the key with mode 600 and the service user as owner. Units:

```ini
Description=AI Sports Bettor Kalshi Structure Ingestion
ExecStart=/opt/ai-sports-bettor/.venv/bin/python -m src.ingest_odds.kalshi_markets_pull
```

```ini
Description=AI Sports Bettor Kalshi Order Book Ingestion
ExecStart=/opt/ai-sports-bettor/.venv/bin/python -m src.ingest_odds.kalshi_order_book_pull
```

```ini
Description=AI Sports Bettor Kalshi Trades Ingestion
ExecStart=/opt/ai-sports-bettor/.venv/bin/python -m src.ingest_odds.kalshi_trades_pull
```

Start `kalshi-markets` first: the other two read their ticker lists from
`kalshi_markets` and skip cycles until structure exists.

## Verified live behavior (August 5, 2026)

- Structure dry run: 264 series matched, 305 open events, 5,995 open markets
  (preseason week games plus futures ladders), 0 settled in the prior 7 days.
- Order-book dry run: real preseason books returned with correct derived
  asks (e.g. `KXNFLGAME-26AUG15DALSEA-SEA` 0.61/0.63, 19 bid levels).
- Trades: end-to-end capture of 4,466 live trades for a single busy ticker in
  a 3-minute window, fields normalized (fractional counts, both prices,
  taker sides).
