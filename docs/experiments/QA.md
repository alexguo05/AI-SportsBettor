# Q&A Log

Results of data analyses run at the user's request, newest first. One
entry per analysis: date, question, terse answer with the numbers that
matter and the dataset/query they came from. AI assistants: append an
entry here whenever you run a data analysis for the user. Strategy
discussion and design decisions do not belong here — only measured
results.

---

**2026-08-05 — Is there more liquidity on Kalshi? How much?**
For NFL games, yes: Pantera study (282 games, Sep 2025–Jan 2026) measured
Kalshi $1.3B vs Polymarket $359M notional (3.6x), ~19k vs ~2.7k trades/game
(7x), ~2x open interest — but Polymarket books are deeper per dollar (3–4x
more notional to move price; Kalshi leads price moves by ~7s median).
Offseason now (Aug 4 tracker): Polymarket leads futures (~$10.8M vs $6.9M)
but game markets are Kalshi-only ($460k vs $2.5k moneyline; Week 1 listed
since May). Kalshi is also US-legal (Polymarket geoblocked). Live API pull:
market metadata public (276 NFL series), volume/orderbook need free auth.

**2026-08-05 — How heavy is Kalshi's exchange-wide feed? (collector sizing)**
Measured live: ~21k trades per 3 minutes (~10M/day) and ~32k settled markets
per hour exchange-wide, dominated by 15-minute crypto ladders. Consequences
for our collectors: settled-market sweep must be per-series (global sweep =
40+ pages of junk per cycle), and the trades poller reads the global feed at
~20-30 pages/cycle (fine, ~2% of Basic-tier read budget) but cannot backfill
deep history — initial lookback capped at 15 min. NFL scope today: 264 series
(KXNFL*/KXSB*), 305 open events, 5,995 open markets; preseason game markets
already trade (~180-460 contracts/24h) with 2-4c spreads and real depth.

**2026-08-05 — Storage cost of the Kalshi collectors (NFL scope only)?**
Measured live at full depth ($1B bound = whole ladder, matching Polymarket
production): one snapshot of all 5,992 open NFL markets (97k levels, zero
truncation) gzips to 0.48 MB → 2.75 GB/day (~83 GB/mo) at 15s cadence ≈
$1.70/mo GCS after month one, accumulating (~$20/mo after a year); expect
2-3x during season. Full depth vs a $10k bound costs only ~20% more (0.48 vs
0.40 MB/cycle). Structure envelopes ~1.3 GB/mo; trades archive near zero
offseason, ~1 GB/season (only tracked NFL trades stored; the ~15 GB/day
exchange-wide feed download is transfer, not storage). Postgres stays small
(current books only; trades ~2 GB/season). Levers: 30s cadence halves books;
empty-book filtering is useless (all 5,992 books were quoted — MMs quote
dead strikes); a nonzero-open-interest filter would likely halve the set.

**2026-08-05 — Kalshi authenticated API check: what data is available?**
Signed requests (RSA-PSS) work; all collector inputs confirmed: market
metadata with volume/OI/bid/ask + settlement `result`, trade prints
(price, fractional size, taker side), full order-book depth ladders.
Note: current API uses string-decimal fields (`volume_fp`,
`yes_bid_dollars`, `orderbook_fp`) — old integer-cent fields in most docs
are gone. Live liquidity sample (random midweek MLB game, Tigers–Mariners
8/5): 2c spread, ~85k contracts at best bid, ~582k at best ask, ~53k
volume in 2 days — vs a few hundred dollars of depth on typical
Polymarket NFL books and $44k lifetime volume on our biggest confirmed
Polymarket mover.

**2026-08-05 — How many markets saw >100 trades in our trade data?**
Zero. Over 46h of trade prints (Aug 3 19:07 – Aug 5 17:27 UTC, 980 prints):
214 of 2,321 markets saw any trade, 29 saw >10, none saw >100. Busiest:
Chiefs Super Bowl futures, 26 trades / ~$1.5k.

**2026-08-05 — Any new data overnight? Rerun the analysis.**
283 new tweets, labels auto-current via VM timer. Zero new gated movers
(still 3 at +30m, 10 at +2h, all Aug 3–4). 201 apparent ≥2c moves in the
new window were all zero-trade flicker on 33–96c-spread books; gates
rejected every one. Dataset: `tweet_market_dataset_20260805T173029Z`.

**2026-08-04 — The gated +30m movers, elaborated.**
Two real events across 3 rows. (1) Kelce retirement "No" 97→80c on 10
trades/$393 after a *positive* practice tweet — wrong direction, part of an
all-day whipsaw, likely not tweet-caused. (2) Sanders Week-1-starter "No"
55→43.5c in 30m after a deep-ball video (right direction, $64 traded),
fully reverted by 2h; counted twice via a retweet (fan-out artifact).

**2026-08-04 — Don't we need order-book liquidity to actually bet?**
Yes; mover markets had $6–$900 total liquidity and 5–90c spreads. Led to
`midpoint_reaction_v2` + gated buckets: spread ≤5c, ≥$100/side depth,
≥2 trades, ≥$50 notional. Result: 17k labeled rows, ~10 movers. A.J. Brown
mover correctly excluded (28.5c spread, $54 bid depth — untradeable).

**2026-08-04 — What data backed the "small markets move" conclusion?**
Prices from 15–20s order-book snapshots (since late July); trades only from
Aug 4 (so "confirmed movers" = one day of evidence); volume from live Gamma
(lifetime cumulative). Row inspection showed most movers were single $2–$10
trades on dead books; only A.J. Brown-to-Patriots looked tweet-caused.

**2026-08-04 — Why don't we store market volume?**
Volume/liquidity/prices are excluded from the Gamma poller's structural
fingerprint (they tick every poll and would break change-detection), so
they're never extracted to Postgres; raw envelopes contain them only on
structural-change writes. Trades collector now gives exact forward volume.

**2026-08-04 — Do big NFL markets move after tweets?**
No — flattest of all. Top-40 by lifetime volume (championship futures,
$650k–$4.8M): max 0.1c move at 2h over 606 reaction rows. All movers were
sub-$50k niche markets. Lifetime volume ≠ current news sensitivity.
Details: `2026-08-04_nfl-volume-audit.md`.
