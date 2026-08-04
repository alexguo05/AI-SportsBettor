# Do high-volume NFL markets move after linked tweets, or is movement only in tiny markets?

- Date: 2026-08-04
- Dataset: `news_market_reactions` in place (tweets 2026-07-28 to 2026-08-04),
  joined live against Gamma `volumeNum` for the 2,320 tracked NFL markets
  (1,366 returned volume)
- Label rule: n/a (raw deltas inspected; move = |delta| >= 2c, confirmation =
  trade_count >= 1)
- Split: n/a
- Model: n/a

## Results

Top-40 NFL markets by lifetime volume are championship/conference futures
($650k-$4.8M volume, ~$100k-$320k liquidity) plus a few roster-destination
markets (Tyreek Hill, Maxx Crosby — high past volume, near-dead books now,
liquidity in the hundreds of dollars).

| Group | Reaction rows | 30m coverage | Moves >=2c (30m) | Moves >=2c (2h) | Mean abs delta 2h |
|-------|--------------|--------------|------------------|-----------------|--------------|
| Top-40 by volume | 606 | 302 | 0 | 0 | 0.0000 |
| All other NFL | 70,674 | 33,688 | 1,313 (25 confirmed) | 2,429 (60 confirmed) | 0.0089 |

- Largest post-tweet |delta 2h| seen on any top-40 market: **0.001** (0.1c).
- Executed trades since collection began (Aug 4) on top-40 markets: roughly
  10 trades and a few hundred dollars notional per market per day — the big
  futures books are nearly idle in the offseason despite huge lifetime volume.
- The 25 trade-confirmed 2h movers rank by lifetime volume: Travis Kelce
  retirement ($44k), season-series markets ($7-$11k), Jalen Hurts MVP ($4k),
  Shedeur Sanders Week 1 starter ($460), Kirk Cousins Week 1 starter ($114).
  All niche, news-sensitive markets — none above $50k volume.
- Only 11 of the top-40 markets have any reaction rows; the very largest
  futures (Chargers/Bucs/Panthers championship) have no linked-tweet
  reactions in the window at all.

## Reading

The flat-heavy label distribution is not an artifact of tiny markets
dominating the dataset — the biggest NFL markets are the flattest. Lifetime
volume measures past activity, not current news sensitivity: championship
futures accrued volume around games and barely trade in early August, and
the Hill/Crosby destination markets peaked when their news was live, before
our collection started. The markets that actually reprice on tweets in this
window are small, single-question, news-sensitive contracts (retirement,
starting QB, MVP, season series). For modeling, current liquidity / recent
trade activity is the meaningful "big market" signal, not lifetime volume,
and offseason weeks will be structurally flat; expect the positive class to
concentrate in niche markets until games start.

## Follow-up: row-level inspection of the confirmed movers (same day)

Reading all 67 confirmed-mover rows (tweet text + midpoints + trades)
weakens the "confirmed" claim:

- All confirmed movers come from Aug 3-4 tweets only, because trade data
  starts 2026-08-04 — this is one day of evidence, not a week.
- Most "confirmed" moves are a single $2-$10 trade repricing a near-dead
  book by 15-30c (season-series tie markets, Shedeur Sanders starter).
- Several move the wrong direction relative to the tweet (Kelce retirement
  Yes rising after a positive practice-catch tweet; Hurts MVP falling after
  a glowing camp report). The Kelce market whipsawed 3c->35c->4c in one day
  on ~$400 total — tweets overlap the swings, they don't explain them.
- Link fan-out double-counts: one move is attributed to every tweet
  published in the window, including clearly unrelated ones.
- The one genuinely causal-looking case: Schefter's A.J. Brown thumb tweet
  (2026-08-03 21:27 UTC) -> "A.J. Brown to join Patriots" 84.8c -> 95.6c in
  30m on a $99 trade, and it held at 2h.

`trade_count >= 1` is too weak a confirmation. Next label iteration should
require a minimum notional (e.g. $50-$100) and/or >= 2 distinct trades.

## Verdict

Big NFL markets are flat after tweets (max 0.1c at 2h); all confirmed movers
are sub-$50k niche markets — flatness is real, not a small-market artifact.
Row inspection: most "confirmed" moves are single tiny trades on dead books;
only A.J. Brown-to-Patriots looks genuinely tweet-caused. Raise the
confirmation bar (min notional / >=2 trades) in the next label version.
