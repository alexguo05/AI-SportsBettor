# What survives the v2 book-quality and volume gates?

- Date: 2026-08-04 (evening; follows the two earlier entries from today)
- Dataset: `tweet_market_dataset_20260805T003846Z.parquet`, tweets
  2026-07-28 to 2026-08-04, 72,234 rows / 1,634 tweets, reactions at
  `midpoint_reaction_v2` (adds `baseline_spread`, `baseline_bid_depth`,
  `baseline_ask_depth` from the archived order books)
- Label rule: gated buckets — baseline spread <= 5c, depth >= $100 per
  side, move >= threshold and > half the spread, confirmed by >= 2 trades
  and >= $50 executed notional
- Split: n/a (label construction only)
- Model: n/a

## Results

- Book-quality gate: 17,123 of 72,234 rows labeled (healthy books only).
  The rest: no baseline snapshot (pre-collection tweets), wide spread,
  or thin/one-sided book.
- Confirmed moves at +30m/2c: **3** (all down). At +2h/1c: **10**.
  Everything else flat (>= 99.9%).
- The +2h/1c survivors look qualitatively real: books with $700-$22k per
  side, 1-4c spreads, 2-10 trades. Best case: Shedeur Sanders Week 1
  starter "No" fell 11.5-14.5c within 30m of bad Browns practice news
  (three tweets share the move — fan-out attribution remains).
- The A.J. Brown "causal" mover from the earlier audit was **excluded by
  the gate**, correctly: its baseline book had a 28.5c spread and $54 of
  bid depth. The 10.8c move was real but untradeable — nothing to bet
  into. A reaction you cannot execute against is not a training positive
  for a betting model.

## Reading

The gates collapse week-1 offseason data to essentially zero usable
positives, and that is the true state of the world: in this window there
were almost no tweet-driven, tradeable repricings in NFL markets. The
label machinery is now trustworthy — what it needs is data from weeks
where liquid markets react to news (i.e., once games start). Fan-out
(several tweets claiming one move) is now the largest remaining labeling
artifact.

## Verdict

Gated labels work and are honest: 17k rows on healthy books, ~10 confirmed
movers, zero-positive regime confirmed for the offseason. Collect through
game weeks before training.
