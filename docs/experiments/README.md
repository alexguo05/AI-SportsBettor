# Modeling Research Log

Running record of modeling experiments: what was tried, on which data, with
which knobs, and what happened. One markdown file per experiment, newest at
the top of the index. Keep results honest — negative results are results and
prevent rerunning dead ends.

## Why this exists

Label construction is cheap and versioned (see `docs/LINKING.md`), so we
sweep thresholds, horizons, and architectures freely. Without a log, the
conclusions evaporate between sessions.

## Instructions for AI assistants

When directed to this file for modeling work:

1. Read "Standing knowledge" below before proposing or running experiments —
   it contains settled findings; do not re-litigate them.
2. After running an experiment, add its file here, update the index table,
   and append any durable finding to "Standing knowledge".
3. Pipeline context lives in `docs/LINKING.md` (links, reaction labels,
   dataset export) and `docs/VM_OPERATIONS.md` (what runs where).

## Index

| Date | File | Question | Verdict |
|------|------|----------|---------|
| 2026-08-04 | [2026-08-04_gated-bucket-sweep.md](2026-08-04_gated-bucket-sweep.md) | What survives book-quality + volume gates? | Gates work: 17k rows on healthy books, ~10 real movers, zero-positive offseason regime confirmed |
| 2026-08-04 | [2026-08-04_nfl-volume-audit.md](2026-08-04_nfl-volume-audit.md) | Do high-volume NFL markets move after tweets? | Big markets are the flattest (max 0.1c at 2h); all confirmed movers are sub-$50k niche markets |
| 2026-08-04 | [2026-08-04_label-bucket-sweep.md](2026-08-04_label-bucket-sweep.md) | Class balance of UP/DOWN/FLAT buckets on week-1 data | Buckets work; ~99.9% flat, 25 trade-confirmed moves at +30m/2c — accumulate more trade-covered weeks before training |

## Conventions

- File name: `YYYY-MM-DD_short-slug.md` in this directory.
- Update the index table above when adding a file.
- Record the dataset export file name and its date range — exports are
  timestamped, so this pins the exact data an experiment ran on.
- Record label knobs explicitly: horizon (`+5m/+30m/+2h`), threshold (cents),
  trade-confirmation on/off, and the train/validation date split.
- Metrics to report for classification: per-class precision/recall, and
  calibration (predicted probability vs observed frequency) — accuracy alone
  is meaningless on imbalanced classes.

## Experiment file template

```markdown
# <Question this experiment answers>

- Date:
- Dataset: <export filename, date range, row counts>
- Label rule: <horizon, threshold, trade confirmation>
- Split: <train range / validation range>
- Model: <baseline / architecture + key hyperparameters>

## Results

<metrics table>

## Reading

<2-5 sentences: what this means, what it changes about next steps>

## Verdict

<one line for the index: e.g. "text adds +12pt recall over metadata baseline">
```

## Standing knowledge

Durable findings that future experiments should assume (append as learned):

- Midpoint moves with zero trades are usually quote-shuffling on thin books,
  not market reaction (e.g. a 43-cent "move" on 0 trades, Aug 4). Prefer
  trade-confirmed labels; trade data exists from 2026-08-04 onward.
- Link fan-out is median ~24 markets per tweet. Never split train/validation
  randomly — split by time, or sibling rows of the same tweet leak across
  the split.
- ~46% of pre-Aug-2026 reaction rows have no price labels because tweets
  predate order-book collection; coverage is near-total for newer tweets.
- On week-1 data, 63% of >=2c moves at +30m happened on zero trades — the
  flicker pattern is the norm. Trade-confirmed positives are rare (25 at
  +30m/2c on 2026-08-04); class weighting or oversampling will be needed
  when training starts.
- Lifetime Gamma volume does not measure current news sensitivity: the
  top-volume NFL markets (championship futures) are the flattest after
  tweets, while every confirmed mover is a sub-$50k niche market
  (retirement, starting QB, MVP, season series). Use current liquidity or
  recent trade activity when a "big market" signal is needed. Offseason
  weeks are structurally flat for futures markets.
- `trade_count >= 1` is too weak as move confirmation: row inspection of
  the Aug 3-4 confirmed movers showed most were a single $2-$10 trade
  repricing a dead book, some in the wrong direction relative to the tweet,
  with the same move fan-out-attributed to unrelated co-occurring tweets.
  Require minimum notional (~$50-100) and/or >= 2 trades in the next label
  version. Only A.J. Brown-to-Patriots (Schefter thumb tweet, +10.8c held
  at 2h, $99 trade) looked genuinely tweet-caused.
- Implemented as `midpoint_reaction_v2` + gated buckets (2026-08-04):
  labels gate on baseline spread <= 5c and >= $100 depth per side, moves
  need >= 2 trades and >= $50 notional, and a move must exceed half the
  spread. On week-1 data this yields 17k labeled rows and ~10 movers.
  A midpoint on a wide/empty book is not a price — untradeable moves
  (e.g. A.J. Brown: 28.5c spread, $54 bid depth) are correctly excluded.
- Fan-out attribution (every tweet in the window claims the same move) is
  the largest remaining labeling artifact after v2.
