# What class balance do the UP/DOWN/FLAT buckets produce on week 1 data?

- Date: 2026-08-04
- Dataset: `tweet_market_dataset_20260804T225756Z.parquet`, tweets
  2026-07-28 to 2026-08-04, 71,280 rows covering 1,614 tweets
- Label rule: full sweep — horizons +1m/+5m/+30m/+2h, thresholds 1c/2c/5c,
  trade confirmation on
- Split: n/a (no model trained; label construction only)
- Model: n/a

## Results

Labeled counts and class balance per rule (unlabeled rows are excluded from
percentages; a row is unlabeled when it has no price coverage at the horizon,
or when it moved past the threshold but trade data was not yet collected):

| Rule | Labeled / total | up | down | flat |
|------|-----------------|----|------|------|
| +30m, 1c, tc | 33,207 / 71,280 | 19 | 17 | 33,171 (99.9%) |
| +30m, 2c, tc | 33,535 / 71,280 | 15 | 10 | 33,510 (99.9%) |
| +30m, 5c, tc | 33,812 / 71,280 | 6 | 6 | 33,800 (100.0%) |
| +2h, 1c, tc | 32,700 / 71,280 | 43 | 28 | 32,629 (99.8%) |
| +2h, 2c, tc | 33,180 / 71,280 | 37 | 23 | 33,120 (99.8%) |
| +1m/+5m rules | ~33,700 / 71,280 | 0-5 | 1-4 | ~100% |

Decomposition of the +30m, 2c rule (why so few moves):

- 37,290 rows have no +30m delta (tweet predates order-book collection or
  the window lacked snapshot coverage).
- 1,313 rows moved >= 2c at +30m. Of those: 833 had **zero trades** in the
  window (quote flicker, labeled flat), 455 predate trade collection
  (unlabeled), and only **25 were trade-confirmed real moves**.
- Trade counts are only known for 14,854 rows because the trades collector
  went live 2026-08-04.

## Reading

The trade-confirmation filter is doing enormous work: 833 of 1,313 apparent
moves (63%) happened on zero trades — the Ravens-style flicker is the norm,
not the exception. With one week of tweets and less than one day of trade
coverage, trade-confirmed positives are extremely rare (25 at +30m/2c).
The +2h horizon roughly doubles positives versus +30m. Any model trained
now would just learn to say "flat"; we need more weeks of collection (with
trades running) before a classifier is viable. When training does start,
class weighting or positive-class oversampling will be mandatory.

## Verdict

Buckets work as designed; ~99.9% flat on week-1 data — accumulate more
trade-covered weeks before training.
