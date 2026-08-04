"""Classification buckets over exported reaction rows.

The stored labels are raw midpoints, deltas, book-quality stats, and trade
stats; this module turns them into the training target: did the market
reprice up, down, or not at all within a horizon. Buckets are a formula,
not data — rerunning with different knobs rebuilds every label in seconds,
so sweeping thresholds and horizons is the expected workflow (record
outcomes in docs/experiments/).

Bucket rules, per row:

- Book-quality gate (unless disabled): the baseline snapshot must have a
  known spread at or below the cap and executable depth on both sides at
  or above the floor, otherwise the midpoint is not a real price and the
  row is unlabeled. A one-sided or empty book fails this gate.
- delta missing (no price coverage)              -> unlabeled (None)
- |delta| below the threshold or inside half the
  baseline spread (quote noise by construction)  -> flat
- otherwise, a candidate move:
    - trade confirmation off                     -> up / down by sign
    - trades unknown (window predates collection)-> unlabeled (None)
    - fewer trades or less notional than required-> flat (one tiny trade
      repricing a dead book is not a market reaction)
    - enough executed volume                     -> up / down by sign
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas

HORIZONS = ("plus_1m", "plus_5m", "plus_30m", "plus_2h")
DEFAULT_HORIZON = "plus_30m"
SWEEP_THRESHOLDS_CENTS = (Decimal("1"), Decimal("2"), Decimal("5"))

UP = "up"
DOWN = "down"
FLAT = "flat"

QUALITY_COLUMNS = ("baseline_spread", "baseline_bid_depth", "baseline_ask_depth")


@dataclass(frozen=True)
class BucketRule:
    horizon: str = DEFAULT_HORIZON
    threshold_cents: Decimal = Decimal("2")
    trade_confirmed: bool = True
    min_trades: int = 2
    min_trade_notional: Decimal = Decimal("50")
    book_quality: bool = True
    max_spread_cents: Decimal = Decimal("5")
    min_depth: Decimal = Decimal("100")


DEFAULT_RULE = BucketRule()


def _decimal(value: Any) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def bucket_label(
    delta: float | Decimal | None,
    *,
    trade_count: int | None,
    trade_notional: float | Decimal | None,
    spread: float | Decimal | None,
    bid_depth: float | Decimal | None,
    ask_depth: float | Decimal | None,
    rule: BucketRule = DEFAULT_RULE,
) -> str | None:
    if rule.book_quality:
        spread_value = _decimal(spread)
        bid = _decimal(bid_depth)
        ask = _decimal(ask_depth)
        if spread_value is None or spread_value > rule.max_spread_cents / 100:
            return None
        if bid is None or ask is None or min(bid, ask) < rule.min_depth:
            return None
    delta_value = _decimal(delta)
    if delta_value is None:
        return None
    magnitude = abs(delta_value)
    threshold = rule.threshold_cents / 100
    if rule.book_quality:
        # A midpoint change smaller than half the spread is quote noise by
        # construction; only meaningful when the book gate is on.
        threshold = max(threshold, (_decimal(spread) or Decimal("0")) / 2)
    if magnitude < threshold:
        return FLAT
    if rule.trade_confirmed:
        if trade_count is None:
            return None
        notional = _decimal(trade_notional) or Decimal("0")
        if trade_count < rule.min_trades or notional < rule.min_trade_notional:
            return FLAT
    return UP if delta_value > 0 else DOWN


def rule_column_name(rule: BucketRule) -> str:
    cents = format(rule.threshold_cents.normalize(), "f").replace(".", "p")
    suffix = "tc" if rule.trade_confirmed else "raw"
    if not rule.book_quality:
        suffix += "_nobook"
    return f"target_{rule.horizon}_{cents}c_{suffix}"


def apply_rule(frame: pandas.DataFrame, rule: BucketRule) -> pandas.Series:
    import pandas as pd

    delta_column = f"delta_{rule.horizon}"
    if delta_column not in frame.columns:
        raise ValueError(f"dataset has no column {delta_column}")
    if rule.book_quality:
        missing = [column for column in QUALITY_COLUMNS if column not in frame.columns]
        if missing:
            raise ValueError(
                f"dataset lacks book-quality columns {missing}; re-export from "
                "midpoint_reaction_v2 reactions or pass --no-book-quality"
            )

    def value(row: pandas.Series, column: str) -> Any:
        if column not in row.index:
            return None
        cell = row[column]
        return None if pd.isna(cell) else cell

    def label(row: pandas.Series) -> str | None:
        trade_count = value(row, "trade_count")
        return bucket_label(
            value(row, delta_column),
            trade_count=None if trade_count is None else int(trade_count),
            trade_notional=value(row, "trade_notional"),
            spread=value(row, "baseline_spread"),
            bid_depth=value(row, "baseline_bid_depth"),
            ask_depth=value(row, "baseline_ask_depth"),
            rule=rule,
        )

    return frame.apply(label, axis=1)


def summarize(series: pandas.Series, column: str) -> str:
    total = len(series)
    labeled = series.notna().sum()
    parts = [f"{column}: {labeled}/{total} labeled"]
    for value in (UP, DOWN, FLAT):
        count = int((series == value).sum())
        share = (count / labeled * 100) if labeled else 0.0
        parts.append(f"{value}={count} ({share:.1f}%)")
    return " | ".join(parts)


def latest_export(directory: Path) -> Path:
    candidates = sorted(directory.glob("tweet_market_dataset_*.parquet"))
    if not candidates:
        raise FileNotFoundError(
            f"no tweet_market_dataset_*.parquet found in {directory}; "
            "run export-training-dataset first"
        )
    return candidates[-1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        help="Dataset parquet from export-training-dataset "
        "(default: newest in data/local/datasets)",
    )
    parser.add_argument("--horizon", choices=HORIZONS, default=DEFAULT_HORIZON)
    parser.add_argument(
        "--threshold-cents",
        type=Decimal,
        default=DEFAULT_RULE.threshold_cents,
        help="Minimum |delta| in cents to count as a move (default: 2)",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=DEFAULT_RULE.min_trades,
        help="Trades required in the window to confirm a move (default: 2)",
    )
    parser.add_argument(
        "--min-trade-notional",
        type=Decimal,
        default=DEFAULT_RULE.min_trade_notional,
        help="Executed notional (USD) required to confirm a move (default: 50)",
    )
    parser.add_argument(
        "--max-spread-cents",
        type=Decimal,
        default=DEFAULT_RULE.max_spread_cents,
        help="Widest baseline spread (cents) a labeled row may have (default: 5)",
    )
    parser.add_argument(
        "--min-depth",
        type=Decimal,
        default=DEFAULT_RULE.min_depth,
        help="Executable notional (USD) required on each side of the baseline "
        "book (default: 100)",
    )
    parser.add_argument(
        "--no-trade-confirmation",
        action="store_true",
        help="Count moves without requiring executed trades in the window",
    )
    parser.add_argument(
        "--no-book-quality",
        action="store_true",
        help="Skip the baseline spread/depth gate (needed for pre-v2 exports)",
    )
    parser.add_argument(
        "--sweep",
        action="store_true",
        help="Add target columns for every horizon x threshold combination "
        "(1c/2c/5c) using the configured gates, instead of a single rule",
    )
    parser.add_argument(
        "--output",
        help="Output parquet path (default: <input stem>_labeled.parquet)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    import pandas as pd

    args = build_parser().parse_args(argv)
    try:
        if args.threshold_cents <= 0:
            raise ValueError("--threshold-cents must be positive")
        input_path = (
            Path(args.input)
            if args.input
            else latest_export(Path("data/local/datasets"))
        )
        frame = pd.read_parquet(input_path)
        print(f"Loaded {len(frame)} rows from {input_path}")

        base_rule = BucketRule(
            horizon=args.horizon,
            threshold_cents=args.threshold_cents,
            trade_confirmed=not args.no_trade_confirmation,
            min_trades=args.min_trades,
            min_trade_notional=args.min_trade_notional,
            book_quality=not args.no_book_quality,
            max_spread_cents=args.max_spread_cents,
            min_depth=args.min_depth,
        )
        rules = (
            [
                replace(base_rule, horizon=horizon, threshold_cents=threshold)
                for horizon in HORIZONS
                for threshold in SWEEP_THRESHOLDS_CENTS
            ]
            if args.sweep
            else [base_rule]
        )
        primary_column: str | None = None
        for rule in rules:
            column = rule_column_name(rule)
            frame[column] = apply_rule(frame, rule)
            primary_column = primary_column or column
            print(summarize(frame[column], column))
        if not args.sweep and primary_column:
            frame["target"] = frame[primary_column]

        output_path = (
            Path(args.output)
            if args.output
            else input_path.with_name(f"{input_path.stem}_labeled.parquet")
        )
        frame.to_parquet(output_path, index=False)
        print(f"Wrote labeled dataset to {output_path}")
        return 0
    except Exception as exc:
        print(f"ERROR: label bucketing failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
