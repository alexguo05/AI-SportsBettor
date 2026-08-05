"""Unit tests for the Kalshi structure, order-book, and trades collectors."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from src.ingest_odds.kalshi_markets_pipeline import (
    KalshiMarketsConfig,
    fetch_structure,
    load_settled_floor,
    normalize_kalshi_event,
    normalize_kalshi_market,
    prepare_records,
    query_fingerprint,
    resolve_series_ticker,
    structural_fingerprint,
)
from src.ingest_odds.kalshi_order_book_pipeline import (
    fetch_books,
    normalize_book,
)
from src.ingest_odds.kalshi_trades_pipeline import (
    KalshiTradesConfig,
    fetch_new_trades,
    load_watermark,
    normalize_trade,
)

NOW = datetime(2026, 8, 5, 18, 0, tzinfo=UTC)


class FakeKalshiClient:
    """Route GET requests to canned responses and record every call."""

    def __init__(self, responder: Any) -> None:
        self.responder = responder
        self.calls: list[tuple[str, Any]] = []

    def get_json(
        self,
        path: str,
        params: Any = None,
        *,
        description: str = "request",
    ) -> tuple[dict[str, Any], int]:
        self.calls.append((path, params))
        return self.responder(path, params), 1


def market_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "ticker": "KXNFLGAME-26AUG15DALSEA-SEA",
        "event_ticker": "KXNFLGAME-26AUG15DALSEA",
        "market_type": "binary",
        "title": "Seattle at Dallas Winner?",
        "yes_sub_title": "Seattle",
        "no_sub_title": "Dallas",
        "status": "active",
        "result": "",
        "can_close_early": True,
        "open_time": "2026-06-01T14:00:00Z",
        "close_time": "2026-08-16T04:00:00Z",
        "latest_expiration_time": "2026-08-23T04:00:00Z",
        "created_time": "2026-05-20T00:00:00Z",
        "updated_time": "2026-08-05T17:00:00Z",
        "settlement_timer_seconds": 3600,
        "rules_primary": "If Seattle wins, the market resolves to Yes.",
        "rules_secondary": "",
        "price_level_structure": "0.01",
        "price_ranges": [{"start": "0.01", "end": "0.99", "step": "0.01"}],
        "notional_value_dollars": "1.0000",
        "expiration_value": "",
        "yes_bid_dollars": "0.5800",
        "yes_ask_dollars": "0.6000",
        "no_bid_dollars": "0.4000",
        "no_ask_dollars": "0.4200",
        "last_price_dollars": "0.5900",
        "previous_price_dollars": "0.5700",
        "yes_bid_size_fp": "85000.00",
        "yes_ask_size_fp": "582000.00",
        "volume_fp": "53000.25",
        "volume_24h_fp": "12000.00",
        "open_interest_fp": "40000.00",
    }
    payload.update(overrides)
    return payload


def test_normalize_kalshi_market_maps_all_fields() -> None:
    record = normalize_kalshi_market(market_payload(), NOW, {"KXNFLGAME"})
    assert record is not None
    assert record["ticker"] == "KXNFLGAME-26AUG15DALSEA-SEA"
    assert record["series_ticker"] == "KXNFLGAME"
    assert record["result"] is None
    assert record["expiration_value"] is None
    assert record["yes_bid"] == "0.5800"
    assert record["volume"] == "53000.25"
    assert record["open_time"] == "2026-06-01T14:00:00+00:00"
    assert record["settlement_timer_seconds"] == 3600
    assert record["can_close_early"] is True


def test_normalize_kalshi_market_captures_settlement() -> None:
    record = normalize_kalshi_market(
        market_payload(
            status="settled",
            result="yes",
            settlement_value_dollars="1.0000",
            settlement_ts="2026-08-16T04:37:31.208599Z",
        ),
        NOW,
        {"KXNFLGAME"},
    )
    assert record is not None
    assert record["result"] == "yes"
    assert record["settlement_value"] == "1.0000"
    assert record["settlement_ts"] == "2026-08-16T04:37:31.208599+00:00"


def test_structural_hash_ignores_volatile_trading_fields() -> None:
    base = normalize_kalshi_market(market_payload(), NOW, {"KXNFLGAME"})
    moved = normalize_kalshi_market(
        market_payload(
            yes_bid_dollars="0.6300",
            volume_fp="99999.00",
            open_interest_fp="1.00",
            updated_time="2026-08-05T17:59:00Z",
        ),
        NOW,
        {"KXNFLGAME"},
    )
    changed = normalize_kalshi_market(
        market_payload(status="closed"),
        NOW,
        {"KXNFLGAME"},
    )
    assert base is not None and moved is not None and changed is not None
    assert base["content_sha256"] == moved["content_sha256"]
    assert base["content_sha256"] != changed["content_sha256"]


def test_resolve_series_ticker_prefers_longest_prefix() -> None:
    series = {"KXNFL", "KXNFLGAME", "KXNFLWINS-PIT"}
    assert resolve_series_ticker("KXNFLGAME-26AUG15DALSEA-SEA", series) == "KXNFLGAME"
    assert resolve_series_ticker("KXNFLWINS-PIT-26-5", series) == "KXNFLWINS-PIT"
    assert resolve_series_ticker("KXMLBGAME-26AUG05DETSEA-SEA", series) is None


def test_normalize_kalshi_event_nests_markets() -> None:
    event = {
        "event_ticker": "KXNFLGAME-26AUG15DALSEA",
        "series_ticker": "KXNFLGAME",
        "title": "Seattle at Dallas",
        "sub_title": "Preseason Week 1",
        "mutually_exclusive": True,
        "collateral_return_type": "binary",
        "settlement_sources": [{"name": "NFL", "url": "https://nfl.com"}],
        "markets": [market_payload(), {"ticker": None}],
    }
    record = normalize_kalshi_event(event, NOW, {"KXNFLGAME"})
    assert record is not None
    assert record["event_ticker"] == "KXNFLGAME-26AUG15DALSEA"
    assert len(record["markets"]) == 1
    assert record["markets"][0]["series_ticker"] == "KXNFLGAME"
    assert record["content_sha256"]


def test_fetch_structure_filters_series_and_sweeps_settled() -> None:
    config = KalshiMarketsConfig(series_patterns=("^KXNFL",))
    settled_ticker = "KXNFLGAME-26AUG08KCDET-KC"

    def responder(path: str, params: Any) -> dict[str, Any]:
        if path.endswith("/series"):
            return {
                "series": [
                    {"ticker": "KXNFLGAME", "category": "Sports"},
                    {"ticker": "KXMLBGAME", "category": "Sports"},
                ]
            }
        if path.endswith("/events"):
            assert params["series_ticker"] == "KXNFLGAME"
            assert params["status"] == "open"
            assert params["with_nested_markets"] == "true"
            return {
                "events": [
                    {
                        "event_ticker": "KXNFLGAME-26AUG15DALSEA",
                        "series_ticker": "KXNFLGAME",
                        "title": "Seattle at Dallas",
                        "markets": [market_payload()],
                    }
                ],
                "cursor": "",
            }
        if path.endswith("/markets"):
            assert params["status"] == "settled"
            assert params["series_ticker"] == "KXNFLGAME"
            assert "min_settled_ts" in params
            return {
                "markets": [
                    market_payload(
                        ticker=settled_ticker,
                        event_ticker="KXNFLGAME-26AUG08KCDET",
                        status="settled",
                        result="yes",
                        settlement_value_dollars="1.0000",
                        settlement_ts="2026-08-09T04:00:00Z",
                    ),
                ],
                "cursor": "",
            }
        raise AssertionError(f"unexpected path {path}")

    client = FakeKalshiClient(responder)
    result = fetch_structure(
        config=config,
        client=client,
        settled_floor=NOW - timedelta(hours=1),
    )
    assert [series["ticker"] for series in result.series] == ["KXNFLGAME"]
    assert len(result.open_events) == 1
    assert [market["ticker"] for market in result.settled_markets] == [settled_ticker]

    records = prepare_records(result, NOW)
    assert len(records["events"]) == 1
    assert len(records["settled_markets"]) == 1
    assert records["settled_markets"][0]["result"] == "yes"
    fingerprint = structural_fingerprint(records["events"], records["settled_markets"])
    assert fingerprint == structural_fingerprint(
        records["events"], records["settled_markets"]
    )


def test_prepare_records_prefers_open_feed_over_settled_sweep() -> None:
    config = KalshiMarketsConfig(series_patterns=("^KXNFL",))
    del config

    class Result:
        series = [{"ticker": "KXNFLGAME"}]
        open_events = [
            {
                "event_ticker": "KXNFLGAME-26AUG15DALSEA",
                "series_ticker": "KXNFLGAME",
                "title": "Seattle at Dallas",
                "markets": [market_payload()],
            }
        ]
        settled_markets = [market_payload(status="settled", result="yes")]

    records = prepare_records(Result(), NOW)
    assert len(records["settled_markets"]) == 0
    assert records["events"][0]["markets"][0]["status"] == "active"


def test_load_settled_floor_uses_checkpoint_with_overlap() -> None:
    config = KalshiMarketsConfig()
    checkpoint = {
        "query_fingerprint": query_fingerprint(config),
        "since_id": "2026-08-05T12:00:00+00:00",
    }
    floor = load_settled_floor(checkpoint, config, NOW)
    assert floor == datetime(2026, 8, 5, 11, 0, tzinfo=UTC)
    fresh = load_settled_floor({}, config, NOW)
    assert fresh == NOW - timedelta(hours=config.settled_initial_lookback_hours)


def test_normalize_book_derives_yes_asks_from_no_bids() -> None:
    orderbook_fp = {
        "yes_dollars": [["0.0100", "1000.00"], ["0.5700", "100.00"], ["0.5800", "50.00"]],
        "no_dollars": [["0.0100", "2000.00"], ["0.3900", "80.00"], ["0.4000", "60.00"]],
    }
    record = normalize_book(
        "KXNFLGAME-26AUG15DALSEA-SEA",
        orderbook_fp,
        depth_usdc=Decimal("10000"),
        observed_at=NOW,
    )
    assert record["best_bid"] == "0.5800"
    # Best NO bid 0.40 implies best YES ask at 0.60.
    assert record["best_ask"] == "0.6000"
    assert record["midpoint"] == "0.5900"
    assert record["spread"] == "0.0200"
    assert record["bids"][0] == {"price": "0.5800", "size": "50.00"}
    assert record["asks"][0] == {"price": "0.6000", "size": "60.00"}
    assert record["asks"][-1]["price"] == "0.9900"


def test_normalize_book_bounds_depth_and_flags_truncation() -> None:
    orderbook_fp = {
        "yes_dollars": [["0.1000", "1000.00"], ["0.5000", "100.00"]],
        "no_dollars": [],
    }
    record = normalize_book(
        "T",
        orderbook_fp,
        depth_usdc=Decimal("40"),
        observed_at=NOW,
    )
    # Best-first ordering retains the 0.50 level (50 notional) before the 0.10 level.
    assert len(record["bids"]) == 1
    assert record["bids"][0]["price"] == "0.5000"
    assert record["bid_truncated"] is True
    assert record["bid_captured_notional"] == "50.000000"
    assert record["bid_total_notional"] == "150.000000"
    assert record["best_ask"] is None
    assert record["midpoint"] is None


def test_fetch_books_batches_with_repeated_ticker_params() -> None:
    tickers = [f"T{i}" for i in range(5)]

    def responder(path: str, params: Any) -> dict[str, Any]:
        requested = [value for key, value in params if key == "tickers"]
        return {
            "orderbooks": [
                {
                    "ticker": ticker,
                    "orderbook_fp": {
                        "yes_dollars": [["0.5000", "10.00"]],
                        "no_dollars": [["0.4000", "10.00"]],
                    },
                }
                for ticker in requested
                if ticker != "T3"
            ]
        }

    client = FakeKalshiClient(responder)
    result = fetch_books(
        client=client,
        tickers=tickers,
        depth_usdc=Decimal("10000"),
        batch_size=2,
        observed_at=NOW,
        report_progress=False,
    )
    assert len(client.calls) == 3
    assert client.calls[0][1] == [("tickers", "T0"), ("tickers", "T1")]
    assert [book["ticker"] for book in result.books] == ["T0", "T1", "T2", "T4"]
    omitted = [batch["omitted_tickers"] for batch in result.request_batches]
    assert omitted == [[], ["T3"], []]


def trade_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "trade_id": "3b983763-7d28-5d89-f79a-c97175211961",
        "ticker": "KXNFLGAME-26AUG15DALSEA-SEA",
        "count_fp": "2.50",
        "yes_price_dollars": "0.6000",
        "no_price_dollars": "0.4000",
        "taker_outcome_side": "yes",
        "taker_book_side": "bid",
        "created_time": "2026-08-05T17:59:00Z",
        "is_block_trade": False,
    }
    payload.update(overrides)
    return payload


def test_normalize_trade_maps_fields_and_falls_back_to_taker_side() -> None:
    record = normalize_trade(trade_payload(), NOW)
    assert record["trade_id"] == "3b983763-7d28-5d89-f79a-c97175211961"
    assert record["count"] == "2.50"
    assert record["yes_price"] == "0.6000"
    assert record["taker_outcome_side"] == "yes"
    assert record["is_block_trade"] is False

    legacy = trade_payload()
    del legacy["taker_outcome_side"]
    legacy["taker_side"] = "no"
    record = normalize_trade(legacy, NOW)
    assert record["taker_outcome_side"] == "no"


def test_fetch_new_trades_filters_pages_and_stops_at_floor() -> None:
    config = KalshiTradesConfig(page_limit=2)
    floor = NOW - timedelta(minutes=10)
    pages = {
        None: {
            "trades": [
                trade_payload(trade_id="t1", created_time="2026-08-05T17:59:00Z"),
                trade_payload(
                    trade_id="t2",
                    ticker="KXBTC-UNTRACKED",
                    created_time="2026-08-05T17:58:00Z",
                ),
            ],
            "cursor": "page2",
        },
        "page2": {
            "trades": [
                trade_payload(trade_id="t1", created_time="2026-08-05T17:59:00Z"),
                trade_payload(trade_id="t3", created_time="2026-08-05T17:40:00Z"),
            ],
            "cursor": "page3",
        },
        "page3": {"trades": [], "cursor": ""},
    }

    def responder(path: str, params: Any) -> dict[str, Any]:
        assert params["min_ts"] == str(int(floor.timestamp()))
        return pages[params.get("cursor")]

    client = FakeKalshiClient(responder)
    result = fetch_new_trades(
        config=config,
        client=client,
        tracked_tickers={"KXNFLGAME-26AUG15DALSEA-SEA"},
        floor=floor,
        observed_at=NOW,
    )
    # Page 2's oldest trade (17:40) is behind the floor, so page 3 is never fetched.
    assert len(client.calls) == 2
    assert [trade["trade_id"] for trade in result.trades] == ["t1"]
    assert result.request_pages[0]["tracked_trade_count"] == 1
    assert result.request_pages[1]["tracked_trade_count"] == 0


def test_fetch_new_trades_sorts_ascending() -> None:
    config = KalshiTradesConfig()
    floor = NOW - timedelta(hours=1)

    def responder(path: str, params: Any) -> dict[str, Any]:
        return {
            "trades": [
                trade_payload(trade_id="new", created_time="2026-08-05T17:59:00Z"),
                trade_payload(trade_id="old", created_time="2026-08-05T17:30:00Z"),
            ],
            "cursor": "",
        }

    client = FakeKalshiClient(responder)
    result = fetch_new_trades(
        config=config,
        client=client,
        tracked_tickers={"KXNFLGAME-26AUG15DALSEA-SEA"},
        floor=floor,
        observed_at=NOW,
    )
    assert [trade["trade_id"] for trade in result.trades] == ["old", "new"]


def test_load_watermark_requires_matching_fingerprint() -> None:
    config = KalshiTradesConfig()
    from src.ingest_odds.kalshi_trades_pipeline import query_fingerprint as trades_fp

    checkpoint = {
        "query_fingerprint": trades_fp(config),
        "since_id": "2026-08-05T17:00:00+00:00",
    }
    assert load_watermark(checkpoint, config, NOW) == datetime(
        2026, 8, 5, 17, 0, tzinfo=UTC
    )
    stale = {"query_fingerprint": "different", "since_id": "2026-08-05T17:00:00+00:00"}
    assert load_watermark(stale, config, NOW) == NOW - timedelta(
        minutes=config.initial_lookback_minutes
    )


def test_trades_config_validation() -> None:
    with pytest.raises(ValueError):
        KalshiTradesConfig(page_limit=0)
    with pytest.raises(ValueError):
        KalshiTradesConfig(overlap_seconds=-1)
    with pytest.raises(ValueError):
        KalshiMarketsConfig(series_patterns=())
