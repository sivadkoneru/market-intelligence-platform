"""Tests for libs/common/schema.py — model validation, JSON round-trip, idempotency key."""

from datetime import datetime, timezone

import pytest

from libs.common.schema import (
    TOPIC_ALERTS,
    TOPIC_INSIGHTS,
    TOPIC_MARKET_RAW,
    TOPIC_NEWS_RAW,
    TOPIC_SIGNALS,
    Alert,
    Insight,
    MarketEvent,
    NewsEvent,
    Signal,
    market_event_key,
)

# ---------------------------------------------------------------------------
# Topic constants
# ---------------------------------------------------------------------------


def test_topic_constants():
    assert TOPIC_MARKET_RAW == "market.raw"
    assert TOPIC_NEWS_RAW == "news.raw"
    assert TOPIC_SIGNALS == "signals"
    assert TOPIC_INSIGHTS == "insights"
    assert TOPIC_ALERTS == "alerts"


# ---------------------------------------------------------------------------
# MarketEvent
# ---------------------------------------------------------------------------


def test_market_event_defaults_are_tz_aware():
    ev = MarketEvent(symbol="BTCUSDT", source="binance", event_type="trade", price=60_000.0)
    assert ev.ts.tzinfo is not None
    assert ev.ts.tzinfo == timezone.utc


def test_market_event_fields():
    ev = MarketEvent(
        symbol="ETHUSD",
        source="coinbase",
        event_type="ticker",
        price=3_000.0,
        volume=10.5,
        bid=2_999.0,
        ask=3_001.0,
    )
    assert ev.symbol == "ETHUSD"
    assert ev.source == "coinbase"
    assert ev.event_type == "ticker"
    assert ev.price == 3_000.0
    assert ev.volume == 10.5
    assert ev.bid == 2_999.0
    assert ev.ask == 3_001.0


def test_market_event_optional_volume_defaults_none():
    ev = MarketEvent(symbol="BTCUSDT", source="binance", event_type="trade", price=1.0)
    assert ev.volume is None
    assert ev.bid is None
    assert ev.ask is None


def test_market_event_has_event_id():
    ev = MarketEvent(symbol="X", source="y", event_type="trade", price=1.0)
    assert isinstance(ev.event_id, str) and len(ev.event_id) > 0


def test_market_event_missing_required_raises():
    with pytest.raises(Exception):
        MarketEvent(symbol="BTCUSDT", source="binance", event_type="trade")  # price missing


def test_market_event_json_roundtrip():
    ev = MarketEvent(symbol="BTCUSDT", source="binance", event_type="trade", price=50_000.0)
    raw = ev.model_dump_json()
    ev2 = MarketEvent.model_validate_json(raw)
    assert ev2.symbol == ev.symbol
    assert ev2.price == ev.price
    # Timestamp survives the round-trip with tz info
    assert ev2.ts.tzinfo is not None
    assert ev2.ts == ev.ts


def test_market_event_correlation_id_default_none():
    ev = MarketEvent(symbol="X", source="y", event_type="trade", price=1.0)
    assert ev.correlation_id is None
    assert ev.trace_id is None


def test_market_event_correlation_id_set():
    ev = MarketEvent(
        symbol="X",
        source="y",
        event_type="trade",
        price=1.0,
        correlation_id="corr-123",
        trace_id="trace-abc",
    )
    assert ev.correlation_id == "corr-123"
    assert ev.trace_id == "trace-abc"


# ---------------------------------------------------------------------------
# NewsEvent
# ---------------------------------------------------------------------------


def test_news_event_valid():
    ev = NewsEvent(
        source="reuters",
        title="BTC hits ATH",
        body="Bitcoin reached a new all-time high...",
        symbols=["BTCUSDT", "BTC"],
    )
    assert ev.source == "reuters"
    assert ev.symbols == ["BTCUSDT", "BTC"]
    assert ev.url is None
    assert ev.author is None


def test_news_event_with_optional_fields():
    ev = NewsEvent(
        source="reuters",
        title="ETH update",
        body="Details here.",
        url="https://reuters.com/eth",
        symbols=["ETHUSD"],
        author="Jane Doe",
    )
    assert ev.url == "https://reuters.com/eth"
    assert ev.author == "Jane Doe"


def test_news_event_missing_required_raises():
    with pytest.raises(Exception):
        NewsEvent(source="x", symbols=["X"])  # title + body missing


def test_news_event_json_roundtrip():
    ev = NewsEvent(
        source="hackernews",
        title="Test",
        body="Body text",
        symbols=["BTCUSDT"],
    )
    ev2 = NewsEvent.model_validate_json(ev.model_dump_json())
    assert ev2.title == ev.title
    assert ev2.ts == ev.ts
    assert ev2.ts.tzinfo is not None


# ---------------------------------------------------------------------------
# Signal
# ---------------------------------------------------------------------------


def test_signal_valid():
    sig = Signal(
        symbol="BTCUSDT",
        indicators={
            "sma": 59_000.0,
            "ema": 59_200.0,
            "rsi": 65.0,
            "volatility": 0.02,
            "trend": 1.0,
            "anomaly_score": 0.1,
        },
    )
    assert sig.symbol == "BTCUSDT"
    assert sig.source == "stream"
    assert sig.anomaly is False


def test_signal_anomaly_flag():
    sig = Signal(symbol="X", indicators={"sma": None}, anomaly=True)
    assert sig.anomaly is True


def test_signal_json_roundtrip():
    sig = Signal(symbol="ETHUSD", indicators={"rsi": 70.0, "sma": None})
    sig2 = Signal.model_validate_json(sig.model_dump_json())
    assert sig2.symbol == sig.symbol
    assert sig2.indicators["rsi"] == 70.0
    assert sig2.indicators["sma"] is None


def test_signal_missing_symbol_raises():
    with pytest.raises(Exception):
        Signal(indicators={})  # symbol missing


# ---------------------------------------------------------------------------
# Insight
# ---------------------------------------------------------------------------


def test_insight_valid():
    ins = Insight(
        symbol="BTCUSDT",
        sentiment_score=0.75,
        sentiment_label="positive",
        summary="BTC looks bullish.",
        explanation="Strong volume and positive news.",
        confidence=0.9,
        grounded=True,
        model="gpt-4o",
    )
    assert ins.citations == []
    assert ins.grounded is True


def test_insight_with_citations():
    ins = Insight(
        symbol="BTCUSDT",
        sentiment_score=0.5,
        sentiment_label="neutral",
        summary="s",
        explanation="e",
        citations=["https://example.com/1"],
        confidence=0.8,
        grounded=True,
        model="mock",
    )
    assert len(ins.citations) == 1


def test_insight_json_roundtrip():
    ins = Insight(
        symbol="ETH",
        sentiment_score=-0.3,
        sentiment_label="negative",
        summary="s",
        explanation="e",
        confidence=0.6,
        grounded=False,
        model="mock",
    )
    ins2 = Insight.model_validate_json(ins.model_dump_json())
    assert ins2.sentiment_score == ins.sentiment_score
    assert ins2.ts == ins.ts


# ---------------------------------------------------------------------------
# Alert
# ---------------------------------------------------------------------------


def test_alert_valid():
    a = Alert(
        symbol="BTCUSDT",
        rule="rsi_overbought",
        severity="high",
        message="RSI crossed 80",
        dedupe_key="BTCUSDT:rsi_overbought:2024-01-01T00:00:00",
    )
    assert a.symbol == "BTCUSDT"
    assert a.severity == "high"


def test_alert_missing_required_raises():
    with pytest.raises(Exception):
        Alert(symbol="X", rule="r")  # severity/message/dedupe_key missing


def test_alert_json_roundtrip():
    a = Alert(
        symbol="X", rule="r", severity="low", message="msg", dedupe_key="key"
    )
    a2 = Alert.model_validate_json(a.model_dump_json())
    assert a2.dedupe_key == a.dedupe_key
    assert a2.ts == a.ts
    assert a2.ts.tzinfo is not None


# ---------------------------------------------------------------------------
# market_event_key — determinism
# ---------------------------------------------------------------------------


def test_market_event_key_is_deterministic():
    ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    k1 = market_event_key("BTCUSDT", ts, "binance")
    k2 = market_event_key("BTCUSDT", ts, "binance")
    assert k1 == k2


def test_market_event_key_differs_on_symbol():
    ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert market_event_key("BTCUSDT", ts, "binance") != market_event_key(
        "ETHUSD", ts, "binance"
    )


def test_market_event_key_differs_on_ts():
    ts1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts2 = datetime(2024, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
    assert market_event_key("BTCUSDT", ts1, "binance") != market_event_key(
        "BTCUSDT", ts2, "binance"
    )


def test_market_event_key_differs_on_source():
    ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert market_event_key("BTCUSDT", ts, "binance") != market_event_key(
        "BTCUSDT", ts, "coinbase"
    )


def test_market_event_key_is_string():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    k = market_event_key("X", ts, "y")
    assert isinstance(k, str) and len(k) > 0
