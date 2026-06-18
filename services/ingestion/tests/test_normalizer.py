from datetime import UTC, datetime

import pytest

from services.ingestion.normalizer import normalize_market_payload


def test_normalize_replay_payload_to_market_event() -> None:
    event = normalize_market_payload(
        {
            "kind": "replay",
            "source": "replay.binance",
            "symbol": "BTCUSDT",
            "event_type": "trade",
            "price": 42000.25,
            "volume": 0.15,
            "ts": "2024-01-01T00:00:00+00:00",
        }
    )

    assert event.symbol == "BTCUSDT"
    assert event.source == "replay.binance"
    assert event.event_type == "trade"
    assert event.price == 42000.25
    assert event.volume == 0.15
    assert event.ts == datetime(2024, 1, 1, tzinfo=UTC)


def test_normalize_binance_payload_to_market_event() -> None:
    event = normalize_market_payload(
        {
            "e": "trade",
            "s": "ETHUSDT",
            "p": "3500.1",
            "q": "0.5",
            "T": 1704067200000,
        }
    )

    assert event.source == "binance"
    assert event.symbol == "ETHUSDT"
    assert event.price == 3500.1
    assert event.volume == 0.5


def test_normalize_coinbase_payload_to_market_event() -> None:
    event = normalize_market_payload(
        {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": "42100.2",
            "best_bid": "42100.0",
            "best_ask": "42100.4",
            "time": "2024-01-01T00:00:02Z",
        }
    )

    assert event.source == "coinbase"
    assert event.symbol == "BTCUSD"
    assert event.bid == 42100.0
    assert event.ask == 42100.4


def test_normalize_rejects_unknown_payload_shape() -> None:
    with pytest.raises(ValueError):
        normalize_market_payload({"foo": "bar"})
