from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from libs.common import TOPIC_MARKET_RAW, InMemoryBus
from scripts import seed_market_data
from services.stream.service import STREAM_SUBSCRIPTION


class _FakeLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def info(self, event: str, **kwargs: object) -> None:
        self.events.append((event, kwargs))


def test_build_parser_defaults_to_local_seed_values() -> None:
    args = seed_market_data.build_parser().parse_args([])

    assert args.events == 15
    assert args.symbols is None
    assert args.base_ts == "2026-01-01T00:00:00+00:00"
    assert args.source_prefix == "seed.local"
    assert args.allow_offline is False


def test_build_market_event_payload_uses_symbol_price_and_run_id() -> None:
    payload, message_id = seed_market_data.build_market_event_payload(
        1,
        symbols=("BTCUSDT", "ETHUSDT"),
        base_ts=datetime(2026, 1, 1, tzinfo=UTC),
        run_id="test-run",
        source_prefix="seed.test",
    )

    assert message_id == "seed-test-run-0001"
    assert payload["event_id"] == "ev-seed-test-run-0001"
    assert payload["symbol"] == "ETHUSDT"
    assert payload["source"] == "seed.test.test-run"
    assert payload["event_type"] == "trade"
    assert payload["price"] == 2251.5
    assert payload["correlation_id"] == "corr-seed-test-run-0001"


def test_publish_seed_data_publishes_market_raw_events_and_logs(monkeypatch) -> None:
    bus = InMemoryBus()
    fake_logger = _FakeLogger()

    async def run() -> list[object]:
        await bus.receive(TOPIC_MARKET_RAW, STREAM_SUBSCRIPTION, max_messages=0)
        monkeypatch.setattr(seed_market_data, "get_message_bus", lambda settings=None: bus)
        monkeypatch.setattr(seed_market_data, "get_logger", lambda name: fake_logger)
        monkeypatch.setattr(
            seed_market_data,
            "get_settings",
            lambda: type("S", (), {"service_bus_connection_string": "placeholder"})(),
        )

        symbols = await seed_market_data.publish_seed_data(
            events=3,
            symbols=("btcusdt", "ETHUSDT"),
            base_ts=datetime(2026, 1, 1, tzinfo=UTC),
            run_id="unit",
            source_prefix="seed.test",
            allow_offline=True,
        )
        messages = await bus.peek(TOPIC_MARKET_RAW, STREAM_SUBSCRIPTION, n=5)
        return [symbols, messages]

    symbols, messages = asyncio.run(run())

    assert symbols == ["BTCUSDT", "ETHUSDT"]
    assert [message.message_id for message in messages] == [
        "seed-unit-0000",
        "seed-unit-0001",
        "seed-unit-0002",
    ]
    assert messages[0].body["symbol"] == "BTCUSDT"
    assert messages[1].body["symbol"] == "ETHUSDT"
    assert [event for event, _ in fake_logger.events] == ["seed_market_data.published"]


def test_publish_seed_data_rejects_inmemory_placeholder_by_default(monkeypatch) -> None:
    monkeypatch.setattr(seed_market_data, "get_message_bus", lambda settings=None: InMemoryBus())
    monkeypatch.setattr(
        seed_market_data,
        "get_settings",
        lambda: type("S", (), {"service_bus_connection_string": "placeholder"})(),
    )

    with pytest.raises(RuntimeError, match="in-memory placeholder"):
        asyncio.run(
            seed_market_data.publish_seed_data(
                events=1,
                symbols=("BTCUSDT",),
                base_ts=datetime(2026, 1, 1, tzinfo=UTC),
                run_id="unit",
                source_prefix="seed.test",
            )
        )


def test_publish_seed_data_requires_at_least_one_symbol() -> None:
    with pytest.raises(ValueError, match="at least one symbol"):
        asyncio.run(
            seed_market_data.publish_seed_data(
                events=1,
                symbols=(),
                base_ts=datetime(2026, 1, 1, tzinfo=UTC),
                run_id="unit",
                source_prefix="seed.test",
                allow_offline=True,
            )
        )
