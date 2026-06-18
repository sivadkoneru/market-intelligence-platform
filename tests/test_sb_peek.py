from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from libs.common import InMemoryBus, ReceivedMessage
from scripts import sb_peek


class _FakeLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def info(self, event: str, **kwargs: object) -> None:
        self.events.append((event, kwargs))


class _FakeBus:
    def __init__(self, messages: list[ReceivedMessage]) -> None:
        self.messages = messages
        self.peek_calls: list[tuple[str, str, int]] = []
        self.closed = False

    async def peek(self, topic: str, subscription: str, n: int = 10) -> list[ReceivedMessage]:
        self.peek_calls.append((topic, subscription, n))
        return self.messages[:n]

    async def close(self) -> None:
        self.closed = True


def test_resolve_subscription_prefers_api_subscription() -> None:
    subscription = sb_peek.resolve_subscription(
        "market.raw",
        requested_subscription=None,
        config_path=sb_peek.SB_CONFIG_PATH,
    )

    assert subscription == "api"


def test_resolve_subscription_honours_explicit_value() -> None:
    subscription = sb_peek.resolve_subscription(
        "market.raw",
        requested_subscription="stream",
        config_path=sb_peek.SB_CONFIG_PATH,
    )

    assert subscription == "stream"


def test_load_topic_subscriptions_reads_servicebus_config() -> None:
    subscriptions = sb_peek.load_topic_subscriptions(sb_peek.SB_CONFIG_PATH)

    assert subscriptions["market.raw"] == ["stream", "api", "api-ws"]
    assert subscriptions["alerts"] == ["api", "api-ws"]


def test_build_parser_parses_messages_and_subscription() -> None:
    args = sb_peek.build_parser().parse_args(["market.raw", "--subscription", "api-ws", "-n", "5"])

    assert args.topic == "market.raw"
    assert args.subscription == "api-ws"
    assert args.messages == 5
    assert args.allow_offline is False


def test_build_parser_parses_allow_offline() -> None:
    args = sb_peek.build_parser().parse_args(["market.raw", "--allow-offline"])

    assert args.allow_offline is True


def test_peek_messages_logs_payloads_and_closes_bus(monkeypatch) -> None:
    fake_logger = _FakeLogger()
    fake_bus = _FakeBus(
        [
            ReceivedMessage(
                topic="market.raw",
                subscription="api",
                body={"symbol": "BTCUSDT"},
                message_id="m-1",
                correlation_id="c-1",
            )
        ]
    )

    monkeypatch.setattr(sb_peek, "get_logger", lambda name: fake_logger)
    monkeypatch.setattr(
        sb_peek,
        "get_settings",
        lambda: type(
            "S",
            (),
            {"service_bus_connection_string": "real"},
        )(),
    )
    monkeypatch.setattr(sb_peek, "get_message_bus", lambda settings=None: fake_bus)

    messages = asyncio.run(sb_peek.peek_messages("market.raw", subscription="api", messages=3))

    assert [event for event, _ in fake_logger.events] == ["sb_peek.message"]
    assert fake_bus.peek_calls == [("market.raw", "api", 3)]
    assert fake_bus.closed is True
    assert messages[0].body == {"symbol": "BTCUSDT"}


def test_peek_messages_rejects_inmemory_placeholder_by_default(monkeypatch) -> None:
    monkeypatch.setattr(
        sb_peek,
        "get_settings",
        lambda: type(
            "S",
            (),
            {
                "service_bus_connection_string": "Endpoint=sb://localhost;SharedAccessKey=SAS_KEY_VALUE_HERE;UseDevelopmentEmulator=true;"
            },
        )(),
    )
    monkeypatch.setattr(sb_peek, "get_message_bus", lambda settings=None: InMemoryBus())

    with pytest.raises(RuntimeError, match="in-memory placeholder"):
        asyncio.run(sb_peek.peek_messages("market.raw", subscription="api", messages=1))


def test_peek_messages_allows_inmemory_placeholder_when_requested(monkeypatch) -> None:
    fake_logger = _FakeLogger()

    monkeypatch.setattr(sb_peek, "get_logger", lambda name: fake_logger)
    monkeypatch.setattr(
        sb_peek,
        "get_settings",
        lambda: type(
            "S",
            (),
            {
                "service_bus_connection_string": "Endpoint=sb://localhost;SharedAccessKey=SAS_KEY_VALUE_HERE;UseDevelopmentEmulator=true;"
            },
        )(),
    )
    monkeypatch.setattr(sb_peek, "get_message_bus", lambda settings=None: InMemoryBus())

    messages = asyncio.run(
        sb_peek.peek_messages(
            "market.raw",
            subscription="api",
            messages=1,
            allow_offline=True,
        )
    )

    assert messages == []
    assert [event for event, _ in fake_logger.events] == ["sb_peek.offline_bus"]


def test_resolve_subscription_raises_for_unknown_topic(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"UserConfig":{"Namespaces":[{"Topics":[]}]} }', encoding="utf-8")

    with pytest.raises(ValueError):
        sb_peek.resolve_subscription(
            "missing.topic",
            requested_subscription=None,
            config_path=config_path,
        )
