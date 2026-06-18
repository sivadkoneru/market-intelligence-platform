from __future__ import annotations

import asyncio
import json

import pytest

from scripts import ws_smoke


class _FakeLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def info(self, event: str, **kwargs: object) -> None:
        self.events.append((event, kwargs))


class _FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self._responses = iter(
            [
                json.dumps({"type": "subscribed", "symbols": ["BTCUSDT", "ETHUSDT"]}),
                json.dumps({"type": "market", "symbol": "BTCUSDT"}),
                json.dumps({"type": "alert", "symbol": "ETHUSDT"}),
            ]
        )

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def recv(self) -> str:
        return next(self._responses)


class _FakeConnection:
    def __init__(self, websocket: _FakeWebSocket) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> _FakeWebSocket:
        return self.websocket

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _HangingWebSocket:
    async def send(self, payload: str) -> None:
        return None

    async def recv(self) -> str:
        await asyncio.sleep(1)
        return ""


def test_build_subscribe_payload_normalises_symbols() -> None:
    payload = ws_smoke.build_subscribe_payload([" btcusdt ", "ETHUSDT", ""])

    assert payload == {"action": "subscribe", "symbols": ["BTCUSDT", "ETHUSDT"]}


def test_parser_only_uses_default_symbol_when_omitted() -> None:
    default_args = ws_smoke.build_parser().parse_args([])
    explicit_args = ws_smoke.build_parser().parse_args(
        ["--symbol", "ETHUSDT", "--messages", "2", "--timeout-seconds", "0.5"]
    )

    assert default_args.symbols is None
    assert default_args.url == "ws://127.0.0.1:8000/ws/stream"
    assert default_args.messages == 1
    assert default_args.timeout_seconds == 15.0
    assert explicit_args.symbols == ["ETHUSDT"]
    assert explicit_args.messages == 2
    assert explicit_args.timeout_seconds == 0.5


def test_run_smoke_sends_subscribe_and_logs_messages(monkeypatch) -> None:
    fake_logger = _FakeLogger()
    fake_websocket = _FakeWebSocket()

    monkeypatch.setattr(ws_smoke, "configure_logging", lambda: None)
    monkeypatch.setattr(ws_smoke, "get_logger", lambda name: fake_logger)
    monkeypatch.setattr(
        ws_smoke.websockets,
        "connect",
        lambda url: _FakeConnection(fake_websocket),
    )

    asyncio.run(
        ws_smoke.run_smoke(
            "ws://example.test/ws/stream",
            ["BTCUSDT", "ethusdt"],
            2,
            timeout_seconds=0.5,
        )
    )

    assert fake_websocket.sent == [
        json.dumps({"action": "subscribe", "symbols": ["BTCUSDT", "ETHUSDT"]})
    ]
    assert [event for event, _ in fake_logger.events] == [
        "ws_smoke.subscribed",
        "ws_smoke.message",
        "ws_smoke.message",
    ]


def test_run_smoke_times_out_when_no_ack_arrives(monkeypatch) -> None:
    monkeypatch.setattr(ws_smoke, "configure_logging", lambda: None)
    monkeypatch.setattr(ws_smoke, "get_logger", lambda name: _FakeLogger())
    monkeypatch.setattr(
        ws_smoke.websockets,
        "connect",
        lambda url: _FakeConnection(_HangingWebSocket()),
    )

    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(
            ws_smoke.run_smoke(
                "ws://example.test/ws/stream",
                ["BTCUSDT"],
                1,
                timeout_seconds=0.01,
            )
        )
