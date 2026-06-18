"""
Binance public market-data WebSocket client.
"""

from __future__ import annotations

from typing import Any

import websockets

from libs.common import CircuitBreaker, MessageBus
from services.ingestion.exchanges.base import (
    ConnectFactory,
    ExchangeWebSocketClient,
    SleepFn,
    TimeFn,
)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"


class BinanceWebSocketClient(ExchangeWebSocketClient):
    def __init__(
        self,
        *,
        bus: MessageBus,
        stream_name: str = "btcusdt@trade",
        connect_factory: ConnectFactory | None = None,
        url: str = BINANCE_WS_URL,
        reconnect_backoff_seconds: float = 1.0,
        max_reconnects: int = 3,
        heartbeat_timeout_seconds: float | None = 30.0,
        circuit_breaker: CircuitBreaker | None = None,
        sleep_fn: SleepFn | None = None,
        time_fn: TimeFn | None = None,
    ) -> None:
        effective_url = f"{url}/{stream_name}" if not url.endswith(stream_name) else url
        super().__init__(
            bus=bus,
            connect_factory=connect_factory or websockets.connect,
            url=effective_url,
            source="binance",
            reconnect_backoff_seconds=reconnect_backoff_seconds,
            max_reconnects=max_reconnects,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
            circuit_breaker=circuit_breaker,
            sleep_fn=sleep_fn,
            time_fn=time_fn,
        )

    def _extract_market_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        if "data" in payload and isinstance(payload["data"], dict):
            payload = payload["data"]

        event_type = payload.get("e")
        if event_type in {"trade", "24hrTicker"}:
            return payload

        if "result" in payload:
            return None

        raise ValueError(f"Unsupported Binance payload shape: {payload!r}")
