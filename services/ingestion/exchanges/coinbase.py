"""
Coinbase public market-data WebSocket client.
"""

from __future__ import annotations

import json
from typing import Any

import websockets

from libs.common import CircuitBreaker, MessageBus
from services.ingestion.exchanges.base import (
    ConnectFactory,
    ExchangeWebSocketClient,
    SleepFn,
    TimeFn,
    WebSocketConnection,
)

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"


class CoinbaseWebSocketClient(ExchangeWebSocketClient):
    def __init__(
        self,
        *,
        bus: MessageBus,
        product_ids: list[str],
        connect_factory: ConnectFactory | None = None,
        url: str = COINBASE_WS_URL,
        reconnect_backoff_seconds: float = 1.0,
        max_reconnects: int = 3,
        heartbeat_timeout_seconds: float | None = 30.0,
        circuit_breaker: CircuitBreaker | None = None,
        sleep_fn: SleepFn | None = None,
        time_fn: TimeFn | None = None,
    ) -> None:
        self._product_ids = [
            self._normalize_product_id(product_id) for product_id in product_ids
        ]
        super().__init__(
            bus=bus,
            connect_factory=connect_factory or websockets.connect,
            url=url,
            source="coinbase",
            reconnect_backoff_seconds=reconnect_backoff_seconds,
            max_reconnects=max_reconnects,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
            circuit_breaker=circuit_breaker,
            sleep_fn=sleep_fn,
            time_fn=time_fn,
        )

    async def _on_connected(self, websocket: WebSocketConnection) -> None:
        await websocket.send(
            json.dumps(
                {
                    "type": "subscribe",
                    "product_ids": self._product_ids,
                    "channels": ["ticker"],
                }
            )
        )

    def _extract_market_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        payload_type = payload.get("type")
        if payload_type == "ticker":
            return payload
        if payload_type in {"subscriptions", "heartbeat"}:
            return None
        raise ValueError(f"Unsupported Coinbase payload shape: {payload!r}")

    def _is_heartbeat_payload(self, payload: dict[str, Any]) -> bool:
        return payload.get("type") == "heartbeat"

    @staticmethod
    def _normalize_product_id(value: str) -> str:
        normalized = value.upper()
        if "-" in normalized:
            return normalized
        if normalized.endswith("USD"):
            return f"{normalized[:-3]}-USD"
        return normalized
