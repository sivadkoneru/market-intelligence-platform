"""
Normalization helpers for ingestion payloads.

The deterministic replay feed uses a compact internal payload shape, but this
module also accepts small Binance/Coinbase-style dictionaries so the service
can evolve without duplicating schema logic.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from libs.common import MarketEvent

_BINANCE_TRADE_EVENT = "trade"
_COINBASE_TICKER_EVENT = "ticker"


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), tz=UTC)
    elif isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    else:
        raise ValueError(f"Unsupported timestamp value: {value!r}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def normalize_market_payload(
    payload: dict[str, Any], *, source_override: str | None = None
) -> MarketEvent:
    """
    Convert a replay or exchange payload into the shared ``MarketEvent`` schema.

    Supported payload families:
    - deterministic replay events
    - Binance trade/ticker-like payloads
    - Coinbase ticker-like payloads
    """
    if "kind" in payload and payload["kind"] == "replay":
        return MarketEvent(
            symbol=str(payload["symbol"]).upper(),
            source=source_override or str(payload.get("source", "replay")),
            event_type=str(payload.get("event_type", "trade")),
            price=float(payload["price"]),
            volume=_optional_float(payload.get("volume")),
            bid=_optional_float(payload.get("bid")),
            ask=_optional_float(payload.get("ask")),
            ts=_coerce_datetime(payload["ts"]),
            correlation_id=payload.get("correlation_id"),
            trace_id=payload.get("trace_id"),
        )

    if payload.get("e") in {_BINANCE_TRADE_EVENT, "24hrTicker"}:
        event_type = "trade" if payload.get("e") == _BINANCE_TRADE_EVENT else "ticker"
        event_ts = payload.get("T", payload.get("E"))
        symbol = str(payload["s"]).upper()
        return MarketEvent(
            symbol=symbol,
            source=source_override or "binance",
            event_type=event_type,
            price=float(payload.get("p") or payload.get("c")),
            volume=_optional_float(payload.get("q") or payload.get("v")),
            bid=_optional_float(payload.get("b")),
            ask=_optional_float(payload.get("a")),
            ts=_coerce_datetime(float(event_ts) / 1000.0),
        )

    if payload.get("type") == _COINBASE_TICKER_EVENT:
        product_id = str(payload["product_id"]).upper().replace("-", "")
        return MarketEvent(
            symbol=product_id,
            source=source_override or "coinbase",
            event_type="ticker",
            price=float(payload["price"]),
            volume=_optional_float(payload.get("last_size") or payload.get("volume_24h")),
            bid=_optional_float(payload.get("best_bid")),
            ask=_optional_float(payload.get("best_ask")),
            ts=_coerce_datetime(payload["time"]),
        )

    raise ValueError(f"Unsupported market payload shape: {payload!r}")


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
