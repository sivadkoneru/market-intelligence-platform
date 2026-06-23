"""Publish deterministic sample market events to ``market.raw`` for local API testing."""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Sequence

from libs.common import (
    TOPIC_MARKET_RAW,
    InMemoryBus,
    MarketEvent,
    configure_logging,
    get_logger,
    get_message_bus,
    get_settings,
)

DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
DEFAULT_BASE_TS = "2026-01-01T00:00:00+00:00"


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def _parse_base_ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish sample MarketEvent payloads to the local market.raw topic.",
    )
    parser.add_argument(
        "--events",
        type=_positive_int,
        default=15,
        help="Number of market events to publish.",
    )
    parser.add_argument(
        "--symbol",
        dest="symbols",
        action="append",
        default=None,
        help="Symbol to include. Repeat for multiple symbols.",
    )
    parser.add_argument(
        "--base-ts",
        default=DEFAULT_BASE_TS,
        help="ISO-8601 timestamp for the first event.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Unique run id used in event ids, message ids, and source names.",
    )
    parser.add_argument(
        "--source-prefix",
        default="seed.local",
        help="Source prefix for generated events.",
    )
    parser.add_argument(
        "--connection-string",
        default=None,
        help="Override SERVICE_BUS_CONNECTION_STRING for this invocation.",
    )
    parser.add_argument(
        "--allow-offline",
        action="store_true",
        help="Allow the in-memory placeholder bus. Useful for tests only.",
    )
    return parser


def build_market_event_payload(
    index: int,
    *,
    symbols: Sequence[str],
    base_ts: datetime,
    run_id: str,
    source_prefix: str,
) -> tuple[dict[str, Any], str]:
    symbol = symbols[index % len(symbols)].strip().upper()
    cycle = index // len(symbols)
    base_prices = {
        "BTCUSDT": 42000.0,
        "ETHUSDT": 2250.0,
        "ETHUSD": 2250.0,
        "SOLUSDT": 110.0,
    }
    pattern = (0.0, 1.5, 3.0, 6.0, 12.0, 24.0, 8.0, 28.0)
    price = base_prices.get(symbol, 100.0) + (cycle * 2.25) + pattern[index % len(pattern)]
    message_id = f"seed-{run_id}-{index:04d}"
    event = MarketEvent(
        event_id=f"ev-{message_id}",
        ts=base_ts + timedelta(seconds=index),
        symbol=symbol,
        source=f"{source_prefix}.{run_id}",
        event_type="trade",
        price=round(price, 4),
        volume=round(1.0 + ((index % 5) * 0.1), 4),
        bid=round(price - 0.25, 4),
        ask=round(price + 0.25, 4),
        correlation_id=f"corr-{message_id}",
        trace_id=f"trace-{message_id}",
    )
    return event.model_dump(mode="json"), message_id


async def publish_seed_data(
    *,
    events: int,
    symbols: Sequence[str],
    base_ts: datetime,
    run_id: str,
    source_prefix: str,
    connection_string: str | None = None,
    allow_offline: bool = False,
) -> list[str]:
    if not symbols:
        raise ValueError("at least one symbol is required")

    settings = get_settings()
    resolved_connection_string = connection_string or settings.service_bus_connection_string
    bus = get_message_bus(SimpleNamespace(service_bus_connection_string=resolved_connection_string))
    log = get_logger(__name__)
    published_symbols: set[str] = set()

    try:
        if isinstance(bus, InMemoryBus) and not allow_offline:
            raise RuntimeError(
                "SERVICE_BUS_CONNECTION_STRING resolves to the in-memory placeholder; "
                "start compose or pass --connection-string for the Service Bus emulator."
            )

        for index in range(events):
            payload, message_id = build_market_event_payload(
                index,
                symbols=symbols,
                base_ts=base_ts,
                run_id=run_id,
                source_prefix=source_prefix,
            )
            await bus.publish(
                TOPIC_MARKET_RAW,
                payload,
                message_id=message_id,
                correlation_id=str(payload.get("correlation_id") or ""),
            )
            published_symbols.add(str(payload["symbol"]))

        log.info(
            "seed_market_data.published",
            topic=TOPIC_MARKET_RAW,
            events=events,
            symbols=sorted(published_symbols),
            run_id=run_id,
            source=f"{source_prefix}.{run_id}",
        )
        return sorted(published_symbols)
    finally:
        close = getattr(bus, "close", None)
        if close is not None:
            await close()


async def run() -> int:
    configure_logging()
    args = build_parser().parse_args()
    run_id = args.run_id or datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S")
    symbols = tuple(
        symbol.strip().upper()
        for symbol in (args.symbols or DEFAULT_SYMBOLS)
        if symbol.strip()
    )
    await publish_seed_data(
        events=args.events,
        symbols=symbols,
        base_ts=_parse_base_ts(args.base_ts),
        run_id=run_id,
        source_prefix=args.source_prefix,
        connection_string=args.connection_string,
        allow_offline=args.allow_offline,
    )
    return 0


def main() -> int:
    return asyncio.run(run())


if __name__ == "__main__":
    raise SystemExit(main())
