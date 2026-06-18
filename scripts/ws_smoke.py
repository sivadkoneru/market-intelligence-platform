"""Connect to the API websocket, subscribe to symbols, and log live messages."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence

import websockets

from libs.common import configure_logging, get_logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Smoke-test the API live stream websocket.",
    )
    parser.add_argument(
        "--url",
        default="ws://127.0.0.1:8000/ws/stream",
        help="WebSocket URL for the API stream endpoint.",
    )
    parser.add_argument(
        "--symbol",
        dest="symbols",
        action="append",
        default=None,
        help="Symbol to subscribe to. Repeat for multiple symbols.",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=1,
        help="Maximum number of live messages to log after the subscribe ack.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="Seconds to wait for the subscribe ack and each live message.",
    )
    return parser


def build_subscribe_payload(symbols: Sequence[str]) -> dict[str, object]:
    return {
        "action": "subscribe",
        "symbols": [symbol.strip().upper() for symbol in symbols if symbol.strip()],
    }


async def run_smoke(
    url: str,
    symbols: Sequence[str],
    messages: int,
    timeout_seconds: float = 15.0,
) -> None:
    if messages < 1:
        raise ValueError("messages must be at least 1")

    configure_logging()
    log = get_logger(__name__)
    payload = build_subscribe_payload(symbols)

    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps(payload))
        ack = await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds)
        log.info("ws_smoke.subscribed", payload=ack)

        received = 0
        while received < messages:
            message = await asyncio.wait_for(
                websocket.recv(),
                timeout=timeout_seconds,
            )
            log.info("ws_smoke.message", payload=message)
            received += 1


def main() -> int:
    args = build_parser().parse_args()
    asyncio.run(
        run_smoke(
            args.url,
            args.symbols or ["BTCUSDT"],
            args.messages,
            args.timeout_seconds,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
