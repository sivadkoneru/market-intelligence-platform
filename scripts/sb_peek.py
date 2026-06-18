"""Peek topic messages from the Service Bus emulator using the shared bus abstraction."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from libs.common import InMemoryBus, configure_logging, get_logger, get_message_bus, get_settings

DEFAULT_SUBSCRIPTION_PREFERENCE = ("api", "api-ws", "stream", "alerting", "ai")
SB_CONFIG_PATH = Path(__file__).resolve().parent.parent / "infra" / "servicebus-config.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Peek Service Bus topic messages via the shared bus abstraction.",
    )
    parser.add_argument("topic", help="Topic name to inspect, for example market.raw.")
    parser.add_argument(
        "--subscription",
        default=None,
        help="Subscription name to inspect. Defaults to a stable topic-specific choice.",
    )
    parser.add_argument(
        "-n",
        "--messages",
        type=int,
        default=3,
        help="Maximum number of queued messages to peek.",
    )
    parser.add_argument(
        "--connection-string",
        default=None,
        help="Override SERVICE_BUS_CONNECTION_STRING for this invocation.",
    )
    parser.add_argument(
        "--config-path",
        default=str(SB_CONFIG_PATH),
        help="Path to the Service Bus emulator topology config JSON.",
    )
    parser.add_argument(
        "--allow-offline",
        action="store_true",
        help="Allow the in-memory placeholder bus instead of requiring the Service Bus emulator.",
    )
    return parser


def load_topic_subscriptions(config_path: str | Path) -> dict[str, list[str]]:
    with Path(config_path).open(encoding="utf-8") as handle:
        config = json.load(handle)

    namespaces = config.get("UserConfig", {}).get("Namespaces", [])
    if not namespaces:
        return {}

    subscriptions: dict[str, list[str]] = {}
    for topic in namespaces[0].get("Topics", []):
        subscriptions[topic["Name"]] = [
            subscription["Name"] for subscription in topic.get("Subscriptions", [])
        ]
    return subscriptions


def resolve_subscription(
    topic: str,
    *,
    requested_subscription: str | None,
    config_path: str | Path,
) -> str:
    if requested_subscription:
        return requested_subscription

    topic_subscriptions = load_topic_subscriptions(config_path).get(topic, [])
    if not topic_subscriptions:
        raise ValueError(f"Topic '{topic}' has no subscriptions in {config_path}")

    for candidate in DEFAULT_SUBSCRIPTION_PREFERENCE:
        if candidate in topic_subscriptions:
            return candidate
    return topic_subscriptions[0]


async def peek_messages(
    topic: str,
    *,
    subscription: str,
    messages: int,
    connection_string: str | None = None,
    allow_offline: bool = False,
) -> list[Any]:
    if messages < 1:
        raise ValueError("messages must be at least 1")

    settings = get_settings()
    resolved_connection_string = connection_string or settings.service_bus_connection_string
    bus = get_message_bus(SimpleNamespace(service_bus_connection_string=resolved_connection_string))
    log = get_logger(__name__)

    try:
        if isinstance(bus, InMemoryBus):
            if not allow_offline:
                raise RuntimeError(
                    "SERVICE_BUS_CONNECTION_STRING resolves to the in-memory placeholder; "
                    "start compose or pass --connection-string for the Service Bus emulator."
                )
            log.info(
                "sb_peek.offline_bus",
                topic=topic,
                subscription=subscription,
                message=(
                    "Using the in-memory bus placeholder; no shared emulator queue is available "
                    "without a real SERVICE_BUS_CONNECTION_STRING."
                ),
            )
            return []

        peeked = await bus.peek(topic, subscription, n=messages)
        for message in peeked:
            log.info(
                "sb_peek.message",
                topic=topic,
                subscription=subscription,
                message_id=message.message_id,
                correlation_id=message.correlation_id,
                payload=message.body,
            )

        if not peeked:
            log.info(
                "sb_peek.empty",
                topic=topic,
                subscription=subscription,
                requested=messages,
            )
        return peeked
    finally:
        close = getattr(bus, "close", None)
        if close is not None:
            await close()


async def run() -> int:
    configure_logging()
    log = get_logger(__name__)
    args = build_parser().parse_args()
    subscription = resolve_subscription(
        args.topic,
        requested_subscription=args.subscription,
        config_path=args.config_path,
    )
    log.info(
        "sb_peek.started",
        topic=args.topic,
        subscription=subscription,
        requested=args.messages,
    )
    await peek_messages(
        args.topic,
        subscription=subscription,
        messages=args.messages,
        connection_string=args.connection_string,
        allow_offline=args.allow_offline,
    )
    return 0


def main() -> int:
    return asyncio.run(run())


if __name__ == "__main__":
    raise SystemExit(main())
