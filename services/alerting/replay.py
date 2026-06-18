"""Dead-letter replay helper for the alerting subscription."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from libs.common import (
    TOPIC_INSIGHTS,
    TOPIC_SIGNALS,
    MessageBus,
    configure_logging,
    get_logger,
    get_message_bus,
)
from services.alerting.service import ALERTING_SUBSCRIPTION


@dataclass(frozen=True)
class ReplayResult:
    topic: str
    subscription: str
    replayed: int


async def replay_dead_letters(
    bus: MessageBus,
    *,
    topic: str,
    subscription: str = ALERTING_SUBSCRIPTION,
    suffix: str = "replay",
) -> ReplayResult:
    messages = await bus.receive_dead_letter(topic, subscription)
    for index, message in enumerate(messages, start=1):
        await bus.publish(
            topic,
            message.body,
            message_id=f"{message.message_id}:{suffix}:{index}",
            correlation_id=message.correlation_id,
        )
        await bus.complete(message)
    return ReplayResult(topic=topic, subscription=subscription, replayed=len(messages))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replay dead-lettered messages for the alerting subscription."
    )
    parser.add_argument(
        "topic",
        choices=[TOPIC_SIGNALS, TOPIC_INSIGHTS],
        help="Topic whose alerting dead-letter queue should be replayed.",
    )
    parser.add_argument(
        "--subscription",
        default=ALERTING_SUBSCRIPTION,
        help="Subscription name to replay from.",
    )
    parser.add_argument(
        "--suffix",
        default="replay",
        help="Suffix appended to replayed message ids.",
    )
    return parser


async def _main_async() -> int:
    configure_logging()
    log = get_logger(__name__)
    args = build_parser().parse_args()
    result = await replay_dead_letters(
        get_message_bus(),
        topic=args.topic,
        subscription=args.subscription,
        suffix=args.suffix,
    )
    log.info(
        "alerting.dead_letter_replayed",
        topic=result.topic,
        subscription=result.subscription,
        replayed=result.replayed,
    )
    return 0


def main() -> int:
    import asyncio

    return asyncio.run(_main_async())


if __name__ == "__main__":
    raise SystemExit(main())
