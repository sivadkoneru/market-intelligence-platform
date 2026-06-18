from typing import Any

import pytest

from libs.common import TOPIC_INSIGHTS, TOPIC_SIGNALS, InMemoryBus, ReceivedMessage
from services.alerting.replay import ReplayResult, build_parser, replay_dead_letters
from services.alerting.service import ALERTING_SUBSCRIPTION


class SettlementRecordingBus(InMemoryBus):
    def __init__(self) -> None:
        super().__init__()
        self.completed: list[ReceivedMessage] = []
        self.published: list[tuple[str, dict[str, Any], str | None]] = []
        self.dead_letters = [
            ReceivedMessage(
                topic=TOPIC_INSIGHTS,
                subscription=ALERTING_SUBSCRIPTION,
                body={"event_id": "ins-1"},
                message_id="dlq-1",
                correlation_id="corr-1",
                _queue_ref=object(),
                _dlq=True,
            )
        ]

    async def receive_dead_letter(
        self,
        topic: str,
        subscription: str,
    ) -> list[ReceivedMessage]:
        return list(self.dead_letters)

    async def publish(
        self,
        topic: str,
        body: dict[str, Any],
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        self.published.append((topic, body, message_id))

    async def complete(self, msg: ReceivedMessage) -> None:
        self.completed.append(msg)


@pytest.mark.asyncio
async def test_replay_dead_letters_requeues_messages_with_new_message_ids() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_SIGNALS, ALERTING_SUBSCRIPTION, max_messages=0)

    await bus.publish(TOPIC_SIGNALS, {"event_id": "sig-1"}, message_id="sig-1")
    received = await bus.receive(TOPIC_SIGNALS, ALERTING_SUBSCRIPTION, max_messages=1)
    await bus.dead_letter(received[0], reason="poison")

    result = await replay_dead_letters(bus, topic=TOPIC_SIGNALS)
    replayed = await bus.receive(TOPIC_SIGNALS, ALERTING_SUBSCRIPTION, max_messages=1)

    assert result == ReplayResult(
        topic=TOPIC_SIGNALS,
        subscription=ALERTING_SUBSCRIPTION,
        replayed=1,
    )
    assert len(replayed) == 1
    assert replayed[0].body["event_id"] == "sig-1"
    assert replayed[0].message_id.startswith("sig-1:replay:")


@pytest.mark.asyncio
async def test_replay_dead_letters_handles_empty_dlq() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_INSIGHTS, ALERTING_SUBSCRIPTION, max_messages=0)

    result = await replay_dead_letters(bus, topic=TOPIC_INSIGHTS, suffix="retry")

    assert result.replayed == 0


@pytest.mark.asyncio
async def test_replay_completes_dead_letter_after_republish() -> None:
    bus = SettlementRecordingBus()

    result = await replay_dead_letters(bus, topic=TOPIC_INSIGHTS, suffix="retry")

    assert result.replayed == 1
    assert bus.published == [
        (TOPIC_INSIGHTS, {"event_id": "ins-1"}, "dlq-1:retry:1")
    ]
    assert bus.completed == bus.dead_letters


def test_replay_parser_accepts_supported_topics() -> None:
    parser = build_parser()
    args = parser.parse_args([TOPIC_INSIGHTS, "--subscription", "custom", "--suffix", "retry"])

    assert args.topic == TOPIC_INSIGHTS
    assert args.subscription == "custom"
    assert args.suffix == "retry"
