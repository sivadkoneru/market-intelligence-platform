"""Tests for libs.common.bus — InMemoryBus, ServiceBusBus settlement, and factory."""

import pytest

from libs.common.bus import (
    InMemoryBus,
    ReceivedMessage,
    ServiceBusBus,
    _decode_servicebus_body,
    _SBMessageRef,
    get_message_bus,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _publish_and_receive(
    bus: InMemoryBus, topic: str, sub: str, body: dict
) -> list[ReceivedMessage]:
    await bus.publish(topic, body)
    return await bus.receive(topic, sub)


# ---------------------------------------------------------------------------
# Basic publish / receive
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_receive_basic():
    bus = InMemoryBus()
    # Prime the subscription queue by accessing it first
    await bus.receive("topic.a", "sub1", max_messages=0)
    await bus.publish("topic.a", {"x": 1})
    msgs = await bus.receive("topic.a", "sub1")
    assert len(msgs) == 1
    assert msgs[0].body == {"x": 1}


@pytest.mark.asyncio
async def test_receive_respects_max_messages():
    bus = InMemoryBus()
    await bus.receive("t", "s", max_messages=0)  # prime
    for i in range(5):
        await bus.publish("t", {"i": i}, message_id=f"id-{i}")
    msgs = await bus.receive("t", "s", max_messages=3)
    assert len(msgs) == 3


@pytest.mark.asyncio
async def test_fanout_to_multiple_subscriptions():
    bus = InMemoryBus()
    # Prime both subs so publish fans out to both
    await bus.receive("t", "sub-a", max_messages=0)
    await bus.receive("t", "sub-b", max_messages=0)
    await bus.publish("t", {"val": 42})
    a = await bus.receive("t", "sub-a")
    b = await bus.receive("t", "sub-b")
    assert a[0].body == {"val": 42}
    assert b[0].body == {"val": 42}


# ---------------------------------------------------------------------------
# Duplicate detection (idempotency — AC-5 / AC-8)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_message_id_is_dropped():
    bus = InMemoryBus()
    await bus.receive("dup-topic", "sub1", max_messages=0)  # prime
    await bus.publish("dup-topic", {"v": 1}, message_id="msg-001")
    await bus.publish("dup-topic", {"v": 2}, message_id="msg-001")  # duplicate
    msgs = await bus.receive("dup-topic", "sub1")
    assert len(msgs) == 1
    assert msgs[0].body == {"v": 1}


@pytest.mark.asyncio
async def test_different_message_ids_both_delivered():
    bus = InMemoryBus()
    await bus.receive("dup-topic", "sub1", max_messages=0)
    await bus.publish("dup-topic", {"v": 1}, message_id="msg-001")
    await bus.publish("dup-topic", {"v": 2}, message_id="msg-002")
    msgs = await bus.receive("dup-topic", "sub1")
    assert len(msgs) == 2


# ---------------------------------------------------------------------------
# Complete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_complete_removes_from_in_flight():
    bus = InMemoryBus()
    await bus.receive("t", "s", max_messages=0)
    await bus.publish("t", {"n": 1})
    msgs = await bus.receive("t", "s")
    assert len(msgs) == 1
    await bus.complete(msgs[0])
    # in_flight should now be empty
    assert len(bus._in_flight) == 0


# ---------------------------------------------------------------------------
# Dead-letter (AC-5 / AC-8)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dead_letter_routes_to_dlq():
    bus = InMemoryBus()
    await bus.receive("t", "s", max_messages=0)
    await bus.publish("t", {"poison": True})
    msgs = await bus.receive("t", "s")
    assert len(msgs) == 1
    await bus.dead_letter(msgs[0], reason="bad message")

    dlq_msgs = await bus.receive_dead_letter("t", "s")
    assert len(dlq_msgs) == 1
    assert dlq_msgs[0].body == {"poison": True}
    assert dlq_msgs[0]._dlq is True


@pytest.mark.asyncio
async def test_dead_lettered_message_not_in_main_queue():
    bus = InMemoryBus()
    await bus.receive("t", "s", max_messages=0)
    await bus.publish("t", {"msg": "x"})
    msgs = await bus.receive("t", "s")
    await bus.dead_letter(msgs[0])
    # Main queue is now empty
    remaining = await bus.receive("t", "s")
    assert remaining == []


@pytest.mark.asyncio
async def test_receive_dead_letter_clears_dlq():
    bus = InMemoryBus()
    await bus.receive("t", "s", max_messages=0)
    await bus.publish("t", {"x": 1})
    msgs = await bus.receive("t", "s")
    await bus.dead_letter(msgs[0])
    # First call returns the message
    first = await bus.receive_dead_letter("t", "s")
    assert len(first) == 1
    # Second call returns nothing
    second = await bus.receive_dead_letter("t", "s")
    assert second == []


# ---------------------------------------------------------------------------
# Peek (non-consuming)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_peek_does_not_consume():
    bus = InMemoryBus()
    await bus.receive("t", "s", max_messages=0)
    await bus.publish("t", {"peek": True})
    peeked = await bus.peek("t", "s")
    assert len(peeked) == 1
    # Still available via receive
    msgs = await bus.receive("t", "s")
    assert len(msgs) == 1


@pytest.mark.asyncio
async def test_peek_respects_n():
    bus = InMemoryBus()
    await bus.receive("t", "s", max_messages=0)
    for i in range(5):
        await bus.publish("t", {"i": i}, message_id=f"peek-{i}")
    peeked = await bus.peek("t", "s", n=2)
    assert len(peeked) == 2
    # All 5 still in queue
    msgs = await bus.receive("t", "s")
    assert len(msgs) == 5


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def test_get_message_bus_returns_in_memory_by_default():
    bus = get_message_bus()
    assert isinstance(bus, InMemoryBus)


# ---------------------------------------------------------------------------
# ServiceBusBus — settlement lifecycle (no live infra)
# ---------------------------------------------------------------------------


class _FakeReceiver:
    """Fake ServiceBusReceiver that records settlement calls without Azure."""

    def __init__(self, received: list | None = None) -> None:
        self.completed: list = []
        self.dead_lettered: list = []
        self.received = received or []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def complete_message(self, raw_msg) -> None:
        self.completed.append(raw_msg)

    async def dead_letter_message(self, raw_msg, *, reason: str = "") -> None:
        self.dead_lettered.append((raw_msg, reason))

    async def receive_messages(self, **kwargs):
        return list(self.received)

    async def peek_messages(self, **kwargs):
        return list(self.received)[: kwargs.get("max_message_count", len(self.received))]


class _FakeRawMessage:
    """Minimal stand-in for ServiceBusReceivedMessage."""

    def __init__(
        self,
        mid: str = "msg-1",
        body: object = b'{"v": 1}',
        correlation_id: str | None = "corr-1",
    ) -> None:
        self.message_id = mid
        self.body = body
        self.correlation_id = correlation_id


class _FakeServiceBusClient:
    def __init__(self, receiver: _FakeReceiver) -> None:
        self.receiver = receiver
        self.receiver_calls: list[dict] = []

    def get_subscription_receiver(self, **kwargs):
        self.receiver_calls.append(dict(kwargs))
        return self.receiver


def _make_servicebus_bus_with_fake_receiver(
    topic: str, subscription: str
) -> tuple[ServiceBusBus, _FakeReceiver]:
    """
    Build a ServiceBusBus with its Azure client mocked out (no real connection)
    and inject a _FakeReceiver into the receiver cache for the given
    (topic, subscription) pair so that complete()/dead_letter() use it.
    """
    # Avoid triggering the real ServiceBusClient constructor.
    bus = object.__new__(ServiceBusBus)
    bus._senders = {}
    bus._receivers = {}
    # Inject the fake receiver
    fake_receiver = _FakeReceiver()
    bus._receivers[(topic, subscription, None)] = fake_receiver
    return bus, fake_receiver


def test_decode_servicebus_body_accepts_bytes_strings_and_sections():
    assert _decode_servicebus_body(b'{"v": 1}') == {"v": 1}
    assert _decode_servicebus_body('{"v": 2}') == {"v": 2}
    assert _decode_servicebus_body([b'{"v"', memoryview(b": 3}")]) == {"v": 3}


@pytest.mark.asyncio
async def test_servicebus_complete_calls_receiver_complete_message():
    """complete() must call complete_message on the open receiver with the raw msg."""
    bus, fake_rx = _make_servicebus_bus_with_fake_receiver("t", "s")
    raw = _FakeRawMessage("id-complete")
    ref = _SBMessageRef(raw=raw, receiver=fake_rx)
    msg = ReceivedMessage(
        topic="t",
        subscription="s",
        body={"v": 1},
        message_id="id-complete",
        _queue_ref=ref,
    )

    await bus.complete(msg)

    assert fake_rx.completed == [raw], "complete_message was not awaited with the raw message"


@pytest.mark.asyncio
async def test_servicebus_dead_letter_calls_receiver_dead_letter_message():
    """dead_letter() must call dead_letter_message on the open receiver with reason."""
    bus, fake_rx = _make_servicebus_bus_with_fake_receiver("t", "s")
    raw = _FakeRawMessage("id-dlq")
    ref = _SBMessageRef(raw=raw, receiver=fake_rx)
    msg = ReceivedMessage(
        topic="t",
        subscription="s",
        body={"bad": True},
        message_id="id-dlq",
        _queue_ref=ref,
    )

    await bus.dead_letter(msg, reason="poison pill")

    assert len(fake_rx.dead_lettered) == 1
    settled_raw, settled_reason = fake_rx.dead_lettered[0]
    assert settled_raw is raw, "dead_letter_message was not called with the raw message"
    assert settled_reason == "poison pill"


@pytest.mark.asyncio
async def test_servicebus_receive_decodes_sectioned_body():
    raw = _FakeRawMessage("id-sectioned", body=[b'{"v"', b": 1}"])
    fake_rx = _FakeReceiver(received=[raw])
    fake_client = _FakeServiceBusClient(fake_rx)
    bus = object.__new__(ServiceBusBus)
    bus._senders = {}
    bus._receivers = {}
    bus._client = fake_client

    messages = await bus.receive("topic", "sub")

    assert len(messages) == 1
    assert messages[0].body == {"v": 1}
    assert messages[0]._queue_ref.raw is raw
    assert fake_client.receiver_calls == [
        {"topic_name": "topic", "subscription_name": "sub"}
    ]


@pytest.mark.asyncio
async def test_servicebus_peek_decodes_sectioned_body():
    raw = _FakeRawMessage("id-peek", body=[b'{"peek"', b": true}"])
    fake_rx = _FakeReceiver(received=[raw])
    fake_client = _FakeServiceBusClient(fake_rx)
    bus = object.__new__(ServiceBusBus)
    bus._client = fake_client

    messages = await bus.peek("topic", "sub")

    assert len(messages) == 1
    assert messages[0].body == {"peek": True}
    assert fake_client.receiver_calls == [
        {"topic_name": "topic", "subscription_name": "sub"}
    ]


@pytest.mark.asyncio
async def test_servicebus_receive_dead_letter_keeps_receiver_for_settlement():
    raw = _FakeRawMessage("id-dlq", body=[b'{"bad"', b": true}"])
    fake_rx = _FakeReceiver(received=[raw])
    fake_client = _FakeServiceBusClient(fake_rx)
    bus = object.__new__(ServiceBusBus)
    bus._senders = {}
    bus._receivers = {}
    bus._client = fake_client

    messages = await bus.receive_dead_letter("topic", "sub")

    assert len(messages) == 1
    assert messages[0].body == {"bad": True}
    assert messages[0]._dlq is True
    assert fake_client.receiver_calls == [
        {
            "topic_name": "topic",
            "subscription_name": "sub",
            "sub_queue": "deadletter",
        }
    ]

    await bus.complete(messages[0])

    assert fake_rx.completed == [raw]


@pytest.mark.asyncio
async def test_servicebus_complete_noop_when_no_queue_ref():
    """complete() must not raise when _queue_ref is None."""
    bus, _ = _make_servicebus_bus_with_fake_receiver("t", "s")
    msg = ReceivedMessage(topic="t", subscription="s", body={}, message_id="x", _queue_ref=None)
    await bus.complete(msg)  # should not raise


@pytest.mark.asyncio
async def test_servicebus_dead_letter_noop_when_no_queue_ref():
    """dead_letter() must not raise when _queue_ref is None."""
    bus, _ = _make_servicebus_bus_with_fake_receiver("t", "s")
    msg = ReceivedMessage(topic="t", subscription="s", body={}, message_id="x", _queue_ref=None)
    await bus.dead_letter(msg, reason="irrelevant")  # should not raise
