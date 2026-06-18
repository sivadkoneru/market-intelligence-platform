"""
Message-bus port: publish / subscribe / dead-letter / duplicate detection.

Public API
----------
MessageBus          — Protocol (interface)
InMemoryBus         — In-memory fake used by unit tests; duplicate detection
                      via message_id; per-subscription dead-letter sub-queue.
ServiceBusBus       — Thin wrapper over ``azure.servicebus`` (real client).
                      Import-guarded so the offline test suite does not fail if
                      the SDK is absent.
get_message_bus()   — Factory: returns InMemoryBus when SERVICE_BUS_CONNECTION_STRING
                      looks like a dev/default placeholder, else ServiceBusBus.

ReceivedMessage     — Simple dataclass representing a received message.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections import defaultdict, deque
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "MessageBus",
    "InMemoryBus",
    "ServiceBusBus",
    "ReceivedMessage",
    "get_message_bus",
]

_FAKE_CONN_STRINGS = {
    "SAS_KEY_VALUE_HERE",  # default placeholder in Settings
    "",
}

_DEV_EMULATOR_MARKER = "UseDevelopmentEmulator=true"


@dataclasses.dataclass
class ReceivedMessage:
    """Thin envelope returned by receive/peek/receive_dead_letter."""

    topic: str
    subscription: str
    body: dict[str, Any]
    message_id: str
    correlation_id: str | None = None
    # Internal — used by InMemoryBus to locate and complete/dead-letter
    _queue_ref: Any = dataclasses.field(default=None, repr=False)
    _dlq: bool = dataclasses.field(default=False, repr=False)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class MessageBus(Protocol):
    async def publish(
        self,
        topic: str,
        body: dict[str, Any],
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        ...

    async def receive(
        self,
        topic: str,
        subscription: str,
        max_messages: int = 10,
    ) -> list[ReceivedMessage]:
        ...

    async def complete(self, msg: ReceivedMessage) -> None:
        ...

    async def dead_letter(self, msg: ReceivedMessage, reason: str = "") -> None:
        ...

    async def peek(
        self,
        topic: str,
        subscription: str,
        n: int = 10,
    ) -> list[ReceivedMessage]:
        ...

    async def receive_dead_letter(
        self,
        topic: str,
        subscription: str,
    ) -> list[ReceivedMessage]:
        ...


# ---------------------------------------------------------------------------
# InMemoryBus
# ---------------------------------------------------------------------------


class InMemoryBus:
    """
    In-memory implementation of MessageBus for unit tests.

    - Duplicate detection: if the same ``message_id`` is published twice on
      the same topic, the second publish is silently dropped (mirrors Azure SB
      native duplicate detection).
    - Dead-letter: ``dead_letter()`` moves a message to a per-subscription DLQ.
    - Peek: returns a snapshot of the queue without consuming items.
    - Subscriptions are created on first use; there is no topology pre-config.
    """

    def __init__(self) -> None:
        # topic → subscription → deque[ReceivedMessage]
        self._queues: dict[str, dict[str, deque[ReceivedMessage]]] = defaultdict(
            lambda: defaultdict(deque)
        )
        # topic → subscription → deque[ReceivedMessage]  (dead-letter)
        self._dlqs: dict[str, dict[str, deque[ReceivedMessage]]] = defaultdict(
            lambda: defaultdict(deque)
        )
        # topic → set[message_id]  (duplicate detection window)
        self._seen_ids: dict[str, set[str]] = defaultdict(set)
        # Messages pending completion: id(msg) → (queue_deque, msg)
        self._in_flight: dict[int, tuple[deque[ReceivedMessage], ReceivedMessage]] = {}

    # --- publisher ---

    async def publish(
        self,
        topic: str,
        body: dict[str, Any],
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        mid = message_id or str(uuid.uuid4())
        if mid in self._seen_ids[topic]:
            return  # duplicate — drop silently
        self._seen_ids[topic].add(mid)

        # Fan out to every subscription that has ever been accessed on this topic
        for sub_name, queue in self._queues[topic].items():
            msg = ReceivedMessage(
                topic=topic,
                subscription=sub_name,
                body=body,
                message_id=mid,
                correlation_id=correlation_id,
                _queue_ref=queue,
            )
            queue.append(msg)

    # --- consumer ---

    async def receive(
        self,
        topic: str,
        subscription: str,
        max_messages: int = 10,
    ) -> list[ReceivedMessage]:
        queue = self._queues[topic][subscription]  # creates if absent
        msgs: list[ReceivedMessage] = []
        while queue and len(msgs) < max_messages:
            msg = queue.popleft()
            msg._queue_ref = queue
            self._in_flight[id(msg)] = (queue, msg)
            msgs.append(msg)
        return msgs

    async def complete(self, msg: ReceivedMessage) -> None:
        self._in_flight.pop(id(msg), None)

    async def dead_letter(self, msg: ReceivedMessage, reason: str = "") -> None:
        self._in_flight.pop(id(msg), None)
        dlq = self._dlqs[msg.topic][msg.subscription]
        msg._dlq = True
        dlq.append(msg)

    async def peek(
        self,
        topic: str,
        subscription: str,
        n: int = 10,
    ) -> list[ReceivedMessage]:
        """Return up to *n* messages without removing them from the queue."""
        queue = self._queues[topic][subscription]
        return list(queue)[:n]

    async def receive_dead_letter(
        self,
        topic: str,
        subscription: str,
    ) -> list[ReceivedMessage]:
        dlq = self._dlqs[topic][subscription]
        msgs = list(dlq)
        dlq.clear()
        return msgs


# ---------------------------------------------------------------------------
# ServiceBusBus (real Azure client — import-guarded)
# ---------------------------------------------------------------------------


def _build_servicebus_bus(connection_string: str) -> "ServiceBusBus":
    try:
        from azure.servicebus.aio import ServiceBusClient  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "azure-servicebus is not installed. "
            "Install it with: pip install azure-servicebus"
        ) from exc
    return ServiceBusBus(connection_string)


@dataclasses.dataclass
class _SBMessageRef:
    """Internal envelope pairing a raw Azure message with its open receiver."""

    raw: Any
    receiver: Any


class ServiceBusBus:
    """
    Thin async wrapper over ``azure.servicebus`` topics + subscriptions.

    Requires ``azure-servicebus`` to be importable. Construct from a
    valid Azure Service Bus connection string.

    Receiver lifecycle
    ------------------
    A ``ServiceBusReceiver`` must remain open between ``receive()`` and the
    subsequent ``complete()`` / ``dead_letter()`` call.  We therefore keep one
    persistent receiver per ``(topic, subscription)`` pair in
    ``self._receivers``, created lazily on first use and never closed inside
    ``receive()``.  Call ``await bus.close()`` to shut everything down.

    Integration tests only — skip without live infra.
    """

    def __init__(self, connection_string: str) -> None:
        from azure.servicebus.aio import ServiceBusClient

        self._connection_string = connection_string
        self._client = ServiceBusClient.from_connection_string(connection_string)
        # topic → sender  (each sender is recreated per publish — senders are
        # lightweight and the async-with pattern closes them on exit)
        self._senders: dict[str, Any] = {}
        # (topic, subscription, sub_queue) → open ServiceBusReceiver
        self._receivers: dict[tuple[str, str, str | None], Any] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_receiver(
        self,
        topic: str,
        subscription: str,
        sub_queue: str | None = None,
    ) -> Any:
        """Return a cached open receiver, creating it lazily."""
        key = (topic, subscription, sub_queue)
        if key not in self._receivers:
            kwargs = {"topic_name": topic, "subscription_name": subscription}
            if sub_queue is not None:
                kwargs["sub_queue"] = sub_queue
            self._receivers[key] = self._client.get_subscription_receiver(**kwargs)
        return self._receivers[key]

    # ------------------------------------------------------------------
    # Publisher
    # ------------------------------------------------------------------

    async def publish(
        self,
        topic: str,
        body: dict[str, Any],
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        import json

        from azure.servicebus import ServiceBusMessage

        sender = self._client.get_topic_sender(topic_name=topic)
        msg = ServiceBusMessage(
            body=json.dumps(body).encode(),
            message_id=message_id or str(uuid.uuid4()),
            correlation_id=correlation_id,
        )
        async with sender:
            await sender.send_messages(msg)

    # ------------------------------------------------------------------
    # Consumer
    # ------------------------------------------------------------------

    async def receive(
        self,
        topic: str,
        subscription: str,
        max_messages: int = 10,
    ) -> list[ReceivedMessage]:
        import json

        receiver = self._get_receiver(topic, subscription)
        received = await receiver.receive_messages(
            max_message_count=max_messages, max_wait_time=5
        )
        msgs: list[ReceivedMessage] = []
        for raw in received:
            body = json.loads(bytes(raw.body))
            msgs.append(
                ReceivedMessage(
                    topic=topic,
                    subscription=subscription,
                    body=body,
                    message_id=str(raw.message_id or ""),
                    correlation_id=str(raw.correlation_id or ""),
                    # _queue_ref carries BOTH the raw message and the open receiver
                    _queue_ref=_SBMessageRef(raw=raw, receiver=receiver),
                )
            )
        return msgs

    async def complete(self, msg: ReceivedMessage) -> None:
        if msg._queue_ref is not None:
            ref: _SBMessageRef = msg._queue_ref
            await ref.receiver.complete_message(ref.raw)

    async def dead_letter(self, msg: ReceivedMessage, reason: str = "") -> None:
        if msg._queue_ref is not None:
            ref: _SBMessageRef = msg._queue_ref
            await ref.receiver.dead_letter_message(ref.raw, reason=reason)

    async def peek(
        self,
        topic: str,
        subscription: str,
        n: int = 10,
    ) -> list[ReceivedMessage]:
        import json

        receiver = self._client.get_subscription_receiver(
            topic_name=topic,
            subscription_name=subscription,
        )
        msgs: list[ReceivedMessage] = []
        async with receiver:
            peeked = await receiver.peek_messages(max_message_count=n)
            for raw in peeked:
                body = json.loads(bytes(raw.body))
                msgs.append(
                    ReceivedMessage(
                        topic=topic,
                        subscription=subscription,
                        body=body,
                        message_id=str(raw.message_id or ""),
                        correlation_id=str(raw.correlation_id or ""),
                    )
                )
        return msgs

    async def receive_dead_letter(
        self,
        topic: str,
        subscription: str,
    ) -> list[ReceivedMessage]:
        import json

        receiver = self._get_receiver(topic, subscription, sub_queue="deadletter")
        msgs: list[ReceivedMessage] = []
        received = await receiver.receive_messages(max_message_count=100, max_wait_time=5)
        for raw in received:
            body = json.loads(bytes(raw.body))
            msgs.append(
                ReceivedMessage(
                    topic=topic,
                    subscription=subscription,
                    body=body,
                    message_id=str(raw.message_id or ""),
                    correlation_id=str(raw.correlation_id or ""),
                    _queue_ref=_SBMessageRef(raw=raw, receiver=receiver),
                    _dlq=True,
                )
            )
        return msgs

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close all cached receivers then the underlying client."""
        for receiver in self._receivers.values():
            await receiver.close()
        self._receivers.clear()
        await self._client.close()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_message_bus(settings: Any = None) -> MessageBus:
    """
    Return InMemoryBus when the connection string is the default placeholder
    or when SERVICE_BUS_CONNECTION_STRING is unset/empty.
    Otherwise return a ServiceBusBus wrapping the real Azure SDK.
    """
    if settings is None:
        from libs.common.config import get_settings

        settings = get_settings()

    conn_str: str = settings.service_bus_connection_string or ""
    is_placeholder = any(marker in conn_str for marker in _FAKE_CONN_STRINGS)
    # If it has "UseDevelopmentEmulator=true" but also "SAS_KEY_VALUE_HERE" it's
    # still the default unset placeholder — keep the is_placeholder logic above.

    if is_placeholder:
        return InMemoryBus()
    return _build_servicebus_bus(conn_str)
