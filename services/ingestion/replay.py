"""
Deterministic replay feed for offline ingestion tests and CI.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any


class ReplayDisconnectError(ConnectionError):
    """Raised by the replay feed to simulate a transient connection drop."""


def build_default_replay_events() -> list[dict[str, Any]]:
    base_ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    return [
        {
            "kind": "replay",
            "source": "replay.binance",
            "symbol": "BTCUSDT",
            "event_type": "trade",
            "price": 42000.25,
            "volume": 0.15,
            "ts": base_ts.isoformat(),
        },
        {
            "kind": "replay",
            "source": "replay.coinbase",
            "symbol": "ETHUSD",
            "event_type": "ticker",
            "price": 2250.5,
            "bid": 2250.1,
            "ask": 2250.9,
            "ts": (base_ts + timedelta(seconds=1)).isoformat(),
        },
        {
            "kind": "replay",
            "source": "replay.binance",
            "symbol": "BTCUSDT",
            "event_type": "trade",
            "price": 42000.25,
            "volume": 0.15,
            "ts": base_ts.isoformat(),
        },
    ]


@dataclass
class DeterministicReplayFeed:
    """
    Async iterable of fixed payloads with optional one-shot disconnect support.

    The feed can be re-instantiated by a factory on reconnect. When
    ``disconnect_at`` is set, the iterator raises ``ReplayDisconnectError`` once
    at that 0-based payload index if ``disconnect_once`` is true.
    """

    events: Iterable[dict[str, Any]] = field(default_factory=build_default_replay_events)
    disconnect_at: int | None = None
    disconnect_once: bool = True
    _disconnect_emitted: bool = False

    def __post_init__(self) -> None:
        self.events = tuple(dict(event) for event in self.events)

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[dict[str, Any]]:
        for index, event in enumerate(self.events):
            if self.disconnect_at == index and (
                not self.disconnect_once or not self._disconnect_emitted
            ):
                self._disconnect_emitted = True
                raise ReplayDisconnectError("deterministic replay disconnect")
            yield dict(event)


def build_replay_feed_factory(
    events: Iterable[dict[str, Any]] | None = None,
    *,
    disconnect_at: int | None = None,
    disconnect_once: bool = True,
) -> Callable[[], DeterministicReplayFeed]:
    """
    Return a feed factory suitable for reconnect loops.

    ``DeterministicReplayFeed`` tracks disconnects per instance. This factory
    tracks a one-shot disconnect across newly-created feed instances, matching
    the way ``IngestionService`` reconnects by asking for a fresh feed.
    """
    source_events = build_default_replay_events() if events is None else events
    replay_events = tuple(dict(event) for event in source_events)
    remaining_disconnects = 1 if disconnect_at is not None and disconnect_once else None

    def factory() -> DeterministicReplayFeed:
        nonlocal remaining_disconnects
        effective_disconnect_at = disconnect_at
        if remaining_disconnects is not None:
            if remaining_disconnects <= 0:
                effective_disconnect_at = None
            else:
                remaining_disconnects -= 1

        return DeterministicReplayFeed(
            replay_events,
            disconnect_at=effective_disconnect_at,
            disconnect_once=False,
        )

    return factory
