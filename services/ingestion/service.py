"""
Core ingestion service loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from libs.common import (
    TOPIC_MARKET_RAW,
    TOPIC_NEWS_RAW,
    MessageBus,
    NewsEvent,
    WorkerMetrics,
    close_backends,
    get_logger,
    market_event_key,
    render_counters,
)
from services.ingestion.normalizer import normalize_market_payload
from services.ingestion.replay import DeterministicReplayFeed, ReplayDisconnectError

ReplayFeedFactory = Callable[[], DeterministicReplayFeed]


@dataclass
class IngestionMetrics(WorkerMetrics):
    events_seen: int = 0
    events_normalized: int = 0
    publish_attempts: int = 0
    unique_publishes: int = 0
    news_publishes: int = 0
    duplicate_events: int = 0
    reconnects: int = 0
    disconnects: int = 0
    published_message_ids: set[str] = field(default_factory=set)

    def render(self) -> str:
        lines = render_counters(
            "ingestion",
            {
                "events_seen": self.events_seen,
                "events_normalized": self.events_normalized,
                "publish_attempts": self.publish_attempts,
                "unique_publishes": self.unique_publishes,
                "news_publishes": self.news_publishes,
                "duplicate_events": self.duplicate_events,
                "reconnects": self.reconnects,
                "disconnects": self.disconnects,
            },
        )
        lines.extend(self.http.render("ingestion"))
        return "\n".join(lines) + "\n"


class IngestionService:
    def __init__(
        self,
        *,
        bus: MessageBus,
        feed_factory: ReplayFeedFactory,
        topic: str = TOPIC_MARKET_RAW,
        news_topic: str = TOPIC_NEWS_RAW,
        reconnect_backoff_seconds: float = 0.01,
    ) -> None:
        self._bus = bus
        self._feed_factory = feed_factory
        self._topic = topic
        self._news_topic = news_topic
        self._reconnect_backoff_seconds = reconnect_backoff_seconds
        self.metrics = IngestionMetrics()
        self._log = get_logger(__name__)

    async def run(
        self,
        *,
        max_events: int | None = None,
        max_reconnects: int = 3,
    ) -> IngestionMetrics:
        processed = 0
        reconnects_used = 0

        while max_events is None or processed < max_events:
            try:
                async for payload in self._feed_factory():
                    self.metrics.events_seen += 1
                    market_event = normalize_market_payload(payload)
                    self.metrics.events_normalized += 1

                    message_id = market_event_key(
                        market_event.symbol, market_event.ts, market_event.source
                    )
                    self.metrics.publish_attempts += 1
                    if message_id in self.metrics.published_message_ids:
                        self.metrics.duplicate_events += 1
                    else:
                        self.metrics.published_message_ids.add(message_id)
                        self.metrics.unique_publishes += 1

                    await self._bus.publish(
                        self._topic,
                        market_event.model_dump(mode="json"),
                        message_id=message_id,
                        correlation_id=market_event.correlation_id,
                    )
                    self._log.info(
                        "ingestion.published",
                        topic=self._topic,
                        symbol=market_event.symbol,
                        source=market_event.source,
                        message_id=message_id,
                    )

                    processed += 1
                    if max_events is not None and processed >= max_events:
                        return self.metrics
                return self.metrics
            except ReplayDisconnectError as exc:
                self.metrics.disconnects += 1
                self.metrics.last_error = str(exc)
                self._log.warning(
                    "ingestion.feed_disconnect",
                    reconnects_used=reconnects_used,
                    error=str(exc),
                )
                if reconnects_used >= max_reconnects:
                    raise
                reconnects_used += 1
                self.metrics.reconnects += 1
                await asyncio.sleep(self._reconnect_backoff_seconds)

        return self.metrics

    async def close(self) -> None:
        """Release every backend that owns a connection."""
        await close_backends((self._bus,), log=self._log, service_name="ingestion")

    async def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "ingestion",
            "topic": self._topic,
            "news_topic": self._news_topic,
            "events_seen": self.metrics.events_seen,
            "reconnects": self.metrics.reconnects,
        }

    async def publish_news_event(self, event: NewsEvent) -> str:
        message_id = event.event_id
        self.metrics.publish_attempts += 1
        self.metrics.news_publishes += 1
        await self._bus.publish(
            self._news_topic,
            event.model_dump(mode="json"),
            message_id=message_id,
            correlation_id=event.correlation_id,
        )
        self._log.info(
            "ingestion.news_published",
            topic=self._news_topic,
            symbols=event.symbols,
            source=event.source,
            message_id=message_id,
        )
        return message_id
