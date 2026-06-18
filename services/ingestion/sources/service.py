"""
Service runner for polling news and social collectors once.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from libs.common import TOPIC_NEWS_RAW, MessageBus, get_logger
from services.ingestion.sources.base import NewsCollector, hash_news_event


@dataclass
class NewsPollingMetrics:
    collectors_polled: int = 0
    events_seen: int = 0
    publish_attempts: int = 0
    unique_publishes: int = 0
    duplicate_events: int = 0
    last_error: str | None = None
    published_message_ids: set[str] = field(default_factory=set)

    def render(self) -> str:
        lines = [
            "# TYPE news_polling_collectors_polled counter",
            f"news_polling_collectors_polled {self.collectors_polled}",
            "# TYPE news_polling_events_seen counter",
            f"news_polling_events_seen {self.events_seen}",
            "# TYPE news_polling_publish_attempts counter",
            f"news_polling_publish_attempts {self.publish_attempts}",
            "# TYPE news_polling_unique_publishes counter",
            f"news_polling_unique_publishes {self.unique_publishes}",
            "# TYPE news_polling_duplicate_events counter",
            f"news_polling_duplicate_events {self.duplicate_events}",
        ]
        return "\n".join(lines) + "\n"


class NewsPollingService:
    """
    Poll a set of collectors once and publish normalized news events to ``news.raw``.
    """

    def __init__(
        self,
        *,
        bus: MessageBus,
        collectors: Sequence[NewsCollector],
        topic: str = TOPIC_NEWS_RAW,
    ) -> None:
        self._bus = bus
        self._collectors = list(collectors)
        self._topic = topic
        self.metrics = NewsPollingMetrics()
        self._log = get_logger(__name__)

    async def run_once(self) -> NewsPollingMetrics:
        for collector in self._collectors:
            self.metrics.collectors_polled += 1
            try:
                events = await collector.poll_once()
            except Exception as exc:  # pragma: no cover - defensive logging path
                self.metrics.last_error = str(exc)
                self._log.warning(
                    "ingestion.news_collector_error",
                    collector=getattr(collector, "name", collector.__class__.__name__),
                    error=str(exc),
                )
                raise

            for event in events:
                self.metrics.events_seen += 1
                message_id = hash_news_event(event)
                self.metrics.publish_attempts += 1
                if message_id in self.metrics.published_message_ids:
                    self.metrics.duplicate_events += 1
                else:
                    self.metrics.published_message_ids.add(message_id)
                    self.metrics.unique_publishes += 1

                await self._bus.publish(
                    self._topic,
                    event.model_dump(mode="json"),
                    message_id=message_id,
                    correlation_id=event.correlation_id,
                )
                self._log.info(
                    "ingestion.news_published",
                    topic=self._topic,
                    source=event.source,
                    title=event.title,
                    message_id=message_id,
                )

        return self.metrics

    async def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "ingestion.news",
            "topic": self._topic,
            "collectors": len(self._collectors),
            "events_seen": self.metrics.events_seen,
        }

