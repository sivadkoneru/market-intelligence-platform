"""
FastAPI app for the ingestion service.
"""

from __future__ import annotations

import functools
from datetime import UTC, datetime

from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel, Field

from libs.common import TOPIC_NEWS_RAW, NewsEvent, get_message_bus
from libs.common.service_app import (
    bootstrap_service_logging,
    create_service_app,
    worker_lifespan,
)
from services.ingestion.replay import DeterministicReplayFeed, build_default_replay_events
from services.ingestion.service import IngestionService


class MockNewsRequest(BaseModel):
    """Payload for publishing a local NewsEvent into news.raw."""

    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT"])
    source: str = "mock.local"
    title: str | None = None
    body: str | None = None
    url: str | None = "https://example.test/local-market-news"
    author: str | None = "local-mock"
    event_id: str | None = None
    ts: datetime | None = None
    correlation_id: str | None = None
    trace_id: str | None = None


def build_default_service() -> IngestionService:
    """Build the offline-safe default ingestion service used by Uvicorn."""
    return IngestionService(
        bus=get_message_bus(),
        feed_factory=lambda: DeterministicReplayFeed(build_default_replay_events()),
    )


def create_app(
    service: IngestionService | None = None,
    *,
    run_on_startup: bool = True,
    startup_max_events: int | None = None,
) -> FastAPI:
    bootstrap_service_logging("ingestion")
    resolved_service = service or build_default_service()

    app = create_service_app(
        service_name="ingestion",
        title="Market Intelligence Ingestion Service",
        summary="Portfolio service for offline-safe market ingestion.",
        service=resolved_service,
        state_attr="ingestion_service",
        render_metrics=resolved_service.metrics.render,
        lifespan=worker_lifespan(
            (
                functools.partial(resolved_service.run, max_events=startup_max_events)
                if run_on_startup
                else None
            ),
            task_name="ingestion-worker",
            state_attr="ingestion_task",
            close=resolved_service.close,
        ),
    )

    @app.post("/mock/news", status_code=202)
    async def publish_mock_news(
        request: MockNewsRequest | None = Body(default=None),
    ) -> dict[str, object]:
        payload = request or MockNewsRequest()
        symbols = _normalise_symbols(payload.symbols)
        if not symbols:
            raise HTTPException(status_code=400, detail="At least one symbol is required")

        primary_symbol = symbols[0]
        event_id = (
            payload.event_id or f"mock-news-{datetime.now(tz=UTC).strftime('%Y%m%d%H%M%S%f')}"
        )
        event = NewsEvent(
            event_id=event_id,
            ts=payload.ts or datetime.now(tz=UTC),
            source=payload.source,
            title=payload.title or f"{primary_symbol} mock market catalyst",
            body=payload.body or _default_mock_news_body(symbols),
            url=payload.url,
            symbols=symbols,
            author=payload.author,
            correlation_id=payload.correlation_id,
            trace_id=payload.trace_id,
        )
        message_id = await resolved_service.publish_news_event(event)
        return {
            "topic": TOPIC_NEWS_RAW,
            "message_id": message_id,
            "event": event.model_dump(mode="json"),
            "insight_urls": [f"/insights/{symbol}" for symbol in symbols],
        }

    return app


app = create_app()


def _normalise_symbols(symbols: list[str]) -> list[str]:
    return sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})


def _default_mock_news_body(symbols: list[str]) -> str:
    joined = ", ".join(symbols)
    return (
        f"{joined} local test coverage points to constructive ETF flow, improving "
        "liquidity, and stronger risk appetite. This mock item exists only to exercise "
        "the RAG and insight pipeline."
    )
