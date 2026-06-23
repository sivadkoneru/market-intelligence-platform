"""
FastAPI app for the ingestion service.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from libs.common import (
    TOPIC_NEWS_RAW,
    NewsEvent,
    configure_logging,
    configure_new_relic,
    get_message_bus,
    get_search_store,
    get_settings,
    install_observability,
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
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        service_name="ingestion",
        search_store=get_search_store(settings),
        log_index=settings.elasticsearch_log_index,
    )
    configure_new_relic(settings, service_name="ingestion")
    resolved_service = service or build_default_service()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        ingestion_task: asyncio.Task[object] | None = None
        if run_on_startup:
            ingestion_task = asyncio.create_task(
                resolved_service.run(max_events=startup_max_events)
            )
            app.state.ingestion_task = ingestion_task

        try:
            yield
        finally:
            if ingestion_task is not None and not ingestion_task.done():
                ingestion_task.cancel()
                with suppress(asyncio.CancelledError):
                    await ingestion_task

    app = FastAPI(
        title="Market Intelligence Ingestion Service",
        version="0.1.0",
        description=(
            "Portfolio service for offline-safe market ingestion. "
            "No financial advice. No real trades."
        ),
        lifespan=lifespan,
    )
    app.state.ingestion_service = resolved_service
    install_observability(app, service_name="ingestion", metrics=resolved_service.metrics)

    @app.get("/health")
    async def health() -> dict[str, object]:
        return await resolved_service.health()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        return resolved_service.metrics.render()

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
            payload.event_id
            or f"mock-news-{datetime.now(tz=UTC).strftime('%Y%m%d%H%M%S%f')}"
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
