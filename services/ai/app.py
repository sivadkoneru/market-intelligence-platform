"""
FastAPI app for the AI-analysis service.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from libs.common import configure_logging, get_cache, get_message_bus, get_search_store
from services.ai.llm import get_provider_bundle
from services.ai.rag import RAGPipeline
from services.ai.service import AIAnalysisService


def build_default_service() -> AIAnalysisService:
    """Build the offline-safe default AI-analysis service used by Uvicorn."""
    providers = get_provider_bundle()
    search_store = get_search_store()
    return AIAnalysisService(
        bus=get_message_bus(),
        cache=get_cache(),
        search_store=search_store,
        rag_pipeline=RAGPipeline(
            search_store=search_store,
            embedding_provider=providers.embedder,
        ),
        llm_provider=providers.generator,
    )


def create_app(
    service: AIAnalysisService | None = None,
    *,
    run_on_startup: bool = True,
) -> FastAPI:
    configure_logging()
    resolved_service = service or build_default_service()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        worker_task: asyncio.Task[object] | None = None
        if run_on_startup:
            worker_task = asyncio.create_task(resolved_service.run_forever())
            app.state.ai_task = worker_task

        try:
            yield
        finally:
            if worker_task is not None and not worker_task.done():
                worker_task.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_task

    app = FastAPI(
        title="Market Intelligence AI Analysis Service",
        version="0.1.0",
        description=(
            "Portfolio service for offline-safe RAG market analysis. "
            "No financial advice. No real trades."
        ),
        lifespan=lifespan,
    )
    app.state.ai_service = resolved_service

    @app.get("/")
    async def root() -> dict[str, object]:
        return {
            "service": "ai-analysis",
            "message": "Portfolio project only. No financial advice. No real trades.",
        }

    @app.get("/health")
    async def health() -> dict[str, object]:
        return await resolved_service.health()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        return resolved_service.metrics.render()

    return app


app = create_app()
