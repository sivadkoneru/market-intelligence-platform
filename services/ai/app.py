"""
FastAPI app for the AI-analysis service.
"""

from __future__ import annotations

from fastapi import FastAPI

from libs.common import get_cache, get_message_bus, get_search_store
from libs.common.service_app import (
    bootstrap_service_logging,
    create_service_app,
    worker_lifespan,
)
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
    bootstrap_service_logging("ai")
    resolved_service = service or build_default_service()

    return create_service_app(
        service_name="ai",
        display_name="ai-analysis",
        title="Market Intelligence AI Analysis Service",
        summary="Portfolio service for offline-safe RAG market analysis.",
        service=resolved_service,
        state_attr="ai_service",
        render_metrics=resolved_service.metrics.render,
        lifespan=worker_lifespan(
            resolved_service.run_forever if run_on_startup else None,
            task_name="ai-worker",
            state_attr="ai_task",
            close=resolved_service.close,
        ),
    )


app = create_app()
