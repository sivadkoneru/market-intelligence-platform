"""FastAPI app for the API service."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from libs.common import (
    configure_logging,
    configure_new_relic,
    get_cache,
    get_message_bus,
    get_search_store,
    get_settings,
    get_timeseries_store,
    install_observability,
)
from services.api.routes.alerts import router as alerts_router
from services.api.routes.insights import router as insights_router
from services.api.routes.market import router as market_router
from services.api.routes.signals import router as signals_router
from services.api.service import APIService
from services.api.ws import router as ws_router


def build_default_service() -> APIService:
    """Build the offline-safe default API service used by Uvicorn."""
    return APIService(
        store=get_timeseries_store(),
        cache=get_cache(),
        bus=get_message_bus(),
    )


def create_app(service: APIService | None = None) -> FastAPI:
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        service_name="api",
        search_store=get_search_store(settings),
        log_index=settings.elasticsearch_log_index,
    )
    configure_new_relic(settings, service_name="api")
    resolved_service = service or build_default_service()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await resolved_service.prime_subscriptions()
        try:
            yield
        finally:
            await resolved_service.close()

    app = FastAPI(
        title="Market Intelligence API Service",
        version="0.1.0",
        description=(
            "Portfolio service for offline-safe market data APIs. "
            "No financial advice. No real trades."
        ),
        lifespan=lifespan,
    )
    app.state.api_service = resolved_service
    install_observability(app, service_name="api", metrics=resolved_service.metrics)

    @app.get("/")
    async def root() -> dict[str, object]:
        return {
            "service": "api",
            "message": "Portfolio project only. No financial advice. No real trades.",
            "routes": [
                "/health",
                "/metrics",
                "/symbols",
                "/market/{symbol}/latest",
                "/market/{symbol}/history",
                "/indicators/{symbol}",
                "/signals",
                "/alerts",
                "/insights/{symbol}",
                "/ws/stream",
            ],
        }

    @app.get("/health")
    async def health() -> dict[str, object]:
        return await resolved_service.health()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        return resolved_service.render_metrics()

    app.include_router(market_router)
    app.include_router(signals_router)
    app.include_router(alerts_router)
    app.include_router(insights_router)
    app.include_router(ws_router)

    return app


app = create_app()
