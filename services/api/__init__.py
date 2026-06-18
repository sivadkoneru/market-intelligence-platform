"""REST API service public exports."""

from services.api.service import APIMetrics, APIService

__all__ = [
    "APIService",
    "APIMetrics",
    "app",
    "build_default_service",
    "create_app",
]


def __getattr__(name: str):
    if name in {"app", "build_default_service", "create_app"}:
        from services.api.app import app, build_default_service, create_app

        return {
            "app": app,
            "build_default_service": build_default_service,
            "create_app": create_app,
        }[name]
    raise AttributeError(name)
