"""
Pydantic-settings configuration for the market intelligence platform.

All fields have offline-safe defaults so Settings() succeeds with no env vars set.
Sensitive/optional fields (LLM keys, New Relic) default to None.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from pydantic_settings import BaseSettings


def _dotenv_path() -> str | None:
    """
    Resolve the dotenv file the settings load from.

    Defaults to the repo-root ``.env``. ``MIP_DOTENV_PATH`` overrides the path;
    setting it to an empty string disables dotenv loading entirely, which the
    test gate uses to stay hermetic against a developer-local ``.env``.
    """
    return os.environ.get("MIP_DOTENV_PATH", ".env") or None


class Settings(BaseSettings):
    """Platform-wide settings loaded from environment variables."""

    # -------------------------------------------------------------------------
    # Messaging / data stores
    # -------------------------------------------------------------------------
    service_bus_connection_string: str = (
        "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
        "SharedAccessKey=SAS_KEY_VALUE_HERE;UseDevelopmentEmulator=true;"
    )
    redis_url: str = "redis://localhost:6379/0"
    druid_url: str = "http://localhost:8888"
    elasticsearch_url: str = "http://localhost:9200"
    postgres_dsn: str = "postgresql+asyncpg://market_intel:market_intel@localhost:5432/market_intel"

    # -------------------------------------------------------------------------
    # LLM / AI provider (offline mock by default; one OpenAI-compatible client
    # for live use — point OPENAI_BASE_URL at OpenAI, Azure, Anthropic, a local
    # server, or any other OpenAI-compatible endpoint).
    # -------------------------------------------------------------------------
    mock_llm: bool = True
    openai_api_key: str | None = None
    openai_base_url: str | None = None
    openai_chat_model: str = "gpt-4o-mini"
    openai_embedding_model: str = "text-embedding-3-small"

    # -------------------------------------------------------------------------
    # Observability
    # -------------------------------------------------------------------------
    new_relic_license_key: str | None = None
    new_relic_config_file: str | None = None
    new_relic_app_name: str | None = None
    new_relic_environment: str | None = None
    elasticsearch_log_index: str | None = None

    # -------------------------------------------------------------------------
    # Service identity / logging
    # -------------------------------------------------------------------------
    log_level: str = "INFO"
    service_name: str = "market-intel"

    model_config = {
        "env_file": _dotenv_path(),
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
    }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached singleton Settings instance."""
    return Settings()


def resolve_settings(settings: Any = None) -> Settings:
    """
    Return *settings*, falling back to the cached singleton.

    Every port factory takes an optional settings object so tests can inject
    one; this is the shared "use what I was given, else the process default"
    step they all opened with.
    """
    if settings is None:
        return get_settings()
    return settings


def is_default(field_name: str, value: Any) -> bool:
    """
    Report whether *value* is the shipped default for ``Settings.<field_name>``.

    The port factories choose between a fake and a real client by asking "is
    this still the placeholder?". Comparing against the field's declared default
    keeps that decision tied to the one place the default is written. Repeating
    the literal in each factory meant changing a default here — a port, a host —
    silently flipped that factory into constructing a *real* client against an
    address nobody configured.
    """
    return value == Settings.model_fields[field_name].default
