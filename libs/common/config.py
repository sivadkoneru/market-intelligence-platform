"""
Pydantic-settings configuration for the market intelligence platform.

All fields have offline-safe defaults so Settings() succeeds with no env vars set.
Sensitive/optional fields (LLM keys, New Relic) default to None.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings


def _dotenv_path() -> Optional[str]:
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
    postgres_dsn: str = (
        "postgresql+asyncpg://market_intel:market_intel@localhost:5432/market_intel"
    )

    # -------------------------------------------------------------------------
    # LLM / AI provider (offline mock by default; one OpenAI-compatible client
    # for live use — point OPENAI_BASE_URL at OpenAI, Azure, Anthropic, a local
    # server, or any other OpenAI-compatible endpoint).
    # -------------------------------------------------------------------------
    mock_llm: bool = True
    openai_api_key: Optional[str] = None
    openai_base_url: Optional[str] = None
    openai_chat_model: str = "gpt-4o-mini"
    openai_embedding_model: str = "text-embedding-3-small"

    # -------------------------------------------------------------------------
    # Observability
    # -------------------------------------------------------------------------
    new_relic_license_key: Optional[str] = None
    new_relic_config_file: Optional[str] = None
    new_relic_app_name: Optional[str] = None
    new_relic_environment: Optional[str] = None
    elasticsearch_log_index: Optional[str] = None

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
