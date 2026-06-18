"""
Pydantic-settings configuration for the market intelligence platform.

All fields have offline-safe defaults so Settings() succeeds with no env vars set.
Sensitive/optional fields (LLM keys, New Relic) default to None.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal, Optional

from pydantic_settings import BaseSettings


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
    # LLM / AI providers (all optional — offline by default)
    # -------------------------------------------------------------------------
    mock_llm: bool = True
    llm_provider: Literal["auto", "mock", "azure_openai", "anthropic"] = "auto"
    embedding_provider: Literal["auto", "mock", "azure_openai"] = "auto"
    azure_openai_api_key: Optional[str] = None
    azure_openai_endpoint: Optional[str] = None
    anthropic_api_key: Optional[str] = None

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
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
    }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached singleton Settings instance."""
    return Settings()
