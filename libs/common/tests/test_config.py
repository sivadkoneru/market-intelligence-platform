"""Tests for libs/common/config.py — offline defaults and env override."""

import os


def test_settings_defaults_offline():
    """Settings() must not fail with no env vars set."""
    # Clear relevant env vars to ensure offline safety
    env_keys = [
        "SERVICE_BUS_CONNECTION_STRING",
        "REDIS_URL",
        "DRUID_URL",
        "ELASTICSEARCH_URL",
        "POSTGRES_DSN",
        "MOCK_LLM",
        "LLM_PROVIDER",
        "EMBEDDING_PROVIDER",
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_ENDPOINT",
        "ANTHROPIC_API_KEY",
        "NEW_RELIC_LICENSE_KEY",
        "NEW_RELIC_CONFIG_FILE",
        "NEW_RELIC_APP_NAME",
        "NEW_RELIC_ENVIRONMENT",
        "ELASTICSEARCH_LOG_INDEX",
        "LOG_LEVEL",
        "SERVICE_NAME",
    ]
    saved = {k: os.environ.pop(k, None) for k in env_keys}
    try:
        from libs.common.config import Settings

        s = Settings()
        assert s.mock_llm is True
        assert s.llm_provider == "auto"
        assert s.embedding_provider == "auto"
        assert s.log_level == "INFO"
        assert s.service_name == "market-intel"
        assert s.new_relic_config_file is None
        assert s.new_relic_app_name is None
        assert s.new_relic_environment is None
        assert s.elasticsearch_log_index is None
        # All optional secrets default to None or empty string — just confirm no exception
        assert s.azure_openai_api_key is None or isinstance(s.azure_openai_api_key, str)
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


def test_settings_env_override(monkeypatch):
    """An env var override is picked up correctly."""
    monkeypatch.setenv("SERVICE_NAME", "test-service")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("MOCK_LLM", "false")
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("EMBEDDING_PROVIDER", "azure_openai")
    monkeypatch.setenv("ELASTICSEARCH_LOG_INDEX", "logs-custom")
    monkeypatch.setenv("NEW_RELIC_APP_NAME", "nr-app")

    # Force re-import to pick up env changes
    import importlib

    import libs.common.config as cfg_module

    importlib.reload(cfg_module)
    s = cfg_module.Settings()
    assert s.service_name == "test-service"
    assert s.log_level == "DEBUG"
    assert s.mock_llm is False
    assert s.llm_provider == "anthropic"
    assert s.embedding_provider == "azure_openai"
    assert s.elasticsearch_log_index == "logs-custom"
    assert s.new_relic_app_name == "nr-app"


def test_get_settings_returns_settings_instance():
    from libs.common.config import Settings, get_settings

    s = get_settings()
    assert isinstance(s, Settings)


def test_get_settings_is_cached():
    """get_settings() should return the same object on repeated calls (lru_cache)."""
    from libs.common.config import get_settings

    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2


def test_redis_url_default():
    """Redis URL has a sensible offline default."""
    import os
    saved = os.environ.pop("REDIS_URL", None)
    try:
        from libs.common.config import Settings
        s = Settings()
        assert isinstance(s.redis_url, str) and len(s.redis_url) > 0
    finally:
        if saved:
            os.environ["REDIS_URL"] = saved
