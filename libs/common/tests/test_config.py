"""Tests for libs/common/config.py — offline defaults and env override."""

import os

import libs.common.config as cfg


def test_dotenv_path_defaults_to_repo_env(monkeypatch):
    """With no override, settings load from the repo-root .env."""
    monkeypatch.delenv("MIP_DOTENV_PATH", raising=False)
    assert cfg._dotenv_path() == ".env"


def test_dotenv_path_can_be_disabled(monkeypatch):
    """An empty MIP_DOTENV_PATH disables dotenv loading (hermetic test gate)."""
    monkeypatch.setenv("MIP_DOTENV_PATH", "")
    assert cfg._dotenv_path() is None


def test_dotenv_path_can_point_at_a_custom_file(monkeypatch):
    """A non-empty MIP_DOTENV_PATH overrides which env file is read."""
    monkeypatch.setenv("MIP_DOTENV_PATH", "/tmp/custom.env")
    assert cfg._dotenv_path() == "/tmp/custom.env"


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
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "OPENAI_CHAT_MODEL",
        "OPENAI_EMBEDDING_MODEL",
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
        assert s.log_level == "INFO"
        assert s.service_name == "market-intel"
        assert s.new_relic_config_file is None
        assert s.new_relic_app_name is None
        assert s.new_relic_environment is None
        assert s.elasticsearch_log_index is None
        # Optional secrets default to None; model names have offline-safe defaults.
        assert s.openai_api_key is None
        assert s.openai_chat_model == "gpt-4o-mini"
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


def test_settings_env_override(monkeypatch):
    """An env var override is picked up correctly."""
    monkeypatch.setenv("SERVICE_NAME", "test-service")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("MOCK_LLM", "false")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
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
    assert s.openai_api_key == "sk-test"
    assert s.elasticsearch_log_index == "logs-custom"
    assert s.new_relic_app_name == "nr-app"


def test_settings_openai_compatible_config(monkeypatch):
    """OpenAI-compatible provider settings load from the environment."""
    monkeypatch.setenv("MOCK_LLM", "false")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://proxy.example/v1")
    monkeypatch.setenv("OPENAI_CHAT_MODEL", "gpt-custom")
    monkeypatch.setenv("OPENAI_EMBEDDING_MODEL", "embed-custom")

    import importlib

    import libs.common.config as cfg_module

    importlib.reload(cfg_module)
    s = cfg_module.Settings()
    assert s.openai_api_key == "sk-test"
    assert s.openai_base_url == "https://proxy.example/v1"
    assert s.openai_chat_model == "gpt-custom"
    assert s.openai_embedding_model == "embed-custom"


def test_settings_model_name_defaults(monkeypatch):
    """Model names have offline-safe defaults when unset."""
    for key in ("OPENAI_CHAT_MODEL", "OPENAI_EMBEDDING_MODEL"):
        monkeypatch.delenv(key, raising=False)

    import importlib

    import libs.common.config as cfg_module

    importlib.reload(cfg_module)
    s = cfg_module.Settings()
    assert s.openai_chat_model == "gpt-4o-mini"
    assert s.openai_embedding_model == "text-embedding-3-small"


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
