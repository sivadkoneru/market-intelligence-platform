"""
Regression tests for the hermetic test gate.

``task test`` must pass with zero secrets and zero live infra. That guarantee
breaks silently when a developer exports platform settings (a real Service Bus
connection string, ``MOCK_LLM=0``, an API key) from their shell profile: every
factory then hands back a *real* client instead of the in-memory fake. The root
conftest strips those vars at session start; these tests keep it honest.
"""

import os

import conftest
from libs.common.config import Settings


def _settings_field_names() -> set[str]:
    return {name.lower() for name in Settings.model_fields}


def test_no_settings_env_vars_leak_into_the_suite():
    """No Settings field may be configured from the ambient environment."""
    leaked = sorted(key for key in os.environ if key.lower() in _settings_field_names())
    assert leaked == []


def test_isolate_settings_environment_strips_ambient_settings_vars(monkeypatch):
    monkeypatch.setenv("SERVICE_BUS_CONNECTION_STRING", "Endpoint=sb://real.example/;")
    monkeypatch.setenv("MOCK_LLM", "0")
    monkeypatch.setenv("REDIS_URL", "redis://real:6379/1")

    conftest._isolate_settings_environment()

    assert "SERVICE_BUS_CONNECTION_STRING" not in os.environ
    assert "MOCK_LLM" not in os.environ
    assert "REDIS_URL" not in os.environ
    # Unrelated environment variables are left alone.
    assert os.environ.get("PATH")


def test_isolate_settings_environment_respects_explicit_dotenv_path(monkeypatch):
    """Pointing MIP_DOTENV_PATH at a real file is an opt-in to a configured run."""
    monkeypatch.setenv("MIP_DOTENV_PATH", "/tmp/custom.env")
    monkeypatch.setenv("REDIS_URL", "redis://real:6379/1")

    conftest._isolate_settings_environment()

    assert os.environ["REDIS_URL"] == "redis://real:6379/1"


def test_offline_defaults_hold_under_isolation():
    """The scrub is what makes the shipped offline defaults observable."""
    settings = Settings()

    assert settings.mock_llm is True
    assert settings.openai_api_key is None
    assert "SAS_KEY_VALUE_HERE" in settings.service_bus_connection_string
