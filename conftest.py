"""
Pytest configuration and shared fixtures for the market intelligence platform.

This module registers pytest markers and provides minimal test fixtures.
Heavy fixtures (fakes, mocks) are added in later tasks.
"""

import os

# Hermetic, offline-safe gate: the suite must pass with zero secrets/infra using
# shipped defaults + fakes. Disable dotenv loading before libs.common.config is
# imported so a developer-local .env (real emulator/LLM connection strings) can
# never leak in and flip "offline default" assertions. This survives the
# importlib.reload() that some config tests perform, because the dotenv path is
# re-resolved from this env var at class-definition time. Operators can still set
# MIP_DOTENV_PATH to a real path to point the suite at a specific env file.
os.environ.setdefault("MIP_DOTENV_PATH", "")


def _isolate_settings_environment() -> None:
    """
    Drop ambient Settings env vars so the gate is hermetic.

    Blanking the dotenv path is not enough on its own: a developer who exports
    ``SERVICE_BUS_CONNECTION_STRING`` / ``MOCK_LLM`` / ``OPENAI_API_KEY`` from
    their shell profile still makes every factory pick a *real* client, which
    turns the offline-default assertions red and would let the suite reach the
    network. Field names are read off ``Settings`` rather than hard-coded so the
    scrub cannot drift as config grows.

    Escape hatch: setting ``MIP_DOTENV_PATH`` to a real file is an explicit
    "run against this configuration" request, so the ambient environment is
    left untouched in that case.
    """
    if os.environ.get("MIP_DOTENV_PATH"):
        return

    from libs.common.config import Settings

    field_names = {name.lower() for name in Settings.model_fields}
    for key in [k for k in os.environ if k.lower() in field_names]:
        del os.environ[key]


_isolate_settings_environment()


def pytest_configure(config):
    """Register pytest markers."""
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "slow: marks tests as slow-running")
