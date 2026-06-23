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


def pytest_configure(config):
    """Register pytest markers."""
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "slow: marks tests as slow-running")
