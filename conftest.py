"""
Pytest configuration and shared fixtures for the market intelligence platform.

This module registers pytest markers and provides minimal test fixtures.
Heavy fixtures (fakes, mocks) are added in later tasks.
"""


def pytest_configure(config):
    """Register pytest markers."""
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "slow: marks tests as slow-running")
