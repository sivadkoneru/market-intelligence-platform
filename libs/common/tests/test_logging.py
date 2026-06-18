"""Tests for libs/common/logging.py — structlog JSON output with correlation IDs."""

import ast
import json
import pathlib

import pytest


def test_configure_logging_does_not_raise():
    from libs.common.logging import configure_logging

    configure_logging(level="DEBUG")  # must not raise


def test_get_logger_returns_bound_logger():
    from libs.common.logging import configure_logging, get_logger

    configure_logging(level="INFO")
    logger = get_logger("test.module")
    assert logger is not None


def test_json_output_parseable(capsys):
    """A logged event must produce parseable JSON lines."""
    from libs.common.logging import configure_logging, get_logger

    configure_logging(level="DEBUG")
    logger = get_logger("test.json")
    logger.info("hello world", key="value")
    captured = capsys.readouterr()
    # At least one line should be valid JSON
    lines = [line for line in captured.out.splitlines() if line.strip()]
    if not lines:
        # Some structlog configs write to stderr
        lines = [line for line in captured.err.splitlines() if line.strip()]
    assert lines, "Expected at least one log line"
    parsed = json.loads(lines[-1])
    assert "event" in parsed or "message" in parsed or "msg" in parsed


def test_correlation_id_appears_in_log(capsys):
    """bind_correlation_id should inject correlation_id into every subsequent log line."""
    from libs.common.logging import bind_correlation_id, configure_logging, get_logger

    configure_logging(level="DEBUG")
    bind_correlation_id("corr-999")
    logger = get_logger("test.corr")
    logger.info("checking correlation")
    captured = capsys.readouterr()
    all_out = captured.out + captured.err
    lines = [line for line in all_out.splitlines() if line.strip()]
    assert lines, "Expected at least one log line"
    last = json.loads(lines[-1])
    assert last.get("correlation_id") == "corr-999"


def test_trace_id_appears_in_log(capsys):
    """bind_trace_id should inject trace_id into every subsequent log line."""
    from libs.common.logging import bind_trace_id, configure_logging, get_logger

    configure_logging(level="DEBUG")
    bind_trace_id("trace-abc")
    logger = get_logger("test.trace")
    logger.info("checking trace")
    captured = capsys.readouterr()
    all_out = captured.out + captured.err
    lines = [line for line in all_out.splitlines() if line.strip()]
    assert lines, "Expected at least one log line"
    last = json.loads(lines[-1])
    assert last.get("trace_id") == "trace-abc"


def test_bind_context_injects_fields(capsys):
    """bind_context should inject arbitrary key/value pairs."""
    from libs.common.logging import bind_context, configure_logging, get_logger

    configure_logging(level="DEBUG")
    bind_context(service="test-svc", env="ci")
    logger = get_logger("test.ctx")
    logger.info("context check")
    captured = capsys.readouterr()
    all_out = captured.out + captured.err
    lines = [line for line in all_out.splitlines() if line.strip()]
    assert lines
    last = json.loads(lines[-1])
    assert last.get("service") == "test-svc"
    assert last.get("env") == "ci"


def test_log_level_field_present(capsys):
    """Each log line must contain a level field."""
    from libs.common.logging import configure_logging, get_logger

    configure_logging(level="DEBUG")
    logger = get_logger("test.level")
    logger.warning("level test")
    captured = capsys.readouterr()
    all_out = captured.out + captured.err
    lines = [line for line in all_out.splitlines() if line.strip()]
    assert lines
    last = json.loads(lines[-1])
    level_val = last.get("level") or last.get("severity") or last.get("log_level") or ""
    assert level_val  # some level field is present


def test_no_bare_print():
    """The logging module itself must not use bare print calls."""
    src = pathlib.Path(
        "/Users/sivakoneru/Development/market-intelligence-platform/libs/common/logging.py"
    ).read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "print":
                pytest.fail("Found bare print() call in libs/common/logging.py")
