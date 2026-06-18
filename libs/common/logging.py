"""
Structured JSON logging for the market intelligence platform.

Uses structlog with contextvars-based context propagation so that
correlation_id and trace_id are injected into every log line automatically.

Usage::

    from libs.common.logging import configure_logging, get_logger, bind_correlation_id

    configure_logging(level="INFO")
    bind_correlation_id("req-abc-123")
    log = get_logger(__name__)
    log.info("ingestion.start", symbol="BTCUSDT")

No bare ``print`` is used anywhere in this module.
"""

from __future__ import annotations

import logging
import sys
from contextvars import ContextVar
from typing import Any

import structlog

# ---------------------------------------------------------------------------
# ContextVars for per-request / per-task context
# ---------------------------------------------------------------------------

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)
_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)
_extra_context: ContextVar[dict[str, Any]] = ContextVar("extra_context", default={})

_configured = False


# ---------------------------------------------------------------------------
# Context injection processor
# ---------------------------------------------------------------------------


def _inject_context(
    logger: Any, method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Structlog processor: injects correlation/trace IDs and extra context into every event."""
    cid = _correlation_id.get()
    if cid is not None:
        event_dict.setdefault("correlation_id", cid)

    tid = _trace_id.get()
    if tid is not None:
        event_dict.setdefault("trace_id", tid)

    extra = _extra_context.get()
    for k, v in extra.items():
        event_dict.setdefault(k, v)

    return event_dict


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def configure_logging(level: str | None = None) -> None:
    """
    Configure structlog for JSON output with ISO timestamps.

    Safe to call multiple times (idempotent). The *level* parameter overrides
    the standard library root logger level; defaults to "INFO".
    """
    global _configured  # noqa: PLW0603

    resolved_level = (level or "INFO").upper()
    numeric_level = getattr(logging, resolved_level, logging.INFO)

    # Configure standard-library logging (structlog routes through it)
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
        force=True,
    )

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        _inject_context,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
        foreign_pre_chain=shared_processors,
    )

    root_logger = logging.getLogger()
    # Replace handlers to avoid duplicate output on re-configure
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(numeric_level)

    _configured = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structlog bound logger for *name*."""
    if not _configured:
        configure_logging()
    return structlog.get_logger(name)


def bind_correlation_id(value: str) -> None:
    """Bind *value* as ``correlation_id`` on the current context."""
    _correlation_id.set(value)


def bind_trace_id(value: str) -> None:
    """Bind *value* as ``trace_id`` on the current context."""
    _trace_id.set(value)


def bind_context(**kwargs: Any) -> None:
    """Bind arbitrary key/value pairs into the current logging context."""
    current = dict(_extra_context.get())
    current.update(kwargs)
    _extra_context.set(current)


def reset_context() -> None:
    """Clear all bound context variables (useful between tests)."""
    _correlation_id.set(None)
    _trace_id.set(None)
    _extra_context.set({})
