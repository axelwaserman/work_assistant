"""Tests for work_assistant.ingest.logging_bind."""

from __future__ import annotations

import structlog

from work_assistant.ingest.logging_bind import bind_source_logger, configure_structlog


def test_configure_structlog_is_idempotent() -> None:
    configure_structlog()
    configure_structlog()  # second call must not raise
    log = structlog.get_logger("ingest")
    log.info("ok")


def test_bind_source_logger_attaches_keys() -> None:
    configure_structlog()
    log = bind_source_logger(source="slack", run_id="abc123")
    bindings = structlog.contextvars.get_contextvars()
    # The bind happens on the BoundLogger, not contextvars; inspect its context:
    assert log._context.get("source") == "slack"  # type: ignore[attr-defined]
    assert log._context.get("run_id") == "abc123"  # type: ignore[attr-defined]
    # Avoid contextvars false positive
    _ = bindings
