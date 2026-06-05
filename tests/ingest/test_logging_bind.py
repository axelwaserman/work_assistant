"""Tests for work_assistant.ingest.logging_bind."""

from __future__ import annotations

import logging

import pytest
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
    ctx = structlog.get_context(log)
    assert ctx["source"] == "slack"
    assert ctx["run_id"] == "abc123"


def test_bound_logger_writes_flat_json_to_stdlib(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """End-to-end: bound keys must reach the stdlib LogRecord as first-class extras,
    not buried inside the message string."""
    configure_structlog()
    log = bind_source_logger(source="slack", run_id="abc123")
    with caplog.at_level(logging.INFO, logger="ingest"):
        log.info("hello", count=3)
    assert len(caplog.records) == 1
    rec = caplog.records[0]
    assert rec.getMessage() == "hello"
    assert rec.__dict__["source"] == "slack"
    assert rec.__dict__["run_id"] == "abc123"
    assert rec.__dict__["count"] == 3
