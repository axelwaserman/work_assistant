"""Tests for work_assistant.logging_setup."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from work_assistant import logging_setup, paths


def test_setup_creates_log_file(isolated_home: Path) -> None:
    paths.ensure_dirs()
    logging_setup.setup("test-proc")
    logger = logging.getLogger("work_assistant.test")
    logger.info("hello world", extra={"event_id": 42})
    for handler in logging.getLogger().handlers:
        handler.flush()

    log_file = paths.logs_dir() / "test-proc.log"
    assert log_file.exists()
    line = log_file.read_text().strip().splitlines()[-1]
    record = json.loads(line)
    assert record["msg"] == "hello world"
    assert record["level"] == "INFO"
    assert record["logger"] == "work_assistant.test"
    assert record["event_id"] == 42
    assert record["proc"] == "test-proc"


def test_setup_is_idempotent(isolated_home: Path) -> None:
    paths.ensure_dirs()
    logging_setup.setup("test-proc")
    logging_setup.setup("test-proc")  # second call must not duplicate handlers
    handlers = logging.getLogger().handlers
    file_handlers = [h for h in handlers if type(h) is logging.FileHandler]
    assert len(file_handlers) == 1
