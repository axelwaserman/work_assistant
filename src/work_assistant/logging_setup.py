"""Structured logging configuration.

Two sinks: a JSON file at `~/.work_assistant/logs/<proc>.log` and a Rich
console handler. `setup(proc)` is idempotent; calling it twice does not stack
handlers.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from rich.logging import RichHandler

from work_assistant import paths

_RESERVED = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
    "asctime",
    "taskName",
}


class _JsonFormatter(logging.Formatter):
    def __init__(self, proc: str) -> None:
        super().__init__()
        self._proc = proc

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "proc": self._proc,
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            try:
                json.dumps(value)
            except (TypeError, ValueError):
                value = repr(value)
            payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


_CONFIGURED: dict[str, bool] = {"done": False}


def setup(proc: str, level: int = logging.INFO) -> None:
    """Configure the root logger to log to console (Rich) and JSON file.

    Idempotent: a second call with the same process name is a no-op.
    """
    if _CONFIGURED["done"]:
        return
    paths.ensure_dirs()

    root = logging.getLogger()
    root.setLevel(level)

    file_handler = logging.FileHandler(paths.logs_dir() / f"{proc}.log", encoding="utf-8")
    file_handler.setFormatter(_JsonFormatter(proc))
    file_handler.setLevel(level)
    root.addHandler(file_handler)

    console_handler = RichHandler(rich_tracebacks=True, show_path=False)
    console_handler.setLevel(level)
    root.addHandler(console_handler)

    _CONFIGURED["done"] = True
