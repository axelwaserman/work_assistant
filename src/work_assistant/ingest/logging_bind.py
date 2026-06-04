"""structlog wiring for the ingest worker.

`configure_structlog()` is idempotent and safe to call alongside the existing
stdlib `logging_setup.setup(proc)` from Phase 0. We hand the rendering off to
structlog's JSON renderer so the same JSON file the stdlib handler writes is
populated with consistently structured records.
"""

from __future__ import annotations

import logging

import structlog

_CONFIGURED = {"done": False}


def configure_structlog() -> None:
    """Configure structlog to emit JSON-shaped records via stdlib `logging`.

    Idempotent: safe to call multiple times across worker invocations and tests.
    """
    if _CONFIGURED["done"]:
        return
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _CONFIGURED["done"] = True


def bind_source_logger(*, source: str, run_id: str) -> structlog.stdlib.BoundLogger:
    """Return a `BoundLogger` pre-bound with `source` + `run_id`."""
    configure_structlog()
    return structlog.get_logger("ingest").bind(source=source, run_id=run_id)
