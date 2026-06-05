"""structlog wiring for the ingest worker.

`configure_structlog()` is idempotent and safe to call alongside the existing
stdlib `logging_setup.setup(proc)` from Phase 0. The final structlog processor
emits the event dict as `extra=`, which the stdlib `_JsonFormatter` promotes to
top-level fields — so `source`, `run_id`, `event`, etc. are first-class keys in
the JSON file rather than nested inside `msg`.
"""

from __future__ import annotations

import logging

import structlog

_CONFIGURED = {"done": False}


def configure_structlog() -> None:
    """Configure structlog to forward events to stdlib `logging` with extras.

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
            structlog.stdlib.render_to_log_kwargs,
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
