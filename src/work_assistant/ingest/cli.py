"""`wa ingest` click subcommand.

Exit codes (spec §6.2):
- 0 ok
- 1 transient error in one or more sources
- 2 usage error (unknown source, bad flag)
- 3 lock held by live worker
- 4 config-fatal
- 5 permanent error in one or more sources
- 130 KeyboardInterrupt
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import datetime

import click

from work_assistant import config as wa_config
from work_assistant import logging_setup
from work_assistant.ingest.clock import SystemClock
from work_assistant.ingest.registry import SOURCES
from work_assistant.ingest.worker import (
    EXIT_CONFIG_FATAL,
    EXIT_USAGE,
    WorkerOptions,
    run_worker,
)


def _parse_source_list(value: str | None) -> list[str] | None:
    """Comma-separated -> list. None -> None (= use config-enabled list)."""
    if value is None:
        return None
    return [s.strip() for s in value.split(",") if s.strip()]


def _parse_since(value: str | None) -> int | None:
    if value is None:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise click.BadParameter("--since must be timezone-aware ISO-8601")
    return int(dt.timestamp())


@click.command("ingest")
@click.option(
    "--source",
    "source_str",
    type=str,
    default=None,
    help="Comma-separated list of source names to run (overrides config).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Fetch + normalize but write no rows.",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="DEBUG-level structured logs.",
)
@click.option(
    "--since",
    "since_str",
    type=str,
    default=None,
    help="ISO-8601 timestamp; one-shot read-only override (cursor not persisted).",
)
def ingest(
    source_str: str | None,
    dry_run: bool,
    verbose: bool,
    since_str: str | None,
) -> None:
    """Run a single ingest pass (one process, all enabled sources)."""
    logging_setup.setup("wa-ingest")
    requested = _parse_source_list(source_str)
    try:
        since_unix = _parse_since(since_str)
    except click.BadParameter as exc:
        click.echo(str(exc), err=True)
        sys.exit(EXIT_USAGE)

    sources_enabled: list[str] = []
    config_fatal = False
    try:
        cfg = wa_config.load()
        sources_enabled = list(cfg.ingest.sources_enabled)
    except wa_config.ConfigError as exc:
        click.echo(f"config error: {exc}", err=True)
        config_fatal = True

    opts = WorkerOptions(
        registry=SOURCES,
        sources_enabled=sources_enabled,
        requested_sources=requested,
        dry_run=dry_run,
        since_unix=since_unix,
        clock=SystemClock(),
        pid=os.getpid(),
        run_id=uuid.uuid4().hex,
    )

    if config_fatal:
        sys.exit(EXIT_CONFIG_FATAL)

    code = asyncio.run(run_worker(opts))
    sys.exit(code)
