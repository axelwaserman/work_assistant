"""Top-level ingest worker.

Acquires the lock, starts the heartbeat, runs each enabled source under
`asyncio.gather(return_exceptions=False)` (each coro is wrapped in
`run_source_safely`), then computes the exit code per spec §6.2.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeVar

from work_assistant import paths
from work_assistant.ingest.clock import Clock
from work_assistant.ingest.context import DbFactory, IngestContext, SqliteDbFactory
from work_assistant.ingest.errors import LockHeldError
from work_assistant.ingest.lock import acquire_lock, heartbeat_managed, release_lock
from work_assistant.ingest.logging_bind import bind_source_logger
from work_assistant.ingest.registry import UnknownSourceError, select_sources
from work_assistant.ingest.runner import SourceRunResult, run_source_safely
from work_assistant.ingest.source import Source
from work_assistant.mcp.client import MCPClient, MCPRequest, MCPResponse

EXIT_OK = 0
EXIT_TRANSIENT = 1
EXIT_USAGE = 2
EXIT_LOCK_HELD = 3
EXIT_CONFIG_FATAL = 4
EXIT_PERMANENT = 5
EXIT_KEYBOARD_INTERRUPT = 130

_RespT = TypeVar("_RespT", bound=MCPResponse)


class _NullMCPClient(MCPClient):
    """Scaffold placeholder. Any call raises; per-source plans wire real bridges."""

    async def call(
        self,
        request: MCPRequest,
        response_model: type[_RespT],
    ) -> _RespT:
        raise RuntimeError("MCP bridge not wired; the scaffold ships without per-source MCP setup")


class _DryRunConnection(sqlite3.Connection):
    """A `sqlite3.Connection` subclass that turns every COMMIT into a ROLLBACK.

    `sqlite3.Connection.execute` is a read-only C-level attribute, so we cannot
    monkey-patch it on an existing connection. Subclassing and routing via
    `connect(..., factory=...)` is the supported escape hatch.
    """

    def execute(self, sql: str, *args: object) -> sqlite3.Cursor:  # type: ignore[override]
        if sql.strip().upper() == "COMMIT":
            return super().execute("ROLLBACK")
        return super().execute(sql, *args)


class _DryRunDbFactory(DbFactory):
    """Open dry-run connections that swallow COMMIT. No rows ever persist."""

    def __init__(self, *, db_path: Path) -> None:
        self._db_path = db_path

    @contextmanager
    def open(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path, isolation_level=None, factory=_DryRunConnection)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            yield conn
        finally:
            conn.close()


@dataclass(frozen=True)
class WorkerOptions:
    """Inputs to `run_worker`. Built by the CLI; tests construct directly."""

    registry: dict[str, type[Source]] = field(default_factory=dict)
    sources_enabled: list[str] = field(default_factory=list)
    requested_sources: list[str] | None = None
    dry_run: bool = False
    since_unix: int | None = None
    clock: Clock | None = None
    pid: int = 0
    run_id: str = ""


def compute_exit_code(
    results: list[SourceRunResult],
    *,
    lock_held: bool,
    config_fatal: bool,
) -> int:
    """Apply spec §6.2 precedence: 4 > 5 > 1 > 3 > 2 > 0."""
    if config_fatal:
        return EXIT_CONFIG_FATAL
    has_permanent = any(r.bucket == "permanent" for r in results)
    has_transient = any(r.bucket == "transient" for r in results)
    if has_permanent:
        return EXIT_PERMANENT
    if has_transient:
        return EXIT_TRANSIENT
    if lock_held:
        return EXIT_LOCK_HELD
    return EXIT_OK


def _resolve_enabled(opts: WorkerOptions) -> list[str]:
    """Pick which source names to run.

    `requested_sources` (CLI `--source`) overrides config; otherwise use the
    config-enabled list. Returns the resolved list of names.
    """
    if opts.requested_sources is not None:
        return list(opts.requested_sources)
    return list(opts.sources_enabled)


def _build_sources(
    *,
    selected: dict[str, type[Source]],
    db_factory: DbFactory,
    clock: Clock,
    run_id: str,
) -> list[Source]:
    """Build one `Source` instance per selected entry, each with its own context."""
    instances: list[Source] = []
    for name, cls in selected.items():
        logger = bind_source_logger(source=name, run_id=run_id)
        ctx = IngestContext(
            db=db_factory,
            mcp=_NullMCPClient(),
            logger=logger,
            settings=None,  # type: ignore[arg-type]
            clock=clock,
        )
        instances.append(cls(ctx))
    return instances


async def run_worker(opts: WorkerOptions) -> int:
    """Drive a single ingest run. Returns the exit code."""
    if opts.clock is None:
        raise ValueError("WorkerOptions.clock is required")

    base_logger = bind_source_logger(source="-", run_id=opts.run_id)

    try:
        names = _resolve_enabled(opts)
        selected = select_sources(registry=opts.registry, requested=names if names else None)
    except UnknownSourceError as exc:
        base_logger.error("unknown_source", detail=str(exc))
        return EXIT_USAGE

    db_path = paths.db_path()
    db_factory: DbFactory = (
        _DryRunDbFactory(db_path=db_path) if opts.dry_run else SqliteDbFactory(db_path=db_path)
    )

    def _new_lock_conn() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    try:
        with _new_lock_conn() as lock_conn:
            try:
                acquire_lock(lock_conn, pid=opts.pid, clock=opts.clock)
            except LockHeldError:
                base_logger.warning("lock_held")
                return compute_exit_code([], lock_held=True, config_fatal=False)
    except sqlite3.Error as exc:
        base_logger.error("lock_db_error", detail=repr(exc))
        return compute_exit_code([], lock_held=False, config_fatal=True)

    results: list[SourceRunResult] = []
    try:
        async with heartbeat_managed(
            db_conn_factory=_new_lock_conn,
            pid=opts.pid,
            clock=opts.clock,
        ):
            sources = _build_sources(
                selected=selected,
                db_factory=db_factory,
                clock=opts.clock,
                run_id=opts.run_id,
            )
            if not sources:
                return EXIT_OK
            results = await asyncio.gather(
                *(run_source_safely(s) for s in sources),
                return_exceptions=False,
            )
    except KeyboardInterrupt:
        base_logger.warning("keyboard_interrupt")
        return EXIT_KEYBOARD_INTERRUPT
    finally:
        with _new_lock_conn() as lock_conn:
            release_lock(lock_conn, pid=opts.pid)

    return compute_exit_code(results, lock_held=False, config_fatal=False)
