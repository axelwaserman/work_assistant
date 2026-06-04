"""`worker_locks` row management for the ingest worker.

Single row per worker name (`'ingest'`). Reclaim logic:
- `INSERT OR IGNORE` on first attempt.
- If the row exists, examine `(pid, acquired_at)`:
  - PID is no longer alive (`os.kill(pid, 0)` raises `ProcessLookupError`)
    → reclaim: DELETE then re-attempt INSERT OR IGNORE.
  - `now - acquired_at > LOCK_TTL_SECONDS` → reclaim.
  - Otherwise → raise `LockHeldError` (worker exits clean, code 3).

Heartbeat refreshes `acquired_at` while the worker runs so a long batch is
not reclaimed by a sibling cron tick.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from collections.abc import Callable
from contextlib import asynccontextmanager, suppress
from typing import Self

from work_assistant.ingest.clock import Clock
from work_assistant.ingest.errors import LockHeldError

LOCK_NAME = "ingest"
LOCK_TTL_SECONDS = 1800
HEARTBEAT_INTERVAL_S = 60.0


def _is_pid_alive(pid: int) -> bool:
    """Cheap macOS/Linux liveness check via `kill(pid, 0)`."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _reclaim_if_stale(conn: sqlite3.Connection, *, now_unix: int) -> bool:
    """If the lock row is stale (dead PID or TTL expired), DELETE it.
    Returns True if the row was deleted; False if it's still held by a live worker."""
    row = conn.execute(
        "SELECT pid, acquired_at FROM worker_locks WHERE name = ?", (LOCK_NAME,)
    ).fetchone()
    if row is None:
        return True
    pid, acquired_at = row["pid"], row["acquired_at"]
    expired = (now_unix - acquired_at) > LOCK_TTL_SECONDS
    dead = not _is_pid_alive(pid)
    if expired or dead:
        conn.execute("DELETE FROM worker_locks WHERE name = ? AND pid = ?", (LOCK_NAME, pid))
        return True
    return False


def acquire_lock(conn: sqlite3.Connection, *, pid: int, clock: Clock) -> None:
    """Acquire the `'ingest'` row. Raises `LockHeldError` if held by a live worker."""
    now = clock.now_unix()
    cur = conn.execute(
        "INSERT OR IGNORE INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
        (LOCK_NAME, pid, now),
    )
    if cur.rowcount == 1:
        return
    reclaimed = _reclaim_if_stale(conn, now_unix=now)
    if not reclaimed:
        raise LockHeldError(f"lock {LOCK_NAME!r} held by a live worker")
    cur = conn.execute(
        "INSERT OR IGNORE INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
        (LOCK_NAME, pid, now),
    )
    if cur.rowcount != 1:
        raise LockHeldError(f"lock {LOCK_NAME!r} race: reclaim succeeded but re-insert failed")


def release_lock(conn: sqlite3.Connection, *, pid: int) -> None:
    """Best-effort release. No-op if a different pid now holds the row."""
    conn.execute("DELETE FROM worker_locks WHERE name = ? AND pid = ?", (LOCK_NAME, pid))


def _refresh(conn: sqlite3.Connection, *, pid: int, now_unix: int) -> None:
    conn.execute(
        "UPDATE worker_locks SET acquired_at = ? WHERE name = ? AND pid = ?",
        (now_unix, LOCK_NAME, pid),
    )


class Heartbeat:
    """Async context manager that refreshes `acquired_at` every `interval_s`.

    Use as `async with Heartbeat(...)` inside the worker run. Cancellation on
    exit is robust to a refresh tick failing — we log and keep going."""

    def __init__(
        self,
        *,
        db_conn_factory: Callable[[], sqlite3.Connection],
        pid: int,
        clock: Clock,
        interval_s: float = HEARTBEAT_INTERVAL_S,
    ) -> None:
        self._db_conn_factory = db_conn_factory
        self._pid = pid
        self._clock = clock
        self._interval_s = interval_s
        self._task: asyncio.Task[None] | None = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval_s)
            try:
                conn = self._db_conn_factory()
                try:
                    _refresh(conn, pid=self._pid, now_unix=self._clock.now_unix())
                finally:
                    conn.close()
            except sqlite3.Error:
                # Diagnostic-only refresh failure; keep heartbeat alive.
                continue

    async def __aenter__(self) -> Self:
        self._task = asyncio.create_task(self._run(), name="ingest-heartbeat")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None


@asynccontextmanager
async def heartbeat_managed(
    *,
    db_conn_factory: Callable[[], sqlite3.Connection],
    pid: int,
    clock: Clock,
    interval_s: float = HEARTBEAT_INTERVAL_S,
):
    """Async helper for code that prefers `async with` syntax."""
    hb = Heartbeat(
        db_conn_factory=db_conn_factory,
        pid=pid,
        clock=clock,
        interval_s=interval_s,
    )
    async with hb:
        yield hb
