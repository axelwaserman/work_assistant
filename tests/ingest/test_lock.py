"""Tests for work_assistant.ingest.lock."""

from __future__ import annotations

import asyncio
import os
import sqlite3
from datetime import UTC, datetime

import pytest

from tests.ingest.fakes import FakeClock
from work_assistant.ingest.errors import LockHeldError
from work_assistant.ingest.lock import (
    LOCK_NAME,
    LOCK_TTL_SECONDS,
    Heartbeat,
    acquire_lock,
    release_lock,
)


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock(datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC))


def _read_lock_row(conn: sqlite3.Connection) -> tuple[int, int] | None:
    row = conn.execute(
        "SELECT pid, acquired_at FROM worker_locks WHERE name = ?", (LOCK_NAME,)
    ).fetchone()
    return (row["pid"], row["acquired_at"]) if row else None


def test_acquire_first_run_inserts_row(db_conn_factory, clock: FakeClock) -> None:
    with db_conn_factory() as conn:
        acquire_lock(conn, pid=os.getpid(), clock=clock)
        row = _read_lock_row(conn)
    assert row == (os.getpid(), clock.now_unix())


def test_acquire_raises_when_live_predecessor_holds(db_conn_factory, clock: FakeClock) -> None:
    with db_conn_factory() as conn:
        conn.execute(
            "INSERT INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
            (LOCK_NAME, os.getpid(), clock.now_unix()),
        )
        with pytest.raises(LockHeldError):
            acquire_lock(conn, pid=os.getpid() + 1, clock=clock)


def test_acquire_reclaims_dead_predecessor(db_conn_factory, clock: FakeClock) -> None:
    """A row pointing at a PID with no live process is reclaimed."""
    dead_pid = 99999
    with db_conn_factory() as conn:
        conn.execute(
            "INSERT INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
            (LOCK_NAME, dead_pid, clock.now_unix() - 10),
        )
        # PID-alive check must say `dead_pid` is gone:
        # ProcessLookupError from os.kill(dead_pid, 0).
        acquire_lock(conn, pid=os.getpid(), clock=clock)
        row = _read_lock_row(conn)
    assert row == (os.getpid(), clock.now_unix())


def test_acquire_reclaims_ttl_expired_even_if_pid_live(db_conn_factory, clock: FakeClock) -> None:
    """Even if the predecessor's PID is alive, an expired acquired_at is reclaimable."""
    with db_conn_factory() as conn:
        conn.execute(
            "INSERT INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
            (LOCK_NAME, os.getpid(), clock.now_unix() - LOCK_TTL_SECONDS - 1),
        )
        acquire_lock(conn, pid=os.getpid(), clock=clock)
        row = _read_lock_row(conn)
    assert row == (os.getpid(), clock.now_unix())


def test_release_only_removes_own_row(db_conn_factory, clock: FakeClock) -> None:
    with db_conn_factory() as conn:
        acquire_lock(conn, pid=os.getpid(), clock=clock)
        release_lock(conn, pid=os.getpid())
        assert _read_lock_row(conn) is None


def test_release_no_op_if_pid_mismatch(db_conn_factory, clock: FakeClock) -> None:
    with db_conn_factory() as conn:
        acquire_lock(conn, pid=os.getpid(), clock=clock)
        release_lock(conn, pid=os.getpid() + 1)
        assert _read_lock_row(conn) is not None


@pytest.mark.asyncio
async def test_heartbeat_refreshes_acquired_at(db_conn_factory, clock: FakeClock) -> None:
    with db_conn_factory() as conn:
        acquire_lock(conn, pid=os.getpid(), clock=clock)
    hb = Heartbeat(
        db_conn_factory=db_conn_factory,
        pid=os.getpid(),
        clock=clock,
        interval_s=0.01,
    )
    async with hb:
        clock.advance(seconds=5)
        await asyncio.sleep(0.05)
    with db_conn_factory() as conn:
        row = _read_lock_row(conn)
    assert row is not None
    assert row[1] >= clock.now_unix() - 5
