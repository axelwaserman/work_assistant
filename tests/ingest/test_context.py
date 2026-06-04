"""Tests for IngestContext and SqliteDbFactory."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.ingest.fakes import FakeClock, FakeMCPClient
from work_assistant.ingest.context import IngestContext, SqliteDbFactory


def test_sqlite_factory_opens_fresh_connection_per_call(initialized_db: Path) -> None:
    factory = SqliteDbFactory(db_path=initialized_db)
    with factory.open() as conn1, factory.open() as conn2:
        assert conn1 is not conn2
        conn1.execute("INSERT INTO worker_locks(name, pid, acquired_at) VALUES ('a', 1, 1)")
        row = conn2.execute("SELECT pid FROM worker_locks WHERE name='a'").fetchone()
        assert row["pid"] == 1


def test_sqlite_factory_applies_pragmas(initialized_db: Path) -> None:
    factory = SqliteDbFactory(db_path=initialized_db)
    with factory.open() as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_ingest_context_is_frozen(initialized_db: Path) -> None:
    """frozen=True dataclass: assignment raises FrozenInstanceError."""
    factory = SqliteDbFactory(db_path=initialized_db)
    fake_logger_calls: list[tuple[str, dict[str, object]]] = []

    class _StubLogger:
        def info(self, event: str, **kw: object) -> None:
            fake_logger_calls.append((event, dict(kw)))

        warning = info
        error = info
        debug = info

    ctx = IngestContext(
        db=factory,
        mcp=FakeMCPClient(),
        logger=_StubLogger(),  # type: ignore[arg-type]
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 4, 0, 0, 0, tzinfo=UTC)),
    )
    with pytest.raises(FrozenInstanceError):
        ctx.db = factory  # type: ignore[misc]
