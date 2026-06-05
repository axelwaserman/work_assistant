"""Tests for work_assistant.ingest.runner."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
import structlog

from tests.ingest.fakes import FakeClock, FakeMCPClient, StubSource, make_event
from work_assistant.ingest.context import IngestContext, SqliteDbFactory
from work_assistant.ingest.errors import (
    PermanentIngestError,
    SourceStallError,
)
from work_assistant.ingest.models import Batch, Cursor
from work_assistant.ingest.runner import (
    run_source,
    run_source_safely,
)


def _ctx(initialized_db: Path) -> IngestContext:
    return IngestContext(
        db=SqliteDbFactory(db_path=initialized_db),
        mcp=FakeMCPClient(),
        logger=structlog.get_logger("test"),
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)),
    )


@pytest.mark.asyncio
async def test_happy_path_inserts_events_and_advances_cursor(
    initialized_db: Path,
) -> None:
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    cls = StubSource.make(
        name="slack",
        batches=[
            Batch(
                events=[make_event(source_id="m1"), make_event(source_id="m2")],
                next_cursor=cursor,
                status="ok",
            )
        ],
    )
    src = cls(ctx)
    result = await run_source(src)
    assert result.status == "ok"
    assert result.inserted == 2
    assert result.ignored == 0
    with sqlite3.connect(initialized_db) as conn:
        n = conn.execute("SELECT count(*) FROM events").fetchone()[0]
    assert n == 2


@pytest.mark.asyncio
async def test_dedup_ignores_duplicate_inserts(initialized_db: Path) -> None:
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    e = make_event(source_id="m1")
    cls = StubSource.make(
        name="slack",
        batches=[Batch(events=[e, e], next_cursor=cursor, status="ok")],
    )
    src = cls(ctx)
    result = await run_source(src)
    assert result.inserted == 1
    assert result.ignored == 1


@pytest.mark.asyncio
async def test_two_zero_insert_batches_raises_stall(initialized_db: Path) -> None:
    """Same event in two consecutive batches → stall after the second."""
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    e = make_event(source_id="m1")
    cls = StubSource.make(
        name="slack",
        batches=[
            Batch(events=[e], next_cursor=cursor, status="ok"),  # inserted=1
            Batch(events=[e], next_cursor=cursor, status="ok"),  # inserted=0 #1
            Batch(events=[e], next_cursor=cursor, status="ok"),  # inserted=0 #2 → stall
        ],
    )
    src = cls(ctx)
    with pytest.raises(SourceStallError):
        await run_source(src)


@pytest.mark.asyncio
async def test_safe_runner_catches_and_classifies_transient(
    initialized_db: Path,
) -> None:
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    cls = StubSource.make(
        name="slack",
        batches=[Batch(events=[make_event()], next_cursor=cursor, status="ok")],
        raise_after=1,
        raise_exc=RuntimeError("network blip"),
    )
    src = cls(ctx)
    result = await run_source_safely(src)
    assert result.status == "error"
    assert result.bucket == "transient"
    assert isinstance(result.exc, RuntimeError)


@pytest.mark.asyncio
async def test_safe_runner_classifies_permanent(initialized_db: Path) -> None:
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    cls = StubSource.make(
        name="slack",
        batches=[Batch(events=[], next_cursor=cursor, status="ok")],
        raise_after=1,
        raise_exc=PermanentIngestError("auth revoked"),
    )
    src = cls(ctx)
    result = await run_source_safely(src)
    assert result.status == "error"
    assert result.bucket == "permanent"


@pytest.mark.asyncio
async def test_safe_runner_propagates_keyboard_interrupt(initialized_db: Path) -> None:
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    cls = StubSource.make(
        name="slack",
        batches=[Batch(events=[], next_cursor=cursor, status="ok")],
        raise_after=1,
        raise_exc=KeyboardInterrupt(),
    )
    src = cls(ctx)
    with pytest.raises(KeyboardInterrupt):
        await run_source_safely(src)


@pytest.mark.asyncio
async def test_empty_batch_with_new_cursor_advances(initialized_db: Path) -> None:
    """Gmail historyId case: events=[] but cursor moves forward."""

    class _MovedCursor(Cursor):
        history_id: str

    ctx = _ctx(initialized_db)
    cls = StubSource.make(
        name="gmail",
        batches=[Batch(events=[], next_cursor=_MovedCursor(history_id="h2"), status="ok")],
    )
    src = cls(ctx)
    result = await run_source(src)
    assert result.status == "ok"
    assert result.inserted == 0
    with sqlite3.connect(initialized_db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT cursor FROM ingest_cursors WHERE source='gmail'").fetchone()
    assert row is not None
    assert "h2" in row["cursor"]
