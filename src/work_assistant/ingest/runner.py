"""Per-source runner.

`run_source(source)` walks `source.fetch(cursor)`, persists each batch in a
single SQLite transaction, advances the cursor on commit, and enforces the
two-consecutive-zero-insert-batches guard.

`run_source_safely(source)` wraps `run_source` so a failure in one source
cannot cancel siblings under `asyncio.gather(return_exceptions=True)` (the
gather still receives a `SourceRunResult`, never an exception).
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from typing import Literal

from work_assistant.ingest.context import DbFactory
from work_assistant.ingest.errors import (
    ErrorBucket,
    SourceStallError,
    classify,
)
from work_assistant.ingest.models import Cursor, NormalizedEvent
from work_assistant.ingest.source import Source

SourceStatus = Literal["ok", "error", "skipped"]


@dataclass(frozen=True)
class SourceRunResult:
    name: str
    status: SourceStatus
    inserted: int = 0
    ignored: int = 0
    bucket: ErrorBucket | None = None
    exc: BaseException | None = None


_INSERT_EVENT_SQL = (
    "INSERT OR IGNORE INTO events ("
    " source, source_id, source_link, content_hash, occurred_at, ingested_at,"
    " actor, thread_key, kind, title, body, metadata_json"
    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

_LOAD_CURSOR_SQL = "SELECT cursor FROM ingest_cursors WHERE source = ?"
_UPSERT_CURSOR_SQL = (
    "INSERT INTO ingest_cursors(source, cursor, updated_at, last_status) "
    "VALUES (?, ?, ?, 'ok') "
    "ON CONFLICT(source) DO UPDATE SET "
    " cursor = excluded.cursor, updated_at = excluded.updated_at, last_status = 'ok'"
)
_UPSERT_CURSOR_ERROR_SQL = (
    "INSERT INTO ingest_cursors(source, cursor, updated_at, last_status) "
    "VALUES (?, COALESCE((SELECT cursor FROM ingest_cursors WHERE source = ?), ''), ?, ?) "
    "ON CONFLICT(source) DO UPDATE SET "
    " updated_at = excluded.updated_at, last_status = excluded.last_status"
)


def _load_cursor_text(conn: sqlite3.Connection, source_name: str) -> str | None:
    row = conn.execute(_LOAD_CURSOR_SQL, (source_name,)).fetchone()
    if row is None:
        return None
    text = row["cursor"]
    return text if text else None


def _persist_batch(
    conn: sqlite3.Connection,
    *,
    source_name: str,
    events: list[NormalizedEvent],
    next_cursor_json: str,
    now_unix: int,
) -> tuple[int, int]:
    """Insert events and upsert the cursor, all in a single transaction.

    Returns `(inserted, ignored)`.
    """
    inserted = 0
    ignored = 0
    conn.execute("BEGIN")
    try:
        for ev in events:
            cur = conn.execute(
                _INSERT_EVENT_SQL,
                (
                    ev.source,
                    ev.source_id,
                    ev.source_link,
                    ev.content_hash,
                    ev.occurred_at,
                    now_unix,
                    ev.actor,
                    ev.thread_key,
                    ev.kind,
                    ev.title,
                    ev.body,
                    ev.metadata.model_dump_json(),
                ),
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                ignored += 1
        conn.execute(_UPSERT_CURSOR_SQL, (source_name, next_cursor_json, now_unix))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return inserted, ignored


def _persist_error_status(
    *,
    db_factory: DbFactory,
    source_name: str,
    now_unix: int,
    detail: str,
) -> None:
    """Best-effort: write `last_status='error: ...'` without changing the cursor."""
    with db_factory.open() as conn:
        conn.execute(
            _UPSERT_CURSOR_ERROR_SQL,
            (source_name, source_name, now_unix, f"error: {detail}"),
        )


async def run_source(source: Source) -> SourceRunResult:
    """Run a single source. Raises on unhandled exception."""
    ctx = source.ctx
    cursor: Cursor | None = None
    consecutive_zero = 0
    inserted_total = 0
    ignored_total = 0

    with ctx.db.open() as conn:
        existing_cursor_text = _load_cursor_text(conn, source.name)

    if existing_cursor_text is not None:
        # Each Source's per-source plan parses this into its Cursor subclass.
        # The scaffold can't parse a per-source shape, so we hand the raw JSON
        # to the source via the `model_validate_json` route once subclasses ship.
        # For now: pass `None` so the source treats it as first-run. Per-source
        # plans override this method to parse their own cursor shape.
        cursor = None

    async for batch in source.fetch(cursor):
        with ctx.db.open() as conn:
            inserted, ignored = _persist_batch(
                conn,
                source_name=source.name,
                events=batch.events,
                next_cursor_json=batch.next_cursor.model_dump_json(),
                now_unix=ctx.clock.now_unix(),
            )
        inserted_total += inserted
        ignored_total += ignored
        ctx.logger.info(
            "batch_committed",
            inserted=inserted,
            ignored=ignored,
            events=len(batch.events),
            status=batch.status,
        )
        if batch.events and inserted == 0:
            consecutive_zero += 1
            ctx.logger.warning(
                "zero_insert_batch",
                ignored=ignored,
                consecutive=consecutive_zero,
            )
            if consecutive_zero >= 2:
                raise SourceStallError(
                    f"{source.name}: 2 consecutive batches inserted 0 / "
                    f"ignored {ignored}+ events. Likely pagination or dedup-key bug."
                )
        else:
            consecutive_zero = 0

    return SourceRunResult(
        name=source.name,
        status="ok",
        inserted=inserted_total,
        ignored=ignored_total,
    )


async def run_source_safely(source: Source) -> SourceRunResult:
    """Wraps `run_source` so siblings under gather() never see the raw raise.

    Propagates `KeyboardInterrupt` and `asyncio.CancelledError` so cooperative
    shutdown still works; classifies everything else.
    """
    try:
        return await run_source(source)
    except (KeyboardInterrupt, asyncio.CancelledError):
        raise
    except BaseException as exc:
        bucket = classify(exc)
        try:
            _persist_error_status(
                db_factory=source.ctx.db,
                source_name=source.name,
                now_unix=source.ctx.clock.now_unix(),
                detail=f"{type(exc).__name__}: {exc}",
            )
        except Exception as inner:
            source.ctx.logger.error(
                "status_write_failed",
                primary=repr(exc),
                inner=repr(inner),
            )
        return SourceRunResult(
            name=source.name,
            status="error",
            bucket=bucket,
            exc=exc,
        )
