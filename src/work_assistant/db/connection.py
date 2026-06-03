"""SQLite connection helper for the spine database.

Every connection opens with WAL, NORMAL sync, FK on, and 5s busy timeout.
Rows are returned as `sqlite3.Row` (dict-like access).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from work_assistant import paths


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA wal_autocheckpoint=1000")
    conn.execute("PRAGMA busy_timeout=5000")


@contextmanager
def connect(db_file: Path | None = None) -> Iterator[sqlite3.Connection]:
    """Open a connection to the spine DB. Closes on context exit."""
    target = db_file or paths.db_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target, isolation_level=None)  # autocommit; transactions explicit
    conn.row_factory = sqlite3.Row
    try:
        _apply_pragmas(conn)
        yield conn
    finally:
        conn.close()
