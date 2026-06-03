"""Tests for work_assistant.db.connection."""

from __future__ import annotations

from pathlib import Path

from work_assistant import paths
from work_assistant.db import connection


def test_connect_uses_spine_path(isolated_home: Path) -> None:
    paths.ensure_dirs()
    with connection.connect() as conn:
        cur = conn.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
    assert paths.db_path().exists()


def test_connect_sets_pragmas(isolated_home: Path) -> None:
    paths.ensure_dirs()
    with connection.connect() as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1  # NORMAL
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000


def test_connect_returns_rows_as_dicts(isolated_home: Path) -> None:
    paths.ensure_dirs()
    with connection.connect() as conn:
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'a')")
        row = conn.execute("SELECT id, name FROM t").fetchone()
    assert row["id"] == 1
    assert row["name"] == "a"
