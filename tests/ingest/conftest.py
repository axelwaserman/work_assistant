"""Fixtures shared by ingest tests."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from work_assistant import paths
from work_assistant.db import migrations


@pytest.fixture()
def initialized_db(isolated_home: Path) -> Path:
    """Apply Phase 0 migrations to the spine DB. Returns the spine path."""
    paths.ensure_dirs()
    repo_root = Path(__file__).resolve().parents[2]
    mig_dir = repo_root / "src" / "work_assistant" / "db" / "migrations_sql"
    migrations.apply(mig_dir)
    return paths.db_path()


@pytest.fixture()
def db_conn_factory(initialized_db: Path):
    """Yields a callable that opens a fresh sqlite3 connection per call."""
    def _factory() -> sqlite3.Connection:
        conn = sqlite3.connect(initialized_db, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn
    return _factory


@pytest.fixture()
def fixed_now() -> datetime:
    """A deterministic 'now' for clock-driven tests."""
    return datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)
