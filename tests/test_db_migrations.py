"""Tests for work_assistant.db.migrations."""

from __future__ import annotations

from pathlib import Path

import pytest

from work_assistant import paths
from work_assistant.db import connection, migrations


def _make_migration(tmp: Path, name: str, sql: str) -> Path:
    tmp.mkdir(parents=True, exist_ok=True)
    target = tmp / name
    target.write_text(sql)
    return target


def test_apply_runs_migrations_in_order(isolated_home: Path, tmp_path: Path) -> None:
    paths.ensure_dirs()
    mig_dir = tmp_path / "migs"
    _make_migration(mig_dir, "0001_init.sql", "CREATE TABLE a (id INTEGER);")
    _make_migration(mig_dir, "0002_more.sql", "CREATE TABLE b (id INTEGER);")
    applied = migrations.apply(mig_dir)
    assert applied == ["0001_init", "0002_more"]
    with connection.connect() as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    names = [r["name"] for r in rows]
    assert "a" in names and "b" in names and "schema_migrations" in names


def test_apply_is_idempotent(isolated_home: Path, tmp_path: Path) -> None:
    paths.ensure_dirs()
    mig_dir = tmp_path / "migs"
    _make_migration(mig_dir, "0001_init.sql", "CREATE TABLE a (id INTEGER);")
    first = migrations.apply(mig_dir)
    second = migrations.apply(mig_dir)  # must not raise "table already exists"
    assert first == ["0001_init"]
    assert second == []
    with connection.connect() as conn:
        applied = conn.execute("SELECT version FROM schema_migrations").fetchall()
    assert [r["version"] for r in applied] == ["0001_init"]


def test_apply_rejects_unsorted_filenames(isolated_home: Path, tmp_path: Path) -> None:
    paths.ensure_dirs()
    mig_dir = tmp_path / "migs"
    _make_migration(mig_dir, "init.sql", "CREATE TABLE a (id INTEGER);")
    with pytest.raises(migrations.MigrationError, match="must start with 4 digits"):
        migrations.apply(mig_dir)


def test_apply_fails_on_bad_sql(isolated_home: Path, tmp_path: Path) -> None:
    paths.ensure_dirs()
    mig_dir = tmp_path / "migs"
    _make_migration(mig_dir, "0001_bad.sql", "NOT VALID SQL;")
    with pytest.raises(migrations.MigrationError, match="0001_bad"):
        migrations.apply(mig_dir)


def test_apply_records_only_successful_migrations(isolated_home: Path, tmp_path: Path) -> None:
    paths.ensure_dirs()
    mig_dir = tmp_path / "migs"
    _make_migration(mig_dir, "0001_ok.sql", "CREATE TABLE good (id INTEGER);")
    _make_migration(mig_dir, "0002_bad.sql", "NOT VALID SQL;")
    with pytest.raises(migrations.MigrationError, match="0002_bad"):
        migrations.apply(mig_dir)
    with connection.connect() as conn:
        applied = [
            row["version"]
            for row in conn.execute("SELECT version FROM schema_migrations ORDER BY version")
        ]
        tables = {
            row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert applied == ["0001_ok"]
    assert "good" in tables


def test_phase0_migration_creates_expected_tables(isolated_home: Path) -> None:
    paths.ensure_dirs()
    repo_root = Path(__file__).resolve().parents[1]
    mig_dir = repo_root / "src" / "work_assistant" / "db" / "migrations_sql"
    migrations.apply(mig_dir)
    with connection.connect() as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    names = {r["name"] for r in rows}
    assert {"events", "ingest_cursors", "worker_locks", "schema_migrations"} <= names
    # FTS5 virtual tables show up as 'events_fts' plus internal shadow tables.
    assert any(n.startswith("events_fts") for n in names)


def test_phase0_fts_trigger_round_trip(isolated_home: Path) -> None:
    paths.ensure_dirs()
    repo_root = Path(__file__).resolve().parents[1]
    mig_dir = repo_root / "src" / "work_assistant" / "db" / "migrations_sql"
    migrations.apply(mig_dir)
    with connection.connect() as conn:
        conn.execute(
            "INSERT INTO events(source, source_id, content_hash, occurred_at,"
            " ingested_at, kind, title, body)"
            " VALUES('slack','x','h',1,1,'message','hello','world body')"
        )
        rows = conn.execute(
            "SELECT title FROM events_fts WHERE events_fts MATCH 'world'"
        ).fetchall()
    assert [r["title"] for r in rows] == ["hello"]
