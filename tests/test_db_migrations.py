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


def test_apply_records_only_successful_migrations(
    isolated_home: Path, tmp_path: Path
) -> None:
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
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert applied == ["0001_ok"]
    assert "good" in tables
