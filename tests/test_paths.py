"""Tests for work_assistant.paths."""

from __future__ import annotations

from pathlib import Path

from work_assistant import paths


def test_root_uses_home_env(isolated_home: Path) -> None:
    assert paths.root() == isolated_home / ".work_assistant"


def test_db_path(isolated_home: Path) -> None:
    assert paths.db_path() == isolated_home / ".work_assistant" / "db" / "spine.sqlite"


def test_logs_dir(isolated_home: Path) -> None:
    assert paths.logs_dir() == isolated_home / ".work_assistant" / "logs"


def test_config_path(isolated_home: Path) -> None:
    assert paths.config_path() == isolated_home / ".work_assistant" / "config.toml"


def test_ensure_dirs_creates_db_and_logs(isolated_home: Path) -> None:
    paths.ensure_dirs()
    assert paths.db_path().parent.is_dir()
    assert paths.logs_dir().is_dir()
