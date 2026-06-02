"""Shared pytest fixtures."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture()
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect $HOME (and XDG_*) to a tmp path so tests never touch the real ~/.

    Returns the tmp HOME path. Creates ~/.work_assistant inside it for convenience.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / ".local" / "share"))
    (tmp_path / ".work_assistant").mkdir()
    return tmp_path
