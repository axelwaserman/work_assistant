"""Canonical filesystem paths for work-assistant.

All filesystem state lives under `~/.work_assistant/`. Tests redirect $HOME so
calling these functions from tests is safe.
"""

from __future__ import annotations

import os
from pathlib import Path


def root() -> Path:
    """Return the work-assistant data root (`~/.work_assistant/`)."""
    return Path(os.environ["HOME"]) / ".work_assistant"


def config_path() -> Path:
    """Return the path to `config.toml`."""
    return root() / "config.toml"


def db_path() -> Path:
    """Return the path to the SQLite spine database."""
    return root() / "db" / "spine.sqlite"


def logs_dir() -> Path:
    """Return the path to the logs directory."""
    return root() / "logs"


def ensure_dirs() -> None:
    """Create `~/.work_assistant/{db,logs}/` if missing. Safe to call repeatedly."""
    db_path().parent.mkdir(parents=True, exist_ok=True)
    logs_dir().mkdir(parents=True, exist_ok=True)
