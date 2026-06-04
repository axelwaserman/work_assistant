"""Per-source ingest context. Frozen, never shared, never mutated."""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from work_assistant.ingest.clock import Clock
from work_assistant.mcp.client import MCPClient

if TYPE_CHECKING:
    from work_assistant.config import Config


class DbFactory(ABC):
    """Opens fresh `sqlite3.Connection` instances on demand.

    Each `open()` is a context manager that yields a brand-new connection;
    no cross-task sharing. WAL pragmas applied on every open.
    """

    @abstractmethod
    def open(self) -> Iterator[sqlite3.Connection]: ...


class SqliteDbFactory(DbFactory):
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    @contextmanager
    def open(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            yield conn
        finally:
            conn.close()


@dataclass(frozen=True)
class IngestContext:
    """Constructed by the worker, one per source. Never shared. Never mutated."""

    db: DbFactory
    mcp: MCPClient
    logger: structlog.stdlib.BoundLogger
    settings: Config
    clock: Clock
