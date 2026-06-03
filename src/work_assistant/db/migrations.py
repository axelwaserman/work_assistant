"""SQLite migration runner.

Migrations are SQL files named `NNNN_<slug>.sql`. They run once each, in order
of filename. Applied migrations are recorded in `schema_migrations(version)`.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from work_assistant.db import connection

_NAME_RE = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")
_log = logging.getLogger(__name__)


class MigrationError(Exception):
    """Raised when a migration cannot be loaded or applied."""


def _list_migrations(directory: Path) -> list[Path]:
    files = sorted(directory.glob("*.sql"))
    for f in files:
        if not _NAME_RE.match(f.name):
            raise MigrationError(
                f"migration filename {f.name!r} must start with 4 digits and a slug"
            )
    return files


def _ensure_meta_table(conn) -> None:  # type: ignore[no-untyped-def]
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        " version TEXT PRIMARY KEY,"
        " applied_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))"
        ")"
    )


def apply(directory: Path) -> list[str]:
    """Apply any pending migrations in `directory`. Return the list of versions applied."""
    files = _list_migrations(directory)
    applied: list[str] = []
    with connection.connect() as conn:
        _ensure_meta_table(conn)
        existing = {
            row["version"] for row in conn.execute("SELECT version FROM schema_migrations")
        }
        for path in files:
            version = path.stem
            if version in existing:
                continue
            sql = path.read_text()
            # `executescript` issues an implicit COMMIT before running, so wrap
            # the migration body and the metadata insert inside a single
            # BEGIN/COMMIT block within the script itself for atomicity.
            script = (
                "BEGIN;\n"
                f"{sql}\n"
                f"INSERT INTO schema_migrations(version) VALUES ('{version}');\n"
                "COMMIT;\n"
            )
            try:
                conn.executescript(script)
            except Exception as exc:
                # On failure, executescript leaves no active transaction (it
                # rolls back implicitly), so no explicit ROLLBACK needed.
                raise MigrationError(f"migration {version} failed: {exc}") from exc
            _log.info("applied migration %s", version)
            applied.append(version)
    return applied
