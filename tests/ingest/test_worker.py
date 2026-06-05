"""Top-level worker integration tests."""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.ingest.fakes import FakeClock, FakeMCPClient, StubSource, make_event
from work_assistant.ingest.errors import PermanentIngestError
from work_assistant.ingest.models import Batch, Cursor
from work_assistant.ingest.runner import SourceRunResult
from work_assistant.ingest.worker import (
    EXIT_CONFIG_FATAL,
    EXIT_LOCK_HELD,
    EXIT_OK,
    EXIT_PERMANENT,
    EXIT_TRANSIENT,
    WorkerOptions,
    compute_exit_code,
    run_worker,
)

_MINIMAL_CONFIG = """
[bedrock]
region = "eu-west-1"
aws_profile = "wa"

[bedrock.models]
sonnet = "x"
opus   = "y"
haiku  = "z"

[mcp]
todoist_command   = ["true"]
slack_command     = ["true"]
workspace_command = ["true"]

[ingest]
backfill_days_slack    = 30
backfill_days_gmail    = 90
backfill_days_calendar = 60
"""


@pytest.fixture(autouse=True)
def _config_file(isolated_home: Path) -> None:
    (isolated_home / ".work_assistant" / "config.toml").write_text(
        _MINIMAL_CONFIG, encoding="utf-8"
    )


@pytest.fixture(autouse=True)
def _stub_mcp(monkeypatch: pytest.MonkeyPatch) -> None:
    from work_assistant.ingest import worker as worker_mod

    monkeypatch.setattr(
        worker_mod,
        "_build_mcp_client",
        lambda server, settings: FakeMCPClient(script={}),
    )


def _opts(initialized_db: Path, **overrides) -> WorkerOptions:
    return WorkerOptions(
        registry=overrides.pop("registry", {}),
        sources_enabled=overrides.pop("sources_enabled", []),
        requested_sources=overrides.pop("requested_sources", None),
        dry_run=overrides.pop("dry_run", False),
        since_unix=overrides.pop("since_unix", None),
        clock=overrides.pop("clock", FakeClock(datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC))),
        pid=overrides.pop("pid", os.getpid()),
        run_id=overrides.pop("run_id", "test-run"),
    )


@pytest.mark.asyncio
async def test_run_worker_no_sources_returns_exit_ok(initialized_db: Path) -> None:
    code = await run_worker(_opts(initialized_db))
    assert code == EXIT_OK


@pytest.mark.asyncio
async def test_run_worker_one_ok_source(initialized_db: Path) -> None:
    cls = StubSource.make(
        name="slack",
        mcp_server="slack",
        batches=[Batch(events=[make_event()], next_cursor=Cursor(), status="ok")],
    )
    code = await run_worker(
        _opts(
            initialized_db,
            registry={"slack": cls},
            sources_enabled=["slack"],
        )
    )
    assert code == EXIT_OK
    with sqlite3.connect(initialized_db) as conn:
        n = conn.execute("SELECT count(*) FROM events").fetchone()[0]
    assert n == 1


@pytest.mark.asyncio
async def test_isolation_one_source_failure_does_not_block_sibling(
    initialized_db: Path,
) -> None:
    bad = StubSource.make(
        name="slack",
        mcp_server="slack",
        batches=[Batch(events=[], next_cursor=Cursor(), status="ok")],
        raise_after=1,
        raise_exc=RuntimeError("boom"),
    )
    good = StubSource.make(
        name="todoist",
        mcp_server="todoist",
        batches=[
            Batch(
                events=[make_event(source="todoist", source_id="t1")],
                next_cursor=Cursor(),
                status="ok",
            )
        ],
    )
    code = await run_worker(
        _opts(
            initialized_db,
            registry={"slack": bad, "todoist": good},
            sources_enabled=["slack", "todoist"],
        )
    )
    assert code == EXIT_TRANSIENT
    with sqlite3.connect(initialized_db) as conn:
        n = conn.execute("SELECT count(*) FROM events").fetchone()[0]
    assert n == 1  # the good source's event landed


@pytest.mark.asyncio
async def test_lock_held_returns_exit_3(initialized_db: Path) -> None:
    """A live predecessor PID's row blocks a fresh acquisition → exit 3."""
    clock = FakeClock(datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC))
    # Acquired_at must be within TTL of the worker's clock, otherwise
    # `_reclaim_if_stale` will reclaim the row and the worker would acquire it.
    with sqlite3.connect(initialized_db) as conn:
        conn.execute(
            "INSERT INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
            ("ingest", os.getpid(), clock.now_unix()),
        )
    opts = _opts(
        initialized_db,
        clock=clock,
        pid=os.getpid() + 1,
    )
    code = await run_worker(opts)
    assert code == EXIT_LOCK_HELD


@pytest.mark.asyncio
async def test_unknown_source_returns_exit_2(initialized_db: Path) -> None:
    code = await run_worker(
        _opts(
            initialized_db,
            registry={},
            sources_enabled=[],
            requested_sources=["nope"],
        )
    )
    assert code == 2


@pytest.mark.asyncio
async def test_dry_run_writes_no_events(initialized_db: Path) -> None:
    cls = StubSource.make(
        name="slack",
        mcp_server="slack",
        batches=[Batch(events=[make_event()], next_cursor=Cursor(), status="ok")],
    )
    code = await run_worker(
        _opts(
            initialized_db,
            registry={"slack": cls},
            sources_enabled=["slack"],
            dry_run=True,
        )
    )
    assert code == EXIT_OK
    with sqlite3.connect(initialized_db) as conn:
        n = conn.execute("SELECT count(*) FROM events").fetchone()[0]
    assert n == 0


def test_compute_exit_code_precedence() -> None:
    base = SourceRunResult(name="x", status="ok", inserted=0, ignored=0)
    perm = SourceRunResult(
        name="y", status="error", bucket="permanent", exc=PermanentIngestError("p")
    )
    trans = SourceRunResult(name="z", status="error", bucket="transient", exc=RuntimeError("t"))
    # 4 > 5 > 1 > 3 > 2 > 0
    assert compute_exit_code([base], lock_held=False, config_fatal=False) == EXIT_OK
    assert compute_exit_code([trans], lock_held=False, config_fatal=False) == EXIT_TRANSIENT
    assert compute_exit_code([perm], lock_held=False, config_fatal=False) == EXIT_PERMANENT
    assert compute_exit_code([perm, trans], lock_held=False, config_fatal=False) == EXIT_PERMANENT
    assert compute_exit_code([], lock_held=False, config_fatal=True) == EXIT_CONFIG_FATAL
    assert compute_exit_code([perm], lock_held=False, config_fatal=True) == EXIT_CONFIG_FATAL
    assert compute_exit_code([], lock_held=True, config_fatal=False) == EXIT_LOCK_HELD
