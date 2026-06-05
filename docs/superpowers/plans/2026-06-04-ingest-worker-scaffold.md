# Phase 1 — Ingest Worker Scaffold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the worker scaffold that runs `wa ingest` — lock model + heartbeat, the `Source` ABC, the `MCPClient` ABC adapter over `MCPBridge`, `IngestContext`, per-source isolation via `asyncio.gather(return_exceptions=True)`, the cursor/batch transaction model, exit-code precedence, and the `--source / --dry-run / --verbose / --since` CLI. No per-source implementations land in this plan — those come as separate per-source plans (Slack, Gmail, Calendar, Todoist).

**Architecture:** A short-lived `wa ingest` process per invocation. The worker acquires a row in `worker_locks` (`INSERT OR IGNORE` + PID-alive/TTL reclaim), starts a heartbeat task that refreshes `acquired_at`, loads the enabled `Source` instances from a registry, groups them by `mcp_server`, instantiates one `MCPBridge` per group wrapped in a `BridgeMCPClient`, builds an `IngestContext` per source, and runs all sources concurrently via `asyncio.gather(*coros, return_exceptions=True)` where each coro is wrapped in `_run_source_safely` so a single source's failure cannot cancel siblings. Each batch persists in a single SQLite transaction; the cursor advances only when the batch transaction commits. A two-consecutive-zero-insert-batches guard surfaces silent stalls.

**Tech Stack:** Python 3.12, `pydantic` v2 (frozen models, discriminated union on `metadata`), `structlog` for source-bound structured logging, the existing `mcp` Python SDK (already loaded in Phase 0), `click` (existing `wa` CLI), `pytest` + `pytest-asyncio`, `ruff`.

---

## File structure

Files this plan creates or modifies:

```
work_assistant/
├── pyproject.toml                                  (modify: add structlog dep)
├── src/work_assistant/
│   ├── cli.py                                      (modify: register `wa ingest`)
│   ├── config.py                                   (modify: add IngestConfig.sources_enabled)
│   ├── mcp/
│   │   └── client.py                               (NEW: MCPClient ABC + BridgeMCPClient)
│   └── ingest/
│       ├── __init__.py                             (NEW: empty)
│       ├── errors.py                               (NEW: ingest exception hierarchy)
│       ├── models.py                               (NEW: Cursor, NormalizedEvent, Batch, metadata variants)
│       ├── clock.py                                (NEW: Clock ABC, SystemClock)
│       ├── source.py                               (NEW: Source ABC + content_hash helper)
│       ├── context.py                              (NEW: IngestContext, DbFactory ABC, SqliteDbFactory)
│       ├── lock.py                                 (NEW: lock acquire/release/reclaim/heartbeat)
│       ├── logging_bind.py                         (NEW: structlog binding helper)
│       ├── registry.py                             (NEW: SOURCES dict)
│       ├── runner.py                               (NEW: _run_source, _run_source_safely, SourceRunResult, exit-code logic)
│       ├── worker.py                               (NEW: run() top-level — locks, gather, exit code)
│       └── cli.py                                  (NEW: `wa ingest` click subcommand)
└── tests/
    ├── ingest/
    │   ├── __init__.py                             (NEW: empty)
    │   ├── conftest.py                             (NEW: fake_clock, fake_mcp_client, tmp_db, stub_source)
    │   ├── fakes.py                                (NEW: FakeClock, FakeMCPClient, StubSource)
    │   ├── test_models.py                          (NEW)
    │   ├── test_mcp_client.py                      (NEW: BridgeMCPClient + FakeMCPClient)
    │   ├── test_clock.py                           (NEW)
    │   ├── test_source.py                          (NEW: ABC compliance + content_hash)
    │   ├── test_lock.py                            (NEW)
    │   ├── test_runner.py                          (NEW: _run_source / _run_source_safely)
    │   ├── test_worker.py                          (NEW: top-level integration)
    │   └── test_cli.py                             (NEW: `wa ingest` flag handling)
```

Files explicitly **out of scope** for this plan (each ships in its own per-source plan):
- `src/work_assistant/ingest/sources/slack.py` and `tests/ingest/sources/test_slack.py`
- Equivalent files for `gmail`, `calendar`, `todoist`
- Per-source `Cursor` subclasses (e.g. `SlackCursor`)
- Per-source `MCPRequest` / `MCPResponse` pairs
- Real-MCP integration smoke tests

---

## Conventions used throughout this plan

- **Working directory:** `/Users/axel/code/work_assistant`. All paths in this plan are relative to that root.
- **Branch:** the implementation lands on `phase1` (already created).
- **Python:** 3.12 only.
- **Package management:** `uv`. New deps via `uv add`.
- **Test runner:** `uv run pytest`.
- **Lint/format:** `uv run ruff check`, `uv run ruff format`.
- **Repo conventions (`CLAUDE.md`):** ABC always — never `Protocol`. No `Any` in our own signatures (carve-outs only at the third-party seam, contained inside an adapter). Pathlib only with `encoding="utf-8"`. Module-level absolute imports. No re-exports.
- **Schema:** the worker uses tables already shipped in `0001_phase0_core.sql` (`events`, `ingest_cursors`, `worker_locks`). No migration in this plan.
- **Column-name reality check:** the spec text says `cursor_json`; the actual table column is `cursor`. Code uses `cursor` (the schema is what shipped).
- **`structlog.BoundLogger`:** carve-out per `CLAUDE.md` — third-party type allowed in our signatures; must not be replaced with stdlib `logging.Logger`.

---

## Task 0: Add `structlog` dep, create directory skeleton, extend conftest

**Files:**
- Modify: `pyproject.toml`
- Create: `src/work_assistant/ingest/__init__.py`
- Create: `tests/ingest/__init__.py`
- Create: `tests/ingest/conftest.py`

- [ ] **Step 1: Add `structlog` to dependencies**

Edit `pyproject.toml` — under `[project] dependencies`, add `"structlog>=24.0"` so the block reads:

```toml
[project]
name = "work-assistant"
version = "0.0.1"
description = "Personal agentic productivity assistant; Phase 0 foundations."
requires-python = ">=3.12"
readme = "README.md"
dependencies = [
  "click>=8.1",
  "pydantic>=2.7",
  "rich>=13.7",
  "keyring>=25.0",
  "claude-agent-sdk>=0.1.0",
  "mcp>=1.0.0",
  "boto3>=1.34",
  "structlog>=24.0",
]
```

- [ ] **Step 2: Sync deps**

Run:

```bash
uv sync --all-extras
```

Expected: `structlog` resolves and lock file updates. No other version churn.

- [ ] **Step 3: Create empty package init files**

Create `src/work_assistant/ingest/__init__.py`:

```python
"""Ingest worker scaffold.

This package owns the worker process (`wa ingest`), the `Source` ABC, the
`MCPClient` adapter, `IngestContext`, lock model, and exit-code logic. Per-
source implementations live under their own modules and are wired into
`registry.SOURCES`.
"""
```

Create `tests/ingest/__init__.py`:

```python
```

- [ ] **Step 4: Write `tests/ingest/conftest.py`**

Create `tests/ingest/conftest.py`:

```python
"""Fixtures shared by ingest tests."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from work_assistant import paths
from work_assistant.db import connection, migrations


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
```

- [ ] **Step 5: Verify pytest still discovers everything**

Run:

```bash
uv run pytest -q
```

Expected: existing Phase 0 tests pass; no new test files yet.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/work_assistant/ingest/__init__.py tests/ingest/__init__.py tests/ingest/conftest.py
git commit -m "chore: scaffold ingest package + structlog dep"
```

---

## Task 1: Ingest models — `Cursor`, metadata variants, `NormalizedEvent`, `Batch`

The data shapes the worker passes between `Source.fetch()` and the SQLite write path. All frozen pydantic. No `Any`. The four metadata variants (Slack, Gmail, Calendar, Todoist) are tagged with `Literal["..."]` discriminators so `EventMetadata` is a discriminated union.

**Files:**
- Create: `src/work_assistant/ingest/models.py`
- Test: `tests/ingest/test_models.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/test_models.py`:

```python
"""Tests for work_assistant.ingest.models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from work_assistant.ingest import models


def test_cursor_is_frozen() -> None:
    cur = models.Cursor()
    with pytest.raises(ValidationError):
        cur.__dict__["x"] = 1  # frozen models reject any attribute set
        models.Cursor.model_validate(cur.model_dump())  # safe parse still works


def test_slack_metadata_round_trips_through_union() -> None:
    md = models.SlackMetadata(
        channel_id="C1",
        channel_name="general",
        is_im=False,
        is_mpim=False,
        is_dm=False,
        is_mention=True,
        reactions_json="[]",
        files_json="[]",
    )
    payload = md.model_dump_json()
    parsed = models.SlackMetadata.model_validate_json(payload)
    assert parsed == md


def test_gmail_metadata_required_fields() -> None:
    with pytest.raises(ValidationError):
        models.GmailMetadata(  # type: ignore[call-arg]
            to=["a@b"], cc=[], labels=[], has_attachments=False,
            # internal_date missing
        )


def test_normalized_event_minimum_construction() -> None:
    md = models.SlackMetadata(
        channel_id="C1", channel_name="g", is_im=False, is_mpim=False,
        is_dm=False, is_mention=False, reactions_json="[]", files_json="[]",
    )
    ev = models.NormalizedEvent(
        source="slack",
        source_id="m1",
        source_link=None,
        content_hash="0" * 64,
        occurred_at=1_700_000_000,
        actor=None,
        thread_key=None,
        kind="message",
        title=None,
        body="hello",
        body_truncated=False,
        metadata=md,
    )
    assert ev.source == "slack"
    assert ev.metadata.kind == "slack"


def test_batch_holds_events_and_status() -> None:
    cursor = models.Cursor()
    batch = models.Batch(events=[], next_cursor=cursor, status="ok")
    assert batch.status == "ok"
    assert batch.events == []
    assert batch.next_cursor is cursor


def test_metadata_union_resolves_by_discriminator() -> None:
    raw = {
        "kind": "calendar",
        "start": "2026-06-04T10:00:00Z",
        "end": "2026-06-04T11:00:00Z",
        "attendees_json": "[]",
        "hangout_link": None,
        "attachments_json": "[]",
        "is_organizer": True,
    }
    parsed = models.parse_event_metadata(raw)
    assert isinstance(parsed, models.CalendarMetadata)
    assert parsed.is_organizer is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_models.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.models'`.

- [ ] **Step 3: Implement `models.py`**

Create `src/work_assistant/ingest/models.py`:

```python
"""Source-facing data shapes shared by the worker and every `Source` impl.

Every model is frozen. `NormalizedEvent.metadata` is a discriminated union
across the four supported sources; per-source code constructs the variant that
matches its `Source.name`.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


class Cursor(BaseModel):
    """Source-specific cursor state. Each source subclasses this with its own
    fields (e.g. `SlackCursor.channels: dict[str, str]`). The base is empty so
    a default cursor (e.g. for `--since`) can still be serialized."""

    model_config = ConfigDict(frozen=True)


class SlackMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["slack"] = "slack"
    channel_id: str
    channel_name: str
    is_im: bool
    is_mpim: bool
    is_dm: bool
    is_mention: bool
    reactions_json: str
    files_json: str


class GmailMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["gmail"] = "gmail"
    to: list[str]
    cc: list[str]
    labels: list[str]
    has_attachments: bool
    internal_date: int


class CalendarMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["calendar"] = "calendar"
    start: str
    end: str
    attendees_json: str
    hangout_link: str | None
    attachments_json: str
    is_organizer: bool


class TodoistMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["todoist"] = "todoist"
    project_id: str
    project_path: str
    labels: list[str]
    due_json: str | None
    priority: int
    completed: bool
    parent_id: str | None
    ws_footer_json: str | None


EventMetadata = Annotated[
    SlackMetadata | GmailMetadata | CalendarMetadata | TodoistMetadata,
    Field(discriminator="kind"),
]


_metadata_adapter: TypeAdapter[
    SlackMetadata | GmailMetadata | CalendarMetadata | TodoistMetadata
] = TypeAdapter(EventMetadata)


def parse_event_metadata(
    payload: dict[str, object],
) -> SlackMetadata | GmailMetadata | CalendarMetadata | TodoistMetadata:
    """Parse a metadata dict into the right variant via the `kind` discriminator."""
    return _metadata_adapter.validate_python(payload)


SourceName = Literal["slack", "gmail", "calendar", "todoist"]
EventKind = Literal["message", "email", "meeting", "doc", "task_state"]


class NormalizedEvent(BaseModel):
    """Aligned with the `events` table in `docs/02-data-model.md`.

    Fields map 1:1 to columns except:
    - `body_truncated` is NOT a column. The worker logs it; sources should
      truncate before constructing the event.
    - `metadata` becomes `metadata_json` at write time via `model_dump_json()`.
    """

    model_config = ConfigDict(frozen=True)

    source: SourceName
    source_id: str
    source_link: str | None
    content_hash: str
    occurred_at: int
    actor: str | None
    thread_key: str | None
    kind: EventKind
    title: str | None
    body: str | None
    body_truncated: bool
    metadata: EventMetadata


class Batch(BaseModel):
    """A unit of fetch progress. The worker wraps each `Batch` in a single
    SQLite transaction; the cursor advances on commit."""

    model_config = ConfigDict(frozen=True)

    events: list[NormalizedEvent]
    next_cursor: Cursor
    status: Literal["ok", "partial"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_models.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/models.py tests/ingest/test_models.py
git commit -m "feat: add ingest models — Cursor, NormalizedEvent, Batch, metadata union"
```

---

## Task 2: Ingest exception hierarchy

Sources signal classifiable failures by raising one of these. The worker maps each class to an exit-code bucket per `§6.2`. Keeping these in a dedicated module avoids circular imports between `runner.py` and per-source code.

**Files:**
- Create: `src/work_assistant/ingest/errors.py`
- Test: `tests/ingest/test_errors.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/test_errors.py`:

```python
"""Tests for work_assistant.ingest.errors."""

from __future__ import annotations

from work_assistant.ingest import errors


def test_stall_is_transient() -> None:
    err = errors.SourceStallError("two zero-insert batches")
    assert isinstance(err, errors.TransientIngestError)
    assert isinstance(err, errors.IngestError)


def test_permanent_distinct_from_transient() -> None:
    perm = errors.PermanentIngestError("auth revoked")
    assert isinstance(perm, errors.IngestError)
    assert not isinstance(perm, errors.TransientIngestError)


def test_mcp_timeout_is_transient() -> None:
    err = errors.MCPTimeoutError("call timed out after 60s")
    assert isinstance(err, errors.TransientIngestError)


def test_classify_unknown_exception_is_transient() -> None:
    bucket = errors.classify(RuntimeError("oops"))
    assert bucket == "transient"


def test_classify_permanent() -> None:
    bucket = errors.classify(errors.PermanentIngestError("nope"))
    assert bucket == "permanent"


def test_classify_stall_is_transient() -> None:
    bucket = errors.classify(errors.SourceStallError("stall"))
    assert bucket == "transient"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_errors.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.errors'`.

- [ ] **Step 3: Implement `errors.py`**

Create `src/work_assistant/ingest/errors.py`:

```python
"""Exception hierarchy for the ingest worker.

The worker catches anything that bubbles out of a `Source` and classifies it
into a transient or permanent bucket via `classify()`. The bucket drives the
worker's exit code per spec §6.2.
"""

from __future__ import annotations

from typing import Literal

ErrorBucket = Literal["transient", "permanent"]


class IngestError(Exception):
    """Base class for ingest worker failures we classify."""


class TransientIngestError(IngestError):
    """Retryable next tick: network 5xx, rate-limit, busy db, MCP timeout, stall."""


class PermanentIngestError(IngestError):
    """Not retryable without operator action: auth revoked, account disabled,
    schema-incompatible payload."""


class SourceStallError(TransientIngestError):
    """Two consecutive zero-insert batches with non-empty input. Almost always
    a pagination or dedup-key bug. Surfaces as exit 1."""


class MCPTimeoutError(TransientIngestError):
    """An MCP tool call exceeded `MCP_CALL_TIMEOUT_S` seconds."""


def classify(exc: BaseException) -> ErrorBucket:
    """Map an exception to its exit-code bucket. Unknown errors are transient."""
    if isinstance(exc, PermanentIngestError):
        return "permanent"
    return "transient"
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_errors.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/errors.py tests/ingest/test_errors.py
git commit -m "feat: add ingest error hierarchy with transient/permanent classifier"
```

---

## Task 3: `Clock` ABC + `SystemClock`, plus `FakeClock` fake

`Clock` is an ABC per repo convention. `now()` returns a UTC-aware `datetime`. The worker also needs a unix-seconds helper for the lock SQL (`acquired_at INTEGER`).

**Files:**
- Create: `src/work_assistant/ingest/clock.py`
- Create: `tests/ingest/fakes.py` (initial — `FakeClock` only; later tasks append)
- Test: `tests/ingest/test_clock.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/test_clock.py`:

```python
"""Tests for work_assistant.ingest.clock."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from tests.ingest.fakes import FakeClock
from work_assistant.ingest.clock import SystemClock


def test_system_clock_returns_utc_now() -> None:
    clock = SystemClock()
    before = datetime.now(UTC)
    got = clock.now()
    after = datetime.now(UTC)
    assert before <= got <= after
    assert got.tzinfo is UTC


def test_system_clock_now_unix() -> None:
    clock = SystemClock()
    delta = abs(clock.now_unix() - int(datetime.now(UTC).timestamp()))
    assert delta <= 2


def test_fake_clock_does_not_advance_on_its_own() -> None:
    start = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)
    clock = FakeClock(start)
    assert clock.now() == start
    assert clock.now() == start
    assert clock.now_unix() == int(start.timestamp())


def test_fake_clock_advance() -> None:
    start = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)
    clock = FakeClock(start)
    clock.advance(seconds=90)
    assert clock.now() == start + timedelta(seconds=90)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_clock.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.clock'`.

- [ ] **Step 3: Implement `clock.py`**

Create `src/work_assistant/ingest/clock.py`:

```python
"""Clock abstraction so tests can drive lock-TTL and heartbeat code paths
deterministically. Repo convention: ABC, not Protocol."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime


class Clock(ABC):
    @abstractmethod
    def now(self) -> datetime:
        """Return the current time as a UTC-aware `datetime`."""

    def now_unix(self) -> int:
        """Convenience: unix seconds. Default impl uses `now()`."""
        return int(self.now().timestamp())


class SystemClock(Clock):
    def now(self) -> datetime:
        return datetime.now(UTC)
```

- [ ] **Step 4: Implement the `FakeClock` fake**

Create `tests/ingest/fakes.py`:

```python
"""Test fakes used by ingest tests. Lives under `tests/` because nothing in
`src/` should depend on a fake."""

from __future__ import annotations

from datetime import datetime, timedelta

from work_assistant.ingest.clock import Clock


class FakeClock(Clock):
    """A `Clock` that only moves when `advance()` is called."""

    def __init__(self, initial: datetime) -> None:
        self._now = initial

    def now(self) -> datetime:
        return self._now

    def advance(self, *, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_clock.py -v
```

Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add src/work_assistant/ingest/clock.py tests/ingest/fakes.py tests/ingest/test_clock.py
git commit -m "feat: add Clock ABC, SystemClock, FakeClock"
```

---

## Task 4: `MCPClient` ABC, `MCPRequest`/`MCPResponse` bases, `BridgeMCPClient`, `FakeMCPClient`

The source-facing adapter that hides `MCPBridge` and `CallToolResult`. `Any` is contained inside `BridgeMCPClient`. Tests use `FakeMCPClient` (extends `tests/ingest/fakes.py`).

**Files:**
- Create: `src/work_assistant/mcp/client.py`
- Modify: `tests/ingest/fakes.py` (append `FakeMCPClient`)
- Test: `tests/ingest/test_mcp_client.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/test_mcp_client.py`:

```python
"""Tests for the MCPClient ABC, BridgeMCPClient, and FakeMCPClient."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import ClassVar

import pytest

from work_assistant import paths
from work_assistant.ingest.errors import MCPTimeoutError
from work_assistant.mcp.bridge import MCPBridge
from work_assistant.mcp.client import (
    BridgeMCPClient,
    MCPRequest,
    MCPResponse,
)
from tests.ingest.fakes import FakeMCPClient, FakeMCPClientError, ScriptedReply


class _PingRequest(MCPRequest):
    tool_name: ClassVar[str] = "ping"


class _PingResponse(MCPResponse):
    text: str


class _OtherResponse(MCPResponse):
    other: str


SERVER_SCRIPT = """\
from mcp.server.fastmcp import FastMCP
mcp = FastMCP("mock")

@mcp.tool()
def ping() -> str:
    return "pong"

@mcp.tool()
def echo(text: str) -> str:
    return text

if __name__ == "__main__":
    mcp.run(transport="stdio")
"""


@pytest.fixture()
def mock_server_path(tmp_path: Path) -> Path:
    p = tmp_path / "mock_mcp_server.py"
    p.write_text(SERVER_SCRIPT, encoding="utf-8")
    return p


@pytest.mark.asyncio
async def test_bridge_client_parses_text_response(
    isolated_home: Path, mock_server_path: Path
) -> None:
    paths.ensure_dirs()
    async with MCPBridge(name="mock", command=[sys.executable, str(mock_server_path)]) as br:
        client = BridgeMCPClient(br)
        resp = await client.call(_PingRequest(), _PingResponse)
        assert resp.text == "pong"


@pytest.mark.asyncio
async def test_bridge_client_raises_mcp_timeout(
    isolated_home: Path, mock_server_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the bridge call exceeds the timeout, BridgeMCPClient raises MCPTimeoutError."""
    paths.ensure_dirs()
    async with MCPBridge(name="mock", command=[sys.executable, str(mock_server_path)]) as br:
        client = BridgeMCPClient(br, timeout_s=0.01)

        async def slow_call(*a: object, **kw: object) -> object:
            await asyncio.sleep(1.0)
            raise AssertionError("should have timed out")

        monkeypatch.setattr(br, "call_tool", slow_call)
        with pytest.raises(MCPTimeoutError):
            await client.call(_PingRequest(), _PingResponse)


@pytest.mark.asyncio
async def test_fake_returns_scripted_response() -> None:
    fake = FakeMCPClient(
        script={"_PingRequest": [ScriptedReply(response=_PingResponse(text="pong"))]}
    )
    resp = await fake.call(_PingRequest(), _PingResponse)
    assert resp.text == "pong"
    assert len(fake.calls) == 1


@pytest.mark.asyncio
async def test_fake_raises_on_unscripted_call() -> None:
    fake = FakeMCPClient(script={})
    with pytest.raises(FakeMCPClientError, match="unexpected call"):
        await fake.call(_PingRequest(), _PingResponse)


@pytest.mark.asyncio
async def test_fake_raises_on_scripted_failure() -> None:
    fake = FakeMCPClient(
        script={"_PingRequest": [ScriptedReply(raises=RuntimeError("boom"))]}
    )
    with pytest.raises(RuntimeError, match="boom"):
        await fake.call(_PingRequest(), _PingResponse)


@pytest.mark.asyncio
async def test_fake_rejects_response_type_mismatch() -> None:
    fake = FakeMCPClient(
        script={"_PingRequest": [ScriptedReply(response=_OtherResponse(other="x"))]}
    )
    with pytest.raises(FakeMCPClientError, match="expected _PingResponse"):
        await fake.call(_PingRequest(), _PingResponse)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_mcp_client.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.mcp.client'`.

- [ ] **Step 3: Implement `mcp/client.py`**

Create `src/work_assistant/mcp/client.py`:

```python
"""Source-facing MCP adapter.

`MCPBridge` (`src/work_assistant/mcp/bridge.py`) returns the MCP SDK's
`CallToolResult` with `Any` payloads. `MCPClient` is the ABC every source
calls; `BridgeMCPClient` is the production impl that wraps `MCPBridge`,
parses the response, and contains all `Any` at the seam.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any, ClassVar, TypeVar

from pydantic import BaseModel, ConfigDict

from work_assistant.ingest.errors import MCPTimeoutError
from work_assistant.mcp.bridge import MCPBridge

MCP_CALL_TIMEOUT_S_DEFAULT = 60.0


class MCPRequest(BaseModel):
    """Per-tool typed request. Subclass per (server, tool).

    Subclasses MUST set `tool_name: ClassVar[str]` to the MCP tool's name.
    Argument fields go on the subclass; `model_dump()` becomes the arguments
    dict passed to `MCPBridge.call_tool`.
    """

    model_config = ConfigDict(frozen=True)

    tool_name: ClassVar[str] = ""


class MCPResponse(BaseModel):
    """Per-tool typed response. Subclass per (server, tool)."""

    model_config = ConfigDict(frozen=True)


_RespT = TypeVar("_RespT", bound=MCPResponse)


class MCPClient(ABC):
    @abstractmethod
    async def call(
        self,
        request: MCPRequest,
        response_model: type[_RespT],
    ) -> _RespT:
        """Dispatch a tool call and parse the result into `response_model`."""


class BridgeMCPClient(MCPClient):
    """Production adapter: wraps `MCPBridge`. The only place `Any` lives."""

    def __init__(
        self,
        bridge: MCPBridge,
        timeout_s: float = MCP_CALL_TIMEOUT_S_DEFAULT,
    ) -> None:
        self._bridge = bridge
        self._timeout_s = timeout_s

    async def call(
        self,
        request: MCPRequest,
        response_model: type[_RespT],
    ) -> _RespT:
        tool_name = type(request).tool_name
        if not tool_name:
            raise ValueError(
                f"{type(request).__name__} missing `tool_name: ClassVar[str]`"
            )
        arguments: dict[str, Any] = request.model_dump()
        try:
            result = await asyncio.wait_for(
                self._bridge.call_tool(tool_name, arguments),
                timeout=self._timeout_s,
            )
        except asyncio.TimeoutError as exc:
            raise MCPTimeoutError(
                f"MCP call {tool_name!r} timed out after {self._timeout_s}s"
            ) from exc
        return self._parse(result, response_model)

    @staticmethod
    def _parse(result: Any, response_model: type[_RespT]) -> _RespT:
        """Pull the first text block out of `CallToolResult` and validate it.

        Most MCP tools return a single text content item with a JSON payload.
        For tools returning a bare string (e.g. `ping -> "pong"`), we wrap it
        as `{"text": "<value>"}` so a one-field `MCPResponse` parses cleanly.
        """
        content = result.content
        if not content:
            raise ValueError(
                f"MCP response had no content; cannot parse into {response_model.__name__}"
            )
        first = content[0]
        text: str | None = getattr(first, "text", None)
        if text is None:
            raise ValueError(
                f"MCP response first content item has no `.text`; got {type(first).__name__}"
            )
        stripped = text.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            return response_model.model_validate_json(text)
        return response_model.model_validate({"text": text})
```

- [ ] **Step 4: Append `FakeMCPClient` to `tests/ingest/fakes.py`**

Edit `tests/ingest/fakes.py` — append after the existing `FakeClock` class:

```python


from dataclasses import dataclass, field
from typing import TypeVar

from work_assistant.mcp.client import MCPClient, MCPRequest, MCPResponse


_RespT = TypeVar("_RespT", bound=MCPResponse)


@dataclass(frozen=True)
class ScriptedReply:
    response: MCPResponse | None = None
    raises: BaseException | None = None


@dataclass(frozen=True)
class RecordedCall:
    request: MCPRequest


class FakeMCPClientError(RuntimeError):
    """Raised by FakeMCPClient on misuse (under-scripted call, type mismatch)."""


@dataclass
class FakeMCPClient(MCPClient):
    """In-memory MCPClient driven by a script keyed by request class name.

    Use the `MCPRequest` subclass `__name__` as the key so error messages name
    the offending call site clearly.
    """

    script: dict[str, list[ScriptedReply]] = field(default_factory=dict)
    calls: list[RecordedCall] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.script = {k: list(v) for k, v in self.script.items()}

    async def call(
        self,
        request: MCPRequest,
        response_model: type[_RespT],
    ) -> _RespT:
        self.calls.append(RecordedCall(request=request))
        key = type(request).__name__
        if key not in self.script or not self.script[key]:
            raise FakeMCPClientError(
                f"FakeMCPClient: unexpected call to {key} with "
                f"args={request.model_dump()!r}. "
                f"Scripted keys: {list(self.script)!r}"
            )
        scripted = self.script[key].pop(0)
        if scripted.raises is not None:
            raise scripted.raises
        if not isinstance(scripted.response, response_model):
            actual = type(scripted.response).__name__ if scripted.response is not None else "None"
            raise FakeMCPClientError(
                f"FakeMCPClient: scripted response for {key} is "
                f"{actual}, expected {response_model.__name__}"
            )
        return scripted.response
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_mcp_client.py -v
```

Expected: 6 passed.

- [ ] **Step 6: Commit**

```bash
git add src/work_assistant/mcp/client.py tests/ingest/fakes.py tests/ingest/test_mcp_client.py
git commit -m "feat: add MCPClient ABC adapter, BridgeMCPClient, FakeMCPClient"
```

---

## Task 5: `Source` ABC + concrete `compute_content_hash`

The contract every source implements. Per repo convention: ABC. Per spec §2: per-source helpers that genuinely differ are abstract; only the canonical content-hash function is concrete.

**Files:**
- Create: `src/work_assistant/ingest/source.py`
- Test: `tests/ingest/test_source.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/test_source.py`:

```python
"""Tests for the Source ABC and its concrete content-hash helper."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator

import pytest

from work_assistant.ingest.models import Batch, Cursor
from work_assistant.ingest.source import Source


def test_cannot_instantiate_abstract_source() -> None:
    with pytest.raises(TypeError):
        Source(ctx=None)  # type: ignore[abstract]


def test_subclass_missing_abstract_methods_cannot_instantiate() -> None:
    class Half(Source):
        name = "slack"
        mcp_server = "slack"

        async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
            if False:
                yield  # pragma: no cover

    with pytest.raises(TypeError):
        Half(ctx=None)  # type: ignore[abstract]


def test_complete_subclass_can_instantiate_and_hash() -> None:
    class Done(Source):
        name = "slack"
        mcp_server = "slack"

        async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
            if False:
                yield  # pragma: no cover

        def normalize_body(self, raw: str) -> tuple[str, bool]:
            return raw, False

        async def resolve_actor(self, raw_actor: str) -> str | None:
            return raw_actor

        def cursor_from_timestamp(self, ts: int) -> Cursor:
            return Cursor()

    src = Done(ctx=None)  # type: ignore[arg-type]
    got = src.compute_content_hash("m1", "hello")
    expected = hashlib.sha256(b"slack:m1:hello").hexdigest()
    assert got == expected


def test_content_hash_handles_none_body() -> None:
    class Done(Source):
        name = "gmail"
        mcp_server = "workspace"

        async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
            if False:
                yield  # pragma: no cover

        def normalize_body(self, raw: str) -> tuple[str, bool]:
            return raw, False

        async def resolve_actor(self, raw_actor: str) -> str | None:
            return None

        def cursor_from_timestamp(self, ts: int) -> Cursor:
            return Cursor()

    src = Done(ctx=None)  # type: ignore[arg-type]
    got = src.compute_content_hash("e1", None)
    expected = hashlib.sha256(b"gmail:e1:").hexdigest()
    assert got == expected
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_source.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.source'`.

- [ ] **Step 3: Implement `source.py`**

Create `src/work_assistant/ingest/source.py`:

```python
"""The `Source` ABC every per-source implementation must satisfy.

Per repo convention (`CLAUDE.md`) this is an ABC, not a Protocol. ABC gives
us runtime validation at instantiation, reliable `isinstance()` checks, and
no structural-typing surprises when sources are registered dynamically.
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, ClassVar

from work_assistant.ingest.models import Batch, Cursor

if TYPE_CHECKING:
    from work_assistant.ingest.context import IngestContext


class Source(ABC):
    """Per-source ingest contract.

    `name` and `mcp_server` are class attributes set by each concrete source.
    The worker reads them to build the registry and group sources by bridge.
    """

    name: ClassVar[str]
    mcp_server: ClassVar[str]

    def __init__(self, ctx: "IngestContext") -> None:
        self.ctx = ctx

    @abstractmethod
    def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
        """Async generator. Each yielded `Batch` is wrapped by the worker in
        a single SQLite transaction; the cursor advances on commit only.

        Implementations: `async def fetch(...) -> AsyncIterator[Batch]: yield ...`.
        """

    @abstractmethod
    def normalize_body(self, raw: str) -> tuple[str, bool]:
        """Strip / decode HTML / truncate to ~100 KB. Returns `(body, truncated)`."""

    @abstractmethod
    async def resolve_actor(self, raw_actor: str) -> str | None:
        """Map provider id to email when possible; else best-effort id; else None."""

    @abstractmethod
    def cursor_from_timestamp(self, ts: int) -> Cursor:
        """Synthesize a one-shot cursor for `wa ingest --since`. Per-source spec
        defines the mapping to its native cursor shape."""

    def compute_content_hash(self, source_id: str, body: str | None) -> str:
        """Concrete. `sha256(name + ':' + source_id + ':' + (body or ''))`."""
        payload = f"{self.name}:{source_id}:{body or ''}".encode()
        return hashlib.sha256(payload).hexdigest()
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_source.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/source.py tests/ingest/test_source.py
git commit -m "feat: add Source ABC with concrete content_hash helper"
```

---

## Task 6: `IngestContext`, `DbFactory` ABC + `SqliteDbFactory`, settings extension

Per-source frozen context. Built once per source per run, never shared, never mutated. `DbFactory.open()` yields a fresh connection per call.

We also extend `IngestConfig` with `sources_enabled: list[str]` so the worker knows which sources to run (override per-flag with `--source`).

**Files:**
- Create: `src/work_assistant/ingest/context.py`
- Modify: `src/work_assistant/config.py` (add `sources_enabled`)
- Test: `tests/ingest/test_context.py`
- Test: `tests/test_config.py` (extend with the new field)

- [ ] **Step 1: Write the failing tests for context**

Create `tests/ingest/test_context.py`:

```python
"""Tests for IngestContext and SqliteDbFactory."""

from __future__ import annotations

from pathlib import Path

import pytest

from work_assistant.ingest.context import IngestContext, SqliteDbFactory


def test_sqlite_factory_opens_fresh_connection_per_call(initialized_db: Path) -> None:
    factory = SqliteDbFactory(db_path=initialized_db)
    with factory.open() as conn1:
        with factory.open() as conn2:
            assert conn1 is not conn2
            conn1.execute("INSERT INTO worker_locks(name, pid, acquired_at) VALUES ('a', 1, 1)")
            row = conn2.execute(
                "SELECT pid FROM worker_locks WHERE name='a'"
            ).fetchone()
            assert row["pid"] == 1


def test_sqlite_factory_applies_pragmas(initialized_db: Path) -> None:
    factory = SqliteDbFactory(db_path=initialized_db)
    with factory.open() as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_ingest_context_is_frozen(initialized_db: Path) -> None:
    """frozen=True dataclass: assignment raises FrozenInstanceError."""
    from datetime import UTC, datetime

    from tests.ingest.fakes import FakeClock, FakeMCPClient

    factory = SqliteDbFactory(db_path=initialized_db)
    fake_logger_calls: list[tuple[str, dict[str, object]]] = []

    class _StubLogger:
        def info(self, event: str, **kw: object) -> None:
            fake_logger_calls.append((event, dict(kw)))
        warning = info
        error = info
        debug = info

    ctx = IngestContext(
        db=factory,
        mcp=FakeMCPClient(),
        logger=_StubLogger(),  # type: ignore[arg-type]
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 4, 0, 0, 0, tzinfo=UTC)),
    )
    with pytest.raises(Exception):
        ctx.db = factory  # type: ignore[misc]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_context.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.context'`.

- [ ] **Step 3: Implement `context.py`**

Create `src/work_assistant/ingest/context.py`:

```python
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
    def open(self) -> "Iterator[sqlite3.Connection]": ...


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
    settings: "Config"
    clock: Clock
```

- [ ] **Step 4: Extend `IngestConfig` in `config.py`**

Modify `src/work_assistant/config.py`. Replace the `IngestConfig` class definition (only that class) with:

```python
class IngestConfig(BaseModel):
    backfill_days_slack: int
    backfill_days_gmail: int
    backfill_days_calendar: int
    sources_enabled: list[str] = []
```

- [ ] **Step 5: Add a test for the new field**

Append to `tests/test_config.py`:

```python


def test_load_supports_sources_enabled(isolated_home: Path) -> None:
    payload = """
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
backfill_days_slack    = 1
backfill_days_gmail    = 1
backfill_days_calendar = 1
sources_enabled = ["slack", "todoist"]
"""
    (isolated_home / ".work_assistant" / "config.toml").write_text(payload, encoding="utf-8")
    cfg = config.load()
    assert cfg.ingest.sources_enabled == ["slack", "todoist"]


def test_load_defaults_sources_enabled_to_empty(isolated_home: Path) -> None:
    payload = """
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
backfill_days_slack    = 1
backfill_days_gmail    = 1
backfill_days_calendar = 1
"""
    (isolated_home / ".work_assistant" / "config.toml").write_text(payload, encoding="utf-8")
    cfg = config.load()
    assert cfg.ingest.sources_enabled == []
```

- [ ] **Step 6: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_context.py tests/test_config.py -v
```

Expected: 3 new context tests pass; both new config tests pass; existing config tests still pass.

- [ ] **Step 7: Commit**

```bash
git add src/work_assistant/ingest/context.py src/work_assistant/config.py tests/ingest/test_context.py tests/test_config.py
git commit -m "feat: add IngestContext, DbFactory ABC, SqliteDbFactory, sources_enabled"
```

---

## Task 7: Worker lock — `acquire_lock`, reclaim policy, `release_lock`, `Heartbeat`

The whole §3.1 lock model: `INSERT OR IGNORE`, PID-alive check via `os.kill(pid, 0)`, TTL-based reclaim, and a heartbeat task that refreshes `acquired_at` every 60s. `LockError` (subclass of `IngestError`) signals "lock held by live worker → exit 3".

**Files:**
- Create: `src/work_assistant/ingest/lock.py`
- Modify: `src/work_assistant/ingest/errors.py` (add `LockHeldError`)
- Test: `tests/ingest/test_lock.py`

- [ ] **Step 1: Add `LockHeldError` to `errors.py`**

Edit `src/work_assistant/ingest/errors.py` — append:

```python


class LockHeldError(IngestError):
    """`worker_locks` row held by a still-alive predecessor; we exit clean (code 3)."""
```

- [ ] **Step 2: Write the failing tests**

Create `tests/ingest/test_lock.py`:

```python
"""Tests for work_assistant.ingest.lock."""

from __future__ import annotations

import asyncio
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from work_assistant.ingest.errors import LockHeldError
from work_assistant.ingest.lock import (
    LOCK_NAME,
    LOCK_TTL_SECONDS,
    Heartbeat,
    acquire_lock,
    release_lock,
)
from tests.ingest.fakes import FakeClock


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock(datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC))


def _read_lock_row(conn: sqlite3.Connection) -> tuple[int, int] | None:
    row = conn.execute(
        "SELECT pid, acquired_at FROM worker_locks WHERE name = ?", (LOCK_NAME,)
    ).fetchone()
    return (row["pid"], row["acquired_at"]) if row else None


def test_acquire_first_run_inserts_row(db_conn_factory, clock: FakeClock) -> None:
    with db_conn_factory() as conn:
        acquire_lock(conn, pid=os.getpid(), clock=clock)
        row = _read_lock_row(conn)
    assert row == (os.getpid(), clock.now_unix())


def test_acquire_raises_when_live_predecessor_holds(
    db_conn_factory, clock: FakeClock
) -> None:
    with db_conn_factory() as conn:
        conn.execute(
            "INSERT INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
            (LOCK_NAME, os.getpid(), clock.now_unix()),
        )
        with pytest.raises(LockHeldError):
            acquire_lock(conn, pid=os.getpid() + 1, clock=clock)


def test_acquire_reclaims_dead_predecessor(db_conn_factory, clock: FakeClock) -> None:
    """A row pointing at a PID with no live process is reclaimed."""
    dead_pid = 99999
    with db_conn_factory() as conn:
        conn.execute(
            "INSERT INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
            (LOCK_NAME, dead_pid, clock.now_unix() - 10),
        )
        # PID-alive check must say `dead_pid` is gone:
        # ProcessLookupError from os.kill(dead_pid, 0).
        acquire_lock(conn, pid=os.getpid(), clock=clock)
        row = _read_lock_row(conn)
    assert row == (os.getpid(), clock.now_unix())


def test_acquire_reclaims_ttl_expired_even_if_pid_live(
    db_conn_factory, clock: FakeClock
) -> None:
    """Even if the predecessor's PID is alive, an expired acquired_at is reclaimable."""
    with db_conn_factory() as conn:
        conn.execute(
            "INSERT INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
            (LOCK_NAME, os.getpid(), clock.now_unix() - LOCK_TTL_SECONDS - 1),
        )
        acquire_lock(conn, pid=os.getpid(), clock=clock)
        row = _read_lock_row(conn)
    assert row == (os.getpid(), clock.now_unix())


def test_release_only_removes_own_row(db_conn_factory, clock: FakeClock) -> None:
    with db_conn_factory() as conn:
        acquire_lock(conn, pid=os.getpid(), clock=clock)
        release_lock(conn, pid=os.getpid())
        assert _read_lock_row(conn) is None


def test_release_no_op_if_pid_mismatch(db_conn_factory, clock: FakeClock) -> None:
    with db_conn_factory() as conn:
        acquire_lock(conn, pid=os.getpid(), clock=clock)
        release_lock(conn, pid=os.getpid() + 1)
        assert _read_lock_row(conn) is not None


@pytest.mark.asyncio
async def test_heartbeat_refreshes_acquired_at(
    db_conn_factory, clock: FakeClock
) -> None:
    with db_conn_factory() as conn:
        acquire_lock(conn, pid=os.getpid(), clock=clock)
    hb = Heartbeat(
        db_conn_factory=db_conn_factory,
        pid=os.getpid(),
        clock=clock,
        interval_s=0.01,
    )
    async with hb:
        clock.advance(seconds=5)
        await asyncio.sleep(0.05)
    with db_conn_factory() as conn:
        row = _read_lock_row(conn)
    assert row is not None
    assert row[1] >= clock.now_unix() - 5
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_lock.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.lock'`.

- [ ] **Step 4: Implement `lock.py`**

Create `src/work_assistant/ingest/lock.py`:

```python
"""`worker_locks` row management for the ingest worker.

Single row per worker name (`'ingest'`). Reclaim logic:
- `INSERT OR IGNORE` on first attempt.
- If the row exists, examine `(pid, acquired_at)`:
  - PID is no longer alive (`os.kill(pid, 0)` raises `ProcessLookupError`)
    → reclaim: DELETE then re-attempt INSERT OR IGNORE.
  - `now - acquired_at > LOCK_TTL_SECONDS` → reclaim.
  - Otherwise → raise `LockHeldError` (worker exits clean, code 3).

Heartbeat refreshes `acquired_at` while the worker runs so a long batch is
not reclaimed by a sibling cron tick.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Self

from work_assistant.ingest.clock import Clock
from work_assistant.ingest.errors import LockHeldError

LOCK_NAME = "ingest"
LOCK_TTL_SECONDS = 1800
HEARTBEAT_INTERVAL_S = 60.0


def _is_pid_alive(pid: int) -> bool:
    """Cheap macOS/Linux liveness check via `kill(pid, 0)`."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _reclaim_if_stale(
    conn: sqlite3.Connection, *, now_unix: int
) -> bool:
    """If the lock row is stale (dead PID or TTL expired), DELETE it.
    Returns True if the row was deleted; False if it's still held by a live worker."""
    row = conn.execute(
        "SELECT pid, acquired_at FROM worker_locks WHERE name = ?", (LOCK_NAME,)
    ).fetchone()
    if row is None:
        return True
    pid, acquired_at = row["pid"], row["acquired_at"]
    expired = (now_unix - acquired_at) > LOCK_TTL_SECONDS
    dead = not _is_pid_alive(pid)
    if expired or dead:
        conn.execute("DELETE FROM worker_locks WHERE name = ? AND pid = ?", (LOCK_NAME, pid))
        return True
    return False


def acquire_lock(conn: sqlite3.Connection, *, pid: int, clock: Clock) -> None:
    """Acquire the `'ingest'` row. Raises `LockHeldError` if held by a live worker."""
    now = clock.now_unix()
    cur = conn.execute(
        "INSERT OR IGNORE INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
        (LOCK_NAME, pid, now),
    )
    if cur.rowcount == 1:
        return
    reclaimed = _reclaim_if_stale(conn, now_unix=now)
    if not reclaimed:
        raise LockHeldError(f"lock {LOCK_NAME!r} held by a live worker")
    cur = conn.execute(
        "INSERT OR IGNORE INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
        (LOCK_NAME, pid, now),
    )
    if cur.rowcount != 1:
        raise LockHeldError(
            f"lock {LOCK_NAME!r} race: reclaim succeeded but re-insert failed"
        )


def release_lock(conn: sqlite3.Connection, *, pid: int) -> None:
    """Best-effort release. No-op if a different pid now holds the row."""
    conn.execute(
        "DELETE FROM worker_locks WHERE name = ? AND pid = ?", (LOCK_NAME, pid)
    )


def _refresh(conn: sqlite3.Connection, *, pid: int, now_unix: int) -> None:
    conn.execute(
        "UPDATE worker_locks SET acquired_at = ? WHERE name = ? AND pid = ?",
        (now_unix, LOCK_NAME, pid),
    )


class Heartbeat:
    """Async context manager that refreshes `acquired_at` every `interval_s`.

    Use as `async with Heartbeat(...)` inside the worker run. Cancellation on
    exit is robust to a refresh tick failing — we log and keep going."""

    def __init__(
        self,
        *,
        db_conn_factory: Callable[[], sqlite3.Connection],
        pid: int,
        clock: Clock,
        interval_s: float = HEARTBEAT_INTERVAL_S,
    ) -> None:
        self._db_conn_factory = db_conn_factory
        self._pid = pid
        self._clock = clock
        self._interval_s = interval_s
        self._task: asyncio.Task[None] | None = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval_s)
            try:
                conn = self._db_conn_factory()
                try:
                    _refresh(conn, pid=self._pid, now_unix=self._clock.now_unix())
                finally:
                    conn.close()
            except sqlite3.Error:
                # Diagnostic-only refresh failure; keep heartbeat alive.
                continue

    async def __aenter__(self) -> Self:
        self._task = asyncio.create_task(self._run(), name="ingest-heartbeat")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None


@asynccontextmanager
async def heartbeat_managed(
    *,
    db_conn_factory: Callable[[], sqlite3.Connection],
    pid: int,
    clock: Clock,
    interval_s: float = HEARTBEAT_INTERVAL_S,
):
    """Async helper for code that prefers `async with` syntax."""
    hb = Heartbeat(
        db_conn_factory=db_conn_factory,
        pid=pid,
        clock=clock,
        interval_s=interval_s,
    )
    async with hb:
        yield hb
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_lock.py -v
```

Expected: 7 passed.

- [ ] **Step 6: Commit**

```bash
git add src/work_assistant/ingest/lock.py src/work_assistant/ingest/errors.py tests/ingest/test_lock.py
git commit -m "feat: add worker lock acquire/release/reclaim with heartbeat"
```

---

## Task 8: structlog binding helper

Phase 0 set up stdlib JSON logging. structlog sits on top so each per-source logger is bound with `source=name, run_id=<uuid>`. The carve-out in `CLAUDE.md` allows `structlog.BoundLogger` in our signatures (third-party type).

**Files:**
- Create: `src/work_assistant/ingest/logging_bind.py`
- Test: `tests/ingest/test_logging_bind.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/test_logging_bind.py`:

```python
"""Tests for work_assistant.ingest.logging_bind."""

from __future__ import annotations

import structlog

from work_assistant.ingest.logging_bind import bind_source_logger, configure_structlog


def test_configure_structlog_is_idempotent() -> None:
    configure_structlog()
    configure_structlog()  # second call must not raise
    log = structlog.get_logger("ingest")
    log.info("ok")


def test_bind_source_logger_attaches_keys() -> None:
    configure_structlog()
    log = bind_source_logger(source="slack", run_id="abc123")
    bindings = structlog.contextvars.get_contextvars()
    # The bind happens on the BoundLogger, not contextvars; inspect its context:
    assert log._context.get("source") == "slack"  # type: ignore[attr-defined]
    assert log._context.get("run_id") == "abc123"  # type: ignore[attr-defined]
    # Avoid contextvars false positive
    _ = bindings
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_logging_bind.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.logging_bind'`.

- [ ] **Step 3: Implement `logging_bind.py`**

Create `src/work_assistant/ingest/logging_bind.py`:

```python
"""structlog wiring for the ingest worker.

`configure_structlog()` is idempotent and safe to call alongside the existing
stdlib `logging_setup.setup(proc)` from Phase 0. We hand the rendering off to
structlog's JSON renderer so the same JSON file the stdlib handler writes is
populated with consistently structured records.
"""

from __future__ import annotations

import logging

import structlog

_CONFIGURED = {"done": False}


def configure_structlog() -> None:
    """Configure structlog to emit JSON-shaped records via stdlib `logging`.

    Idempotent: safe to call multiple times across worker invocations and tests.
    """
    if _CONFIGURED["done"]:
        return
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _CONFIGURED["done"] = True


def bind_source_logger(*, source: str, run_id: str) -> structlog.stdlib.BoundLogger:
    """Return a `BoundLogger` pre-bound with `source` + `run_id`."""
    configure_structlog()
    return structlog.get_logger("ingest").bind(source=source, run_id=run_id)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_logging_bind.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/logging_bind.py tests/ingest/test_logging_bind.py
git commit -m "feat: add structlog wiring with per-source binding"
```

---

## Task 9: Source registry

A trivial dict + a small validation helper. Per-source plans add entries; for now it's empty. The worker takes this dict (or an override) and constructs `Source` instances per the enabled list.

**Files:**
- Create: `src/work_assistant/ingest/registry.py`
- Test: `tests/ingest/test_registry.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/test_registry.py`:

```python
"""Tests for work_assistant.ingest.registry."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from work_assistant.ingest.models import Batch, Cursor
from work_assistant.ingest.registry import (
    SOURCES,
    UnknownSourceError,
    select_sources,
)
from work_assistant.ingest.source import Source


class _StubSlack(Source):
    name = "slack"
    mcp_server = "slack"

    async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
        if False:
            yield  # pragma: no cover

    def normalize_body(self, raw: str) -> tuple[str, bool]:
        return raw, False

    async def resolve_actor(self, raw_actor: str) -> str | None:
        return None

    def cursor_from_timestamp(self, ts: int) -> Cursor:
        return Cursor()


def test_default_registry_is_empty() -> None:
    assert SOURCES == {}


def test_select_sources_filters_to_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    registry: dict[str, type[Source]] = {"slack": _StubSlack}
    selected = select_sources(registry=registry, requested=["slack"])
    assert selected == {"slack": _StubSlack}


def test_select_sources_raises_on_unknown_name() -> None:
    registry: dict[str, type[Source]] = {"slack": _StubSlack}
    with pytest.raises(UnknownSourceError, match="gmail"):
        select_sources(registry=registry, requested=["slack", "gmail"])


def test_select_sources_returns_all_when_requested_is_none() -> None:
    registry: dict[str, type[Source]] = {"slack": _StubSlack}
    selected = select_sources(registry=registry, requested=None)
    assert selected == {"slack": _StubSlack}


def test_select_sources_returns_empty_when_requested_is_empty() -> None:
    registry: dict[str, type[Source]] = {"slack": _StubSlack}
    selected = select_sources(registry=registry, requested=[])
    assert selected == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_registry.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.registry'`.

- [ ] **Step 3: Implement `registry.py`**

Create `src/work_assistant/ingest/registry.py`:

```python
"""Source registry. Per-source plans add entries to `SOURCES` in their own
modules; this module owns the dict and the `select_sources()` helper."""

from __future__ import annotations

from work_assistant.ingest.source import Source


class UnknownSourceError(ValueError):
    """Raised when `--source` names a source not present in the registry."""


SOURCES: dict[str, type[Source]] = {}


def select_sources(
    *,
    registry: dict[str, type[Source]],
    requested: list[str] | None,
) -> dict[str, type[Source]]:
    """Pick the subset of `registry` matching `requested`.

    - `requested=None` → return the full registry.
    - `requested=[]` → return an empty dict (caller decides whether that's an error).
    - Any name not in `registry` → raise `UnknownSourceError`.
    """
    if requested is None:
        return dict(registry)
    unknown = [n for n in requested if n not in registry]
    if unknown:
        raise UnknownSourceError(
            f"unknown source(s): {', '.join(unknown)}; available: {sorted(registry)}"
        )
    return {n: registry[n] for n in requested}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_registry.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/registry.py tests/ingest/test_registry.py
git commit -m "feat: add source registry with select_sources helper"
```

---

## Task 10: Per-source runner — `_run_source`, `_run_source_safely`, `SourceRunResult`

The heart of the worker. Loads the source's cursor, iterates its `fetch()`, wraps each batch in a single transaction, advances the cursor on commit, enforces the two-zero-insert-batches guard, and returns a `SourceRunResult`. `_run_source_safely` wraps everything so siblings can't be cancelled by an uncaught exception.

This task also adds a `StubSource` test fake so we can drive the runner without a real source.

**Files:**
- Create: `src/work_assistant/ingest/runner.py`
- Modify: `tests/ingest/fakes.py` (append `StubSource` builder)
- Test: `tests/ingest/test_runner.py`

- [ ] **Step 1: Append `StubSource` to `tests/ingest/fakes.py`**

Edit `tests/ingest/fakes.py` — append:

```python


from collections.abc import AsyncIterator

from work_assistant.ingest.context import IngestContext
from work_assistant.ingest.models import Batch, Cursor
from work_assistant.ingest.source import Source


class StubSource(Source):
    """A `Source` whose `fetch()` yields a pre-scripted list of batches.

    Use `StubSource.make(batches=...)` to build a class on the fly with the
    required `name`/`mcp_server` set, then construct it with an `IngestContext`.
    Set `raise_after` to inject an exception after N batches have been yielded.
    """

    _scripted_batches: list[Batch] = []
    _raise_after: int | None = None
    _raise_exc: BaseException | None = None

    name = "stub"
    mcp_server = "stub"

    @classmethod
    def make(
        cls,
        *,
        name: str = "stub",
        mcp_server: str = "stub",
        batches: list[Batch] | None = None,
        raise_after: int | None = None,
        raise_exc: BaseException | None = None,
    ) -> type[Source]:
        attrs: dict[str, object] = {
            "name": name,
            "mcp_server": mcp_server,
            "_scripted_batches": list(batches or []),
            "_raise_after": raise_after,
            "_raise_exc": raise_exc,
        }
        return type(f"StubSource_{name}", (cls,), attrs)

    async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
        emitted = 0
        for batch in self._scripted_batches:
            yield batch
            emitted += 1
            if self._raise_after is not None and emitted >= self._raise_after:
                if self._raise_exc is not None:
                    raise self._raise_exc

    def normalize_body(self, raw: str) -> tuple[str, bool]:
        return raw, False

    async def resolve_actor(self, raw_actor: str) -> str | None:
        return raw_actor

    def cursor_from_timestamp(self, ts: int) -> Cursor:
        return Cursor()


def make_event(
    *,
    source: str = "slack",
    source_id: str = "m1",
    body: str = "hello",
    occurred_at: int = 1_700_000_000,
):
    """Helper: builds a NormalizedEvent with a Slack metadata variant."""
    from work_assistant.ingest.models import NormalizedEvent, SlackMetadata

    md = SlackMetadata(
        channel_id="C1",
        channel_name="general",
        is_im=False,
        is_mpim=False,
        is_dm=False,
        is_mention=False,
        reactions_json="[]",
        files_json="[]",
    )
    return NormalizedEvent(
        source=source,  # type: ignore[arg-type]
        source_id=source_id,
        source_link=None,
        content_hash="0" * 64,
        occurred_at=occurred_at,
        actor=None,
        thread_key=None,
        kind="message",
        title=None,
        body=body,
        body_truncated=False,
        metadata=md,
    )
```

- [ ] **Step 2: Write the failing tests**

Create `tests/ingest/test_runner.py`:

```python
"""Tests for work_assistant.ingest.runner."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from work_assistant.ingest.context import IngestContext, SqliteDbFactory
from work_assistant.ingest.errors import (
    PermanentIngestError,
    SourceStallError,
)
from work_assistant.ingest.models import Batch, Cursor
from work_assistant.ingest.runner import (
    SourceRunResult,
    run_source,
    run_source_safely,
)
from tests.ingest.fakes import FakeClock, FakeMCPClient, StubSource, make_event


def _ctx(initialized_db: Path) -> IngestContext:
    import structlog

    return IngestContext(
        db=SqliteDbFactory(db_path=initialized_db),
        mcp=FakeMCPClient(),
        logger=structlog.get_logger("test"),
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)),
    )


@pytest.mark.asyncio
async def test_happy_path_inserts_events_and_advances_cursor(
    initialized_db: Path,
) -> None:
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    cls = StubSource.make(
        name="slack",
        batches=[
            Batch(
                events=[make_event(source_id="m1"), make_event(source_id="m2")],
                next_cursor=cursor,
                status="ok",
            )
        ],
    )
    src = cls(ctx)
    result = await run_source(src)
    assert result.status == "ok"
    assert result.inserted == 2
    assert result.ignored == 0
    with sqlite3.connect(initialized_db) as conn:
        n = conn.execute("SELECT count(*) FROM events").fetchone()[0]
    assert n == 2


@pytest.mark.asyncio
async def test_dedup_ignores_duplicate_inserts(initialized_db: Path) -> None:
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    e = make_event(source_id="m1")
    cls = StubSource.make(
        name="slack",
        batches=[Batch(events=[e, e], next_cursor=cursor, status="ok")],
    )
    src = cls(ctx)
    result = await run_source(src)
    assert result.inserted == 1
    assert result.ignored == 1


@pytest.mark.asyncio
async def test_two_zero_insert_batches_raises_stall(initialized_db: Path) -> None:
    """Same event in two consecutive batches → stall after the second."""
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    e = make_event(source_id="m1")
    cls = StubSource.make(
        name="slack",
        batches=[
            Batch(events=[e], next_cursor=cursor, status="ok"),  # inserted=1
            Batch(events=[e], next_cursor=cursor, status="ok"),  # inserted=0 #1
            Batch(events=[e], next_cursor=cursor, status="ok"),  # inserted=0 #2 → stall
        ],
    )
    src = cls(ctx)
    with pytest.raises(SourceStallError):
        await run_source(src)


@pytest.mark.asyncio
async def test_safe_runner_catches_and_classifies_transient(
    initialized_db: Path,
) -> None:
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    cls = StubSource.make(
        name="slack",
        batches=[Batch(events=[make_event()], next_cursor=cursor, status="ok")],
        raise_after=1,
        raise_exc=RuntimeError("network blip"),
    )
    src = cls(ctx)
    result = await run_source_safely(src)
    assert result.status == "error"
    assert result.bucket == "transient"
    assert isinstance(result.exc, RuntimeError)


@pytest.mark.asyncio
async def test_safe_runner_classifies_permanent(initialized_db: Path) -> None:
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    cls = StubSource.make(
        name="slack",
        batches=[Batch(events=[], next_cursor=cursor, status="ok")],
        raise_after=1,
        raise_exc=PermanentIngestError("auth revoked"),
    )
    src = cls(ctx)
    result = await run_source_safely(src)
    assert result.status == "error"
    assert result.bucket == "permanent"


@pytest.mark.asyncio
async def test_safe_runner_propagates_keyboard_interrupt(initialized_db: Path) -> None:
    ctx = _ctx(initialized_db)
    cursor = Cursor()
    cls = StubSource.make(
        name="slack",
        batches=[Batch(events=[], next_cursor=cursor, status="ok")],
        raise_after=1,
        raise_exc=KeyboardInterrupt(),
    )
    src = cls(ctx)
    with pytest.raises(KeyboardInterrupt):
        await run_source_safely(src)


@pytest.mark.asyncio
async def test_empty_batch_with_new_cursor_advances(initialized_db: Path) -> None:
    """Gmail historyId case: events=[] but cursor moves forward."""

    class _MovedCursor(Cursor):
        history_id: str

    ctx = _ctx(initialized_db)
    cls = StubSource.make(
        name="gmail",
        batches=[Batch(events=[], next_cursor=_MovedCursor(history_id="h2"), status="ok")],
    )
    src = cls(ctx)
    result = await run_source(src)
    assert result.status == "ok"
    assert result.inserted == 0
    with sqlite3.connect(initialized_db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT cursor FROM ingest_cursors WHERE source='gmail'").fetchone()
    assert row is not None
    assert "h2" in row["cursor"]
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_runner.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.runner'`.

- [ ] **Step 4: Implement `runner.py`**

Create `src/work_assistant/ingest/runner.py`:

```python
"""Per-source runner.

`run_source(source)` walks `source.fetch(cursor)`, persists each batch in a
single SQLite transaction, advances the cursor on commit, and enforces the
two-consecutive-zero-insert-batches guard.

`run_source_safely(source)` wraps `run_source` so a failure in one source
cannot cancel siblings under `asyncio.gather(return_exceptions=True)` (the
gather still receives a `SourceRunResult`, never an exception).
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from typing import Literal

from work_assistant.ingest.errors import (
    ErrorBucket,
    SourceStallError,
    classify,
)
from work_assistant.ingest.models import Batch, Cursor, NormalizedEvent
from work_assistant.ingest.source import Source

SourceStatus = Literal["ok", "error", "skipped"]


@dataclass(frozen=True)
class SourceRunResult:
    name: str
    status: SourceStatus
    inserted: int = 0
    ignored: int = 0
    bucket: ErrorBucket | None = None
    exc: BaseException | None = None


_INSERT_EVENT_SQL = (
    "INSERT OR IGNORE INTO events ("
    " source, source_id, source_link, content_hash, occurred_at, ingested_at,"
    " actor, thread_key, kind, title, body, metadata_json"
    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

_LOAD_CURSOR_SQL = "SELECT cursor FROM ingest_cursors WHERE source = ?"
_UPSERT_CURSOR_SQL = (
    "INSERT INTO ingest_cursors(source, cursor, updated_at, last_status) "
    "VALUES (?, ?, ?, 'ok') "
    "ON CONFLICT(source) DO UPDATE SET "
    " cursor = excluded.cursor, updated_at = excluded.updated_at, last_status = 'ok'"
)
_UPSERT_CURSOR_ERROR_SQL = (
    "INSERT INTO ingest_cursors(source, cursor, updated_at, last_status) "
    "VALUES (?, COALESCE((SELECT cursor FROM ingest_cursors WHERE source = ?), ''), ?, ?) "
    "ON CONFLICT(source) DO UPDATE SET "
    " updated_at = excluded.updated_at, last_status = excluded.last_status"
)


def _load_cursor_text(conn: sqlite3.Connection, source_name: str) -> str | None:
    row = conn.execute(_LOAD_CURSOR_SQL, (source_name,)).fetchone()
    if row is None:
        return None
    text = row["cursor"]
    return text if text else None


def _persist_batch(
    conn: sqlite3.Connection,
    *,
    source_name: str,
    events: list[NormalizedEvent],
    next_cursor_json: str,
    now_unix: int,
) -> tuple[int, int]:
    """Insert events and upsert the cursor, all in a single transaction.

    Returns `(inserted, ignored)`.
    """
    inserted = 0
    ignored = 0
    conn.execute("BEGIN")
    try:
        for ev in events:
            cur = conn.execute(
                _INSERT_EVENT_SQL,
                (
                    ev.source,
                    ev.source_id,
                    ev.source_link,
                    ev.content_hash,
                    ev.occurred_at,
                    now_unix,
                    ev.actor,
                    ev.thread_key,
                    ev.kind,
                    ev.title,
                    ev.body,
                    ev.metadata.model_dump_json(),
                ),
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                ignored += 1
        conn.execute(
            _UPSERT_CURSOR_SQL, (source_name, next_cursor_json, now_unix)
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return inserted, ignored


def _persist_error_status(
    *,
    db_factory,
    source_name: str,
    now_unix: int,
    detail: str,
) -> None:
    """Best-effort: write `last_status='error: ...'` without changing the cursor."""
    with db_factory.open() as conn:
        conn.execute(
            _UPSERT_CURSOR_ERROR_SQL,
            (source_name, source_name, now_unix, f"error: {detail}"),
        )


async def run_source(source: Source) -> SourceRunResult:
    """Run a single source. Raises on unhandled exception."""
    ctx = source.ctx
    cursor: Cursor | None = None
    consecutive_zero = 0
    inserted_total = 0
    ignored_total = 0

    with ctx.db.open() as conn:
        existing_cursor_text = _load_cursor_text(conn, source.name)

    if existing_cursor_text is not None:
        # Each Source's per-source plan parses this into its Cursor subclass.
        # The scaffold can't parse a per-source shape, so we hand the raw JSON
        # to the source via the `model_validate_json` route once subclasses ship.
        # For now: pass `None` so the source treats it as first-run. Per-source
        # plans override this method to parse their own cursor shape.
        cursor = None

    async for batch in source.fetch(cursor):
        with ctx.db.open() as conn:
            inserted, ignored = _persist_batch(
                conn,
                source_name=source.name,
                events=batch.events,
                next_cursor_json=batch.next_cursor.model_dump_json(),
                now_unix=ctx.clock.now_unix(),
            )
        inserted_total += inserted
        ignored_total += ignored
        ctx.logger.info(
            "batch_committed",
            inserted=inserted,
            ignored=ignored,
            events=len(batch.events),
            status=batch.status,
        )
        if batch.events and inserted == 0:
            consecutive_zero += 1
            ctx.logger.warning(
                "zero_insert_batch",
                ignored=ignored,
                consecutive=consecutive_zero,
            )
            if consecutive_zero >= 2:
                raise SourceStallError(
                    f"{source.name}: 2 consecutive batches inserted 0 / "
                    f"ignored {ignored}+ events. Likely pagination or dedup-key bug."
                )
        else:
            consecutive_zero = 0

    return SourceRunResult(
        name=source.name,
        status="ok",
        inserted=inserted_total,
        ignored=ignored_total,
    )


async def run_source_safely(source: Source) -> SourceRunResult:
    """Wraps `run_source` so siblings under gather() never see the raw raise.

    Propagates `KeyboardInterrupt` and `asyncio.CancelledError` so cooperative
    shutdown still works; classifies everything else.
    """
    try:
        return await run_source(source)
    except (KeyboardInterrupt, asyncio.CancelledError):
        raise
    except BaseException as exc:
        bucket = classify(exc)
        try:
            _persist_error_status(
                db_factory=source.ctx.db,
                source_name=source.name,
                now_unix=source.ctx.clock.now_unix(),
                detail=f"{type(exc).__name__}: {exc}",
            )
        except Exception as inner:
            source.ctx.logger.error(
                "status_write_failed",
                primary=repr(exc),
                inner=repr(inner),
            )
        return SourceRunResult(
            name=source.name,
            status="error",
            bucket=bucket,
            exc=exc,
        )
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_runner.py -v
```

Expected: 7 passed.

- [ ] **Step 6: Commit**

```bash
git add src/work_assistant/ingest/runner.py tests/ingest/fakes.py tests/ingest/test_runner.py
git commit -m "feat: add per-source runner with stall guard and safe wrapper"
```

---

## Task 11: Top-level worker — `run()` orchestrator + exit-code mapping

The single async entrypoint the CLI calls. It opens the lock connection, acquires the lock, starts the heartbeat, dispatches `run_source_safely` per source via `asyncio.gather(return_exceptions=False)` (the safe wrapper never raises), then computes the exit code.

**Files:**
- Create: `src/work_assistant/ingest/worker.py`
- Test: `tests/ingest/test_worker.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/test_worker.py`:

```python
"""Top-level worker integration tests."""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from work_assistant.ingest.errors import PermanentIngestError
from work_assistant.ingest.models import Batch, Cursor
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
from work_assistant.ingest.runner import SourceRunResult
from tests.ingest.fakes import FakeClock, StubSource, make_event


def _opts(initialized_db: Path, **overrides) -> WorkerOptions:
    return WorkerOptions(
        registry=overrides.pop("registry", {}),
        sources_enabled=overrides.pop("sources_enabled", []),
        requested_sources=overrides.pop("requested_sources", None),
        dry_run=overrides.pop("dry_run", False),
        since_unix=overrides.pop("since_unix", None),
        clock=overrides.pop(
            "clock", FakeClock(datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC))
        ),
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
        batches=[Batch(events=[make_event(source="todoist", source_id="t1")], next_cursor=Cursor(), status="ok")],
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
    with sqlite3.connect(initialized_db) as conn:
        conn.execute(
            "INSERT INTO worker_locks(name, pid, acquired_at) VALUES (?, ?, ?)",
            ("ingest", os.getpid(), 1_700_000_000),
        )
    opts = _opts(
        initialized_db,
        clock=FakeClock(datetime(2026, 6, 4, 0, 0, 0, tzinfo=UTC)),  # within TTL
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
    trans = SourceRunResult(
        name="z", status="error", bucket="transient", exc=RuntimeError("t")
    )
    # 4 > 5 > 1 > 3 > 2 > 0
    assert compute_exit_code([base], lock_held=False, config_fatal=False) == EXIT_OK
    assert compute_exit_code([trans], lock_held=False, config_fatal=False) == EXIT_TRANSIENT
    assert compute_exit_code([perm], lock_held=False, config_fatal=False) == EXIT_PERMANENT
    assert (
        compute_exit_code([perm, trans], lock_held=False, config_fatal=False)
        == EXIT_PERMANENT
    )
    assert (
        compute_exit_code([], lock_held=False, config_fatal=True) == EXIT_CONFIG_FATAL
    )
    assert (
        compute_exit_code([perm], lock_held=False, config_fatal=True)
        == EXIT_CONFIG_FATAL
    )
    assert compute_exit_code([], lock_held=True, config_fatal=False) == EXIT_LOCK_HELD
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_worker.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.worker'`.

- [ ] **Step 3: Implement `worker.py`**

Create `src/work_assistant/ingest/worker.py`:

```python
"""Top-level ingest worker.

Acquires the lock, starts the heartbeat, runs each enabled source under
`asyncio.gather(return_exceptions=False)` (each coro is wrapped in
`run_source_safely`), then computes the exit code per spec §6.2.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from work_assistant import paths
from work_assistant.ingest.clock import Clock
from work_assistant.ingest.context import DbFactory, IngestContext, SqliteDbFactory
from work_assistant.ingest.errors import LockHeldError
from work_assistant.ingest.lock import acquire_lock, heartbeat_managed, release_lock
from work_assistant.ingest.logging_bind import bind_source_logger
from work_assistant.ingest.registry import UnknownSourceError, select_sources
from work_assistant.ingest.runner import SourceRunResult, run_source_safely
from work_assistant.ingest.source import Source
from work_assistant.mcp.client import MCPClient, MCPRequest, MCPResponse

EXIT_OK = 0
EXIT_TRANSIENT = 1
EXIT_USAGE = 2
EXIT_LOCK_HELD = 3
EXIT_CONFIG_FATAL = 4
EXIT_PERMANENT = 5
EXIT_KEYBOARD_INTERRUPT = 130


class _NullMCPClient(MCPClient):
    """Scaffold placeholder. Any call raises; per-source plans wire real bridges."""

    async def call(
        self,
        request: MCPRequest,
        response_model: type[MCPResponse],
    ) -> MCPResponse:
        raise RuntimeError(
            "MCP bridge not wired; the scaffold ships without per-source MCP setup"
        )


class _DryRunDbFactory(DbFactory):
    """Wrap a real factory; on COMMIT, ROLLBACK instead. No rows ever persist."""

    def __init__(self, *, inner: SqliteDbFactory) -> None:
        self._inner = inner

    @contextmanager
    def open(self) -> Iterator[sqlite3.Connection]:
        with self._inner.open() as conn:
            original_execute = conn.execute

            def _swap(stmt: str, *args: object) -> sqlite3.Cursor:
                if stmt.strip().upper() == "COMMIT":
                    return original_execute("ROLLBACK")
                return original_execute(stmt, *args)

            conn.execute = _swap  # type: ignore[method-assign]
            yield conn


@dataclass(frozen=True)
class WorkerOptions:
    """Inputs to `run_worker`. Built by the CLI; tests construct directly."""

    registry: dict[str, type[Source]] = field(default_factory=dict)
    sources_enabled: list[str] = field(default_factory=list)
    requested_sources: list[str] | None = None
    dry_run: bool = False
    since_unix: int | None = None
    clock: Clock | None = None
    pid: int = 0
    run_id: str = ""


def compute_exit_code(
    results: list[SourceRunResult],
    *,
    lock_held: bool,
    config_fatal: bool,
) -> int:
    """Apply spec §6.2 precedence: 4 > 5 > 1 > 3 > 2 > 0."""
    if config_fatal:
        return EXIT_CONFIG_FATAL
    has_permanent = any(r.bucket == "permanent" for r in results)
    has_transient = any(r.bucket == "transient" for r in results)
    if has_permanent:
        return EXIT_PERMANENT
    if has_transient:
        return EXIT_TRANSIENT
    if lock_held:
        return EXIT_LOCK_HELD
    return EXIT_OK


def _resolve_enabled(opts: WorkerOptions) -> list[str]:
    """Pick which source names to run.

    `requested_sources` (CLI `--source`) overrides config; otherwise use the
    config-enabled list. Returns the resolved list of names.
    """
    if opts.requested_sources is not None:
        return list(opts.requested_sources)
    return list(opts.sources_enabled)


def _build_sources(
    *,
    selected: dict[str, type[Source]],
    db_factory: DbFactory,
    clock: Clock,
    run_id: str,
) -> list[Source]:
    """Build one `Source` instance per selected entry, each with its own context."""
    instances: list[Source] = []
    for name, cls in selected.items():
        logger = bind_source_logger(source=name, run_id=run_id)
        ctx = IngestContext(
            db=db_factory,
            mcp=_NullMCPClient(),
            logger=logger,
            settings=None,  # type: ignore[arg-type]
            clock=clock,
        )
        instances.append(cls(ctx))
    return instances


async def run_worker(opts: WorkerOptions) -> int:
    """Drive a single ingest run. Returns the exit code."""
    if opts.clock is None:
        raise ValueError("WorkerOptions.clock is required")

    base_logger = bind_source_logger(source="-", run_id=opts.run_id)

    try:
        names = _resolve_enabled(opts)
        selected = select_sources(
            registry=opts.registry, requested=names if names else None
        )
    except UnknownSourceError as exc:
        base_logger.error("unknown_source", detail=str(exc))
        return EXIT_USAGE

    db_path = paths.db_path()
    real_factory = SqliteDbFactory(db_path=db_path)
    db_factory: DbFactory = (
        _DryRunDbFactory(inner=real_factory) if opts.dry_run else real_factory
    )

    def _new_lock_conn() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    try:
        with _new_lock_conn() as lock_conn:
            try:
                acquire_lock(lock_conn, pid=opts.pid, clock=opts.clock)
            except LockHeldError:
                base_logger.warning("lock_held")
                return compute_exit_code([], lock_held=True, config_fatal=False)
    except sqlite3.Error as exc:
        base_logger.error("lock_db_error", detail=repr(exc))
        return compute_exit_code([], lock_held=False, config_fatal=True)

    results: list[SourceRunResult] = []
    try:
        async with heartbeat_managed(
            db_conn_factory=_new_lock_conn,
            pid=opts.pid,
            clock=opts.clock,
        ):
            sources = _build_sources(
                selected=selected,
                db_factory=db_factory,
                clock=opts.clock,
                run_id=opts.run_id,
            )
            if not sources:
                return EXIT_OK
            results = await asyncio.gather(
                *(run_source_safely(s) for s in sources),
                return_exceptions=False,
            )
    except KeyboardInterrupt:
        base_logger.warning("keyboard_interrupt")
        return EXIT_KEYBOARD_INTERRUPT
    finally:
        with _new_lock_conn() as lock_conn:
            release_lock(lock_conn, pid=opts.pid)

    return compute_exit_code(results, lock_held=False, config_fatal=False)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_worker.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/worker.py tests/ingest/test_worker.py
git commit -m "feat: add ingest worker run() with lock, heartbeat, gather, exit codes"
```

---

## Task 12: `wa ingest` CLI subcommand + flag handling

Wires `run_worker` into the existing `wa` CLI. Flags: `--source`, `--dry-run`, `--verbose`, `--since`. `--since` is parsed as ISO-8601 and converted to unix seconds (UTC). Unknown source → exit 2.

The CLI imports `SOURCES` from `work_assistant.ingest.registry`. Today that dict is empty; per-source plans add entries.

**Files:**
- Create: `src/work_assistant/ingest/cli.py`
- Modify: `src/work_assistant/cli.py` (register the subcommand)
- Test: `tests/ingest/test_cli.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/test_cli.py`:

```python
"""Tests for the `wa ingest` CLI subcommand."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from work_assistant.ingest import cli as ingest_cli


def test_ingest_cli_no_sources_returns_zero(initialized_db: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(ingest_cli.ingest, [])
    assert result.exit_code == 0, result.output


def test_ingest_cli_unknown_source_exits_two(
    initialized_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = CliRunner()
    result = runner.invoke(ingest_cli.ingest, ["--source", "nope"])
    assert result.exit_code == 2, result.output


def test_ingest_cli_passes_since_as_unix(
    initialized_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_worker(opts):  # type: ignore[no-untyped-def]
        captured["opts"] = opts
        return 0

    monkeypatch.setattr(ingest_cli, "run_worker", fake_run_worker)
    runner = CliRunner()
    result = runner.invoke(
        ingest_cli.ingest, ["--since", "2026-06-01T00:00:00+00:00"]
    )
    assert result.exit_code == 0
    expected_unix = int(datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC).timestamp())
    assert captured["opts"].since_unix == expected_unix


def test_ingest_cli_parses_comma_separated_source_list(
    initialized_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_worker(opts):  # type: ignore[no-untyped-def]
        captured["opts"] = opts
        return 0

    monkeypatch.setattr(ingest_cli, "run_worker", fake_run_worker)
    runner = CliRunner()
    # Empty registry — but we mock `run_worker` so the registry check is skipped.
    result = runner.invoke(ingest_cli.ingest, ["--source", "slack,gmail"])
    assert result.exit_code == 0
    assert captured["opts"].requested_sources == ["slack", "gmail"]


def test_ingest_cli_dry_run_flag_propagates(
    initialized_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_worker(opts):  # type: ignore[no-untyped-def]
        captured["opts"] = opts
        return 0

    monkeypatch.setattr(ingest_cli, "run_worker", fake_run_worker)
    runner = CliRunner()
    result = runner.invoke(ingest_cli.ingest, ["--dry-run"])
    assert result.exit_code == 0
    assert captured["opts"].dry_run is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/ingest/test_cli.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.cli'`.

- [ ] **Step 3: Implement `ingest/cli.py`**

Create `src/work_assistant/ingest/cli.py`:

```python
"""`wa ingest` click subcommand.

Exit codes (spec §6.2):
- 0 ok
- 1 transient error in one or more sources
- 2 usage error (unknown source, bad flag)
- 3 lock held by live worker
- 4 config-fatal
- 5 permanent error in one or more sources
- 130 KeyboardInterrupt
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import datetime

import click

from work_assistant import config as wa_config
from work_assistant import logging_setup
from work_assistant.ingest.clock import SystemClock
from work_assistant.ingest.registry import SOURCES
from work_assistant.ingest.worker import (
    EXIT_CONFIG_FATAL,
    EXIT_USAGE,
    WorkerOptions,
    run_worker,
)


def _parse_source_list(value: str | None) -> list[str] | None:
    """Comma-separated → list. None → None (= use config-enabled list)."""
    if value is None:
        return None
    return [s.strip() for s in value.split(",") if s.strip()]


def _parse_since(value: str | None) -> int | None:
    if value is None:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise click.BadParameter("--since must be timezone-aware ISO-8601")
    return int(dt.timestamp())


@click.command("ingest")
@click.option(
    "--source",
    "source_str",
    type=str,
    default=None,
    help="Comma-separated list of source names to run (overrides config).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Fetch + normalize but write no rows.",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="DEBUG-level structured logs.",
)
@click.option(
    "--since",
    "since_str",
    type=str,
    default=None,
    help="ISO-8601 timestamp; one-shot read-only override (cursor not persisted).",
)
def ingest(
    source_str: str | None,
    dry_run: bool,
    verbose: bool,
    since_str: str | None,
) -> None:
    """Run a single ingest pass (one process, all enabled sources)."""
    logging_setup.setup("wa-ingest")
    requested = _parse_source_list(source_str)
    try:
        since_unix = _parse_since(since_str)
    except click.BadParameter as exc:
        click.echo(str(exc), err=True)
        sys.exit(EXIT_USAGE)

    sources_enabled: list[str] = []
    config_fatal = False
    try:
        cfg = wa_config.load()
        sources_enabled = list(cfg.ingest.sources_enabled)
    except wa_config.ConfigError as exc:
        click.echo(f"config error: {exc}", err=True)
        config_fatal = True

    opts = WorkerOptions(
        registry=SOURCES,
        sources_enabled=sources_enabled,
        requested_sources=requested,
        dry_run=dry_run,
        since_unix=since_unix,
        clock=SystemClock(),
        pid=os.getpid(),
        run_id=uuid.uuid4().hex,
    )

    if config_fatal:
        sys.exit(EXIT_CONFIG_FATAL)

    code = asyncio.run(run_worker(opts))
    sys.exit(code)
```

- [ ] **Step 4: Register the subcommand in the main CLI**

Edit `src/work_assistant/cli.py`. Append to the bottom of the file (after `if __name__ == "__main__"` is fine; before is also fine — placement just needs to be after `cli` is defined and before any module re-runs on import):

```python
from work_assistant.ingest.cli import ingest as _ingest_cmd

cli.add_command(_ingest_cmd)
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/ingest/test_cli.py -v
```

Expected: 5 passed.

- [ ] **Step 6: Verify CLI registration**

Run:

```bash
uv run wa --help
uv run wa ingest --help
```

Expected: `wa --help` lists `ingest` alongside `doctor` and `db`. `wa ingest --help` shows the four flags.

- [ ] **Step 7: Commit**

```bash
git add src/work_assistant/ingest/cli.py src/work_assistant/cli.py tests/ingest/test_cli.py
git commit -m "feat: add wa ingest cli with --source/--dry-run/--verbose/--since"
```

---

## Task 13: Final lint + full suite

Verify the whole scaffold is green and clean.

**Files:** none created.

- [ ] **Step 1: Run the full suite**

Run:

```bash
uv run pytest -v
```

Expected: all Phase 0 tests still pass, plus all new ingest tests pass. No regressions.

- [ ] **Step 2: Run ruff**

Run:

```bash
uv run ruff check
uv run ruff format --check
```

If anything fails, fix and run again. Commit fixes as `chore: ruff cleanup`.

- [ ] **Step 3: Sanity-check `wa ingest` end-to-end with empty registry**

Run:

```bash
uv run wa ingest
```

Expected: exit code 0, no output (no sources registered yet). The lock row was acquired and released; `worker_locks` is empty after exit:

```bash
sqlite3 ~/.work_assistant/db/spine.sqlite "SELECT * FROM worker_locks"
```

Expected: no rows.

- [ ] **Step 4: Sanity-check unknown-source exit code**

Run:

```bash
uv run wa ingest --source bogus
echo "exit=$?"
```

Expected: `exit=2`.

- [ ] **Step 5: Phase 1 scaffold complete — tag**

```bash
git tag phase-1-ingest-scaffold
git log --oneline phase-0-foundations..phase-1-ingest-scaffold
```

Expected: a clean linear history of ~13 commits, one per task plus any ruff-cleanup commit.

---

## Self-review notes

**Spec coverage:**
- §1 architecture (lock + heartbeat + gather flow) → Tasks 7, 11.
- §2 `Source` ABC with abstract `fetch`/`normalize_body`/`resolve_actor`/`cursor_from_timestamp` and concrete `compute_content_hash` → Task 5.
- §2.1 `MCPClient` adapter ABC + `BridgeMCPClient` containing `Any` → Task 4.
- §2.2 `Cursor` base, `NormalizedEvent` aligned with `events`, `Batch`, four metadata variants in a discriminated union → Task 1.
- §3.1 lock model (`INSERT OR IGNORE`, PID-alive, TTL, heartbeat, release) → Task 7.
- §3.2 isolation via `gather(return_exceptions=True)` semantics implemented as `run_source_safely` + `gather(..., return_exceptions=False)` → Tasks 10, 11.
- §3.3 run flow → Task 11.
- §3.4 fetch loop with stall guard → Task 10.
- §3.5 invariants (cursor advances only inside successful tx; empty batch may still advance; SQLite WAL writes serialize; clock injection) → Tasks 10, 11. Per-source `Cursor` parsing happens in per-source plans; the scaffold's `run_source` reads the cursor row but defers per-source parsing.
- §4 `IngestContext` (frozen dataclass; `db`, `mcp`, `logger`, `settings`, `clock`) → Tasks 6, 8, 11.
- §5 test harness (`FakeMCPClient`, `FakeClock`, `tmp_db`, scripted misuse errors) → Tasks 3, 4. Per-source fixture loaders ship with each per-source plan.
- §5.2 worker-level tests (lock, isolation, `--source` filter, `--dry-run`, unknown source) → Tasks 7, 11, 12.
- §5.3 integration tier explicitly out of scope per spec.
- §6.1 CLI flags including `--since` → Task 12.
- §6.2 exit codes with precedence `4 > 5 > 1 > 3 > 2 > 0` → Task 11.
- §6.3 failure matrix: covered by the runner + worker; per-source rows that depend on per-source code (network 5xx, auth revoked) materialize when each source is implemented and raises a `TransientIngestError` / `PermanentIngestError`.

**Type consistency:**
- `MCPClient.call(request, response_model)` signature is identical across `BridgeMCPClient` and `FakeMCPClient`.
- `SourceRunResult` shape is the same in `run_source` and `run_source_safely`; `bucket: ErrorBucket | None` populated only on error.
- `Clock.now()` returns `datetime`; `Clock.now_unix()` returns `int` — used uniformly by the lock SQL.
- `compute_content_hash(source_id, body)` accepts `body: str | None` so callers don't need to coerce upstream.

**Out-of-scope guards:**
- Per-source `Source` impls (Slack, Gmail, Calendar, Todoist) → each gets its own design spec + plan.
- Webhook receiver, backfill horizon policy, embedding generation, long-running daemon mode: explicitly excluded.
