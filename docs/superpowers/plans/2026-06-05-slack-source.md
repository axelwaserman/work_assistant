# Slack Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** First concrete `Source` impl: cron-driven Slack ingest. Pulls `conversations.history` per member channel, fetches `conversations.replies` for user-participated or user-mentioned threads, normalizes into `events`, advances per-channel cursor, classifies errors, wires the real `BridgeMCPClient` into the worker (replacing `_NullMCPClient`).

**Architecture:** Single module `src/work_assistant/ingest/sources/slack.py` (~500 lines) holding `SlackCursor`, `ChannelCursor`, all per-tool MCP request/response models, `_SlackUserCache`, and `SlackSource(Source)`. New SQLite migration adds `slack_users`. New package `ingest/sources/__init__.py` registers the source on import. Worker rewires to `BridgeMCPClient` grouped by `mcp_server`. `run_source` retains its scaffold contract; `SlackSource` handles cursor-row read internally to satisfy the deferred Phase 1 follow-up.

**Tech Stack:** Python 3.13, `pydantic` v2 (frozen models), `mcp` SDK (already wired via `MCPBridge` and `BridgeMCPClient`), `pytest` + `pytest-asyncio`, `ruff`. No new dependencies.

---

## File structure

Files this plan creates or modifies:

```
work_assistant/
├── src/work_assistant/
│   ├── ingest/
│   │   ├── sources/
│   │   │   ├── __init__.py                          (NEW: imports slack module to register)
│   │   │   └── slack.py                             (NEW: cursor + MCP types + cache + source)
│   │   └── worker.py                                (MODIFY: drop _NullMCPClient; build BridgeMCPClient grouped by mcp_server; pass real settings)
│   ├── db/migrations_sql/
│   │   └── 0002_slack_users.sql                     (NEW: slack_users cache table)
└── tests/
    └── ingest/
        └── sources/
            ├── __init__.py                          (NEW: empty)
            ├── test_slack_models.py                 (NEW: cursor types)
            ├── test_slack_user_cache.py             (NEW: _SlackUserCache against SQLite)
            ├── test_slack_source.py                 (NEW: fetch happy + branches against FakeMCPClient)
            ├── test_slack_errors.py                 (NEW: error classification)
            ├── test_slack_registry.py               (NEW: import side-effect registers)
            └── test_worker_slack.py                 (NEW: end-to-end with real SlackSource + FakeMCPClient)
```

Out of scope for this plan (per spec §9):
- Webhook receiver.
- Backfill via Slack export ZIP.
- Multi-workspace support.
- Reaction/edit/delete events.
- Real-API smoke tests.

---

## Conventions used throughout this plan

- **Working directory:** `/Users/axel/code/work_assistant`. All paths relative to that root.
- **Branch:** `slack-source` (created off `master` after PR #3 merged).
- **Python:** 3.13 only.
- **Test runner:** `uv run pytest`.
- **Lint/format:** `uv run ruff check`, `uv run ruff format`.
- **Repo conventions (`CLAUDE.md`):** ABC always — never `Protocol`. No `Any` in our own signatures (carve-outs at the parse seam inside `BridgeMCPClient` only). Module-level absolute imports. Pathlib + `encoding="utf-8"`. Frozen pydantic models.
- **Cursor scaffold gap:** the Phase 1 runner passes `cursor=None` to `source.fetch`. `SlackSource.fetch` reads its own cursor row via `ctx.db` rather than relying on the runner's parsing. This is the override path the scaffold's docstring promises ("per-source plans override this method to parse their own cursor shape").
- **Tool name strings:** the **Official Slack MCP** binary's exact tool names are confirmed in Task 0 (probe step). Constants live alongside each `MCPRequest` subclass.

---

## Task 0: Branch + probe Slack MCP tool names + create directory skeleton

**Files:**
- Create: `src/work_assistant/ingest/sources/__init__.py` (empty)
- Create: `tests/ingest/sources/__init__.py` (empty)

This task verifies the branch is clean, captures the actual Slack MCP tool name strings (the spec's strings are best-effort), and creates the empty package directories so subsequent imports work.

- [ ] **Step 1: Create branch off master**

```bash
git fetch origin
git checkout master
git pull --ff-only
git checkout -b slack-source
```

Verify clean: `git status` shows nothing modified.

- [ ] **Step 2: Probe Slack MCP tool names**

The plan assumes these tool names. Verify against the actual Official Slack MCP binary you intend to ship:

| Spec name | Likely actual name |
|---|---|
| `slack_list_channels` | `conversations_list` (Slack standard) |
| `slack_conversations_history` | `conversations_history` |
| `slack_conversations_replies` | `conversations_replies` |
| `slack_users_info` | `users_info` |
| `slack_get_permalink` | `chat_get_permalink` |
| `slack_auth_test` | `auth_test` |

If you don't have the MCP server installed locally, take this from the published tool schema. If unsure, **STOP and ask** before proceeding — the rest of the plan hard-codes these strings as `tool_name: ClassVar[str]` on each `MCPRequest`. Use whatever the server actually exposes.

For the rest of this plan, references use the canonical Slack API endpoint names (`conversations_history`, `conversations_replies`, etc.). If the chosen MCP binary uses a different convention, do a global rename in the implementer step before committing.

- [ ] **Step 3: Create empty package init files**

```bash
mkdir -p src/work_assistant/ingest/sources tests/ingest/sources
touch src/work_assistant/ingest/sources/__init__.py
touch tests/ingest/sources/__init__.py
```

`src/work_assistant/ingest/sources/__init__.py` will be populated in Task 6 (registers via import side-effect after `slack` module exists).

- [ ] **Step 4: Verify pytest collects**

```bash
uv run pytest tests/ingest/sources/ -v
```

Expected: `no tests ran in 0.0X s`. Sanity check that the new test directory is discovered.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/sources/__init__.py tests/ingest/sources/__init__.py
git commit -m "chore: scaffold slack source package directory"
```

---

## Task 1: Migration `0002_slack_users.sql`

**Files:**
- Create: `src/work_assistant/db/migrations_sql/0002_slack_users.sql`
- Test: existing `tests/test_db_migrations.py` already exercises any new file matching `NNNN_*.sql`. Add no new test, but verify the migration applies.

- [ ] **Step 1: Write the migration**

Create `src/work_assistant/db/migrations_sql/0002_slack_users.sql`:

```sql
-- Slack user cache. Refreshed weekly per docs/04-ingestion-pipelines.md §4.1.

CREATE TABLE slack_users (
  user_id      TEXT PRIMARY KEY,
  email        TEXT,
  display_name TEXT NOT NULL,
  fetched_at   INTEGER NOT NULL
);

CREATE INDEX idx_slack_users_fetched_at ON slack_users(fetched_at);
```

- [ ] **Step 2: Verify migration applies cleanly via existing test**

Run:

```bash
uv run pytest tests/test_db_migrations.py -v
```

Expected: existing tests pass; the harness loads every `*.sql` file in the migrations dir.

- [ ] **Step 3: Manual sanity check (optional)**

```bash
uv run python -c "
from pathlib import Path
from work_assistant import paths
from work_assistant.db import migrations
import os, tempfile
with tempfile.TemporaryDirectory() as td:
    os.environ['HOME'] = td
    paths.ensure_dirs()
    repo_root = Path(__file__).resolve().parents[0] if False else Path('.').resolve()
    migrations.apply(repo_root / 'src' / 'work_assistant' / 'db' / 'migrations_sql')
    import sqlite3
    conn = sqlite3.connect(paths.db_path())
    cols = [row[1] for row in conn.execute('PRAGMA table_info(slack_users)').fetchall()]
    assert cols == ['user_id', 'email', 'display_name', 'fetched_at'], cols
    print('ok')
"
```

Expected: prints `ok`.

- [ ] **Step 4: Commit**

```bash
git add src/work_assistant/db/migrations_sql/0002_slack_users.sql
git commit -m "feat: add slack_users cache migration"
```

---

## Task 2: `SlackCursor` + `ChannelCursor` + cursor helpers

**Files:**
- Create: `src/work_assistant/ingest/sources/slack.py` (initial version — cursor types only).
- Test: `tests/ingest/sources/test_slack_models.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/sources/test_slack_models.py`:

```python
"""Tests for SlackCursor and ChannelCursor."""

from __future__ import annotations

import pytest

from work_assistant.ingest.sources.slack import ChannelCursor, SlackCursor


def test_slack_cursor_default_is_empty() -> None:
    cur = SlackCursor()
    assert cur.channels == []


def test_slack_cursor_serializes_to_json() -> None:
    cur = SlackCursor(
        channels=[
            ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="100.000"),
        ]
    )
    payload = cur.model_dump_json()
    assert "C1" in payload
    assert "general" in payload
    assert "100.000" in payload


def test_slack_cursor_round_trip() -> None:
    original = SlackCursor(
        channels=[
            ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="100.000"),
            ChannelCursor(channel_id="C2", channel_name="random", last_seen_ts="200.000"),
        ]
    )
    payload = original.model_dump_json()
    restored = SlackCursor.model_validate_json(payload)
    assert restored == original


def test_slack_cursor_lookup_returns_match_or_none() -> None:
    cur = SlackCursor(
        channels=[
            ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="100.000"),
        ]
    )
    found = cur.lookup("C1")
    assert found is not None
    assert found.channel_name == "general"
    assert cur.lookup("C_MISSING") is None


def test_slack_cursor_with_updated_replaces_existing() -> None:
    cur = SlackCursor(
        channels=[
            ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="100.000"),
            ChannelCursor(channel_id="C2", channel_name="random", last_seen_ts="200.000"),
        ]
    )
    new = cur.with_updated(
        ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="500.000")
    )
    assert new.lookup("C1").last_seen_ts == "500.000"
    assert new.lookup("C2").last_seen_ts == "200.000"
    # original unchanged (frozen)
    assert cur.lookup("C1").last_seen_ts == "100.000"


def test_slack_cursor_with_updated_appends_new() -> None:
    cur = SlackCursor()
    new = cur.with_updated(
        ChannelCursor(channel_id="C9", channel_name="new", last_seen_ts="42.000")
    )
    assert len(new.channels) == 1
    assert new.lookup("C9").last_seen_ts == "42.000"


def test_channel_cursor_is_frozen() -> None:
    ch = ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="1.0")
    with pytest.raises(Exception):
        ch.last_seen_ts = "2.0"  # type: ignore[misc]
```

- [ ] **Step 2: Run failing tests**

```bash
uv run pytest tests/ingest/sources/test_slack_models.py -v
```

Expected: `ModuleNotFoundError: No module named 'work_assistant.ingest.sources.slack'`.

- [ ] **Step 3: Implement cursor types**

Create `src/work_assistant/ingest/sources/slack.py`:

```python
"""Slack source implementation.

Cron-driven incremental pull. Per-channel cursor stored in `ingest_cursors`
under `source='slack'`. Per-source plans were promised cursor-shape parsing
in the Phase 1 scaffold; this module reads its own cursor row via
`ctx.db` rather than relying on the runner's deferred parsing.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from work_assistant.ingest.models import Cursor


class ChannelCursor(BaseModel):
    """Per-channel high-water-mark for incremental pulls.

    `last_seen_ts` is a Slack ts string (e.g. `"1717420800.123456"`); pass it
    as the `oldest` argument to `conversations.history` to fetch newer messages.
    """

    model_config = ConfigDict(frozen=True)

    channel_id: str
    channel_name: str
    last_seen_ts: str


class SlackCursor(Cursor):
    """Frozen pydantic cursor for the Slack source.

    `channels` is a list (not a dict) so JSON dumps stay self-describing.
    Lookups are O(N) but N == joined-channel-count which is small.
    """

    channels: list[ChannelCursor] = Field(default_factory=list)

    def lookup(self, channel_id: str) -> ChannelCursor | None:
        for ch in self.channels:
            if ch.channel_id == channel_id:
                return ch
        return None

    def with_updated(self, ch: ChannelCursor) -> "SlackCursor":
        """Return a new SlackCursor with `ch` replacing or appended."""
        replaced = False
        new_channels: list[ChannelCursor] = []
        for existing in self.channels:
            if existing.channel_id == ch.channel_id:
                new_channels.append(ch)
                replaced = True
            else:
                new_channels.append(existing)
        if not replaced:
            new_channels.append(ch)
        return SlackCursor(channels=new_channels)
```

- [ ] **Step 4: Run tests pass**

```bash
uv run pytest tests/ingest/sources/test_slack_models.py -v
uv run ruff check src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_models.py
uv run ruff format --check src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_models.py
```

Expected: 7 passed, lint + format clean.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_models.py
git commit -m "feat(slack): add SlackCursor and ChannelCursor"
```

---

## Task 3: Slack domain types + MCP request/response models

**Files:**
- Modify: `src/work_assistant/ingest/sources/slack.py` (append)
- Test: `tests/ingest/sources/test_slack_models.py` (append)

This task adds the typed structs (`SlackChannel`, `SlackMessage`, `SlackUser`) and the per-tool `MCPRequest`/`MCPResponse` pairs.

- [ ] **Step 1: Append failing tests for MCP models**

Append to `tests/ingest/sources/test_slack_models.py`:

```python


from work_assistant.ingest.sources.slack import (
    AuthTestRequest,
    AuthTestResponse,
    ConversationsHistoryRequest,
    ConversationsHistoryResponse,
    ConversationsListRequest,
    ConversationsListResponse,
    ConversationsRepliesRequest,
    ConversationsRepliesResponse,
    GetPermalinkRequest,
    GetPermalinkResponse,
    SlackChannel,
    SlackMessage,
    SlackUser,
    UsersInfoRequest,
    UsersInfoResponse,
)


def test_slack_channel_parses() -> None:
    ch = SlackChannel(
        id="C1", name="general", is_member=True, is_archived=False,
        is_im=False, is_mpim=False,
    )
    assert ch.id == "C1"
    assert ch.is_member is True


def test_slack_message_parses_with_optional_fields() -> None:
    msg = SlackMessage(
        ts="100.000", user="U1", text="hi", thread_ts=None, subtype=None,
    )
    assert msg.ts == "100.000"
    assert msg.thread_ts is None


def test_slack_user_parses() -> None:
    user = SlackUser(id="U1", name="alice", real_name="Alice", email="alice@example.com")
    assert user.email == "alice@example.com"


def test_conversations_list_request_tool_name() -> None:
    assert ConversationsListRequest.tool_name == "conversations_list"


def test_conversations_history_request_serializes_args() -> None:
    req = ConversationsHistoryRequest(channel="C1", oldest="100.000", limit=200)
    assert req.tool_name == "conversations_history"
    args = req.model_dump()
    assert args == {"channel": "C1", "oldest": "100.000", "limit": 200}


def test_conversations_history_response_round_trip() -> None:
    payload = """{
        "messages": [{"ts": "100.000", "user": "U1", "text": "hi"}],
        "has_more": false,
        "response_metadata": null
    }"""
    resp = ConversationsHistoryResponse.model_validate_json(payload)
    assert len(resp.messages) == 1
    assert resp.messages[0].ts == "100.000"
    assert resp.has_more is False


def test_conversations_replies_request_tool_name() -> None:
    assert ConversationsRepliesRequest.tool_name == "conversations_replies"


def test_users_info_request_tool_name() -> None:
    assert UsersInfoRequest.tool_name == "users_info"


def test_get_permalink_request_tool_name() -> None:
    assert GetPermalinkRequest.tool_name == "chat_get_permalink"


def test_auth_test_request_tool_name() -> None:
    assert AuthTestRequest.tool_name == "auth_test"


def test_users_info_response_round_trip() -> None:
    payload = '{"user": {"id": "U1", "name": "alice", "real_name": "Alice", "email": "a@b.com"}}'
    resp = UsersInfoResponse.model_validate_json(payload)
    assert resp.user.email == "a@b.com"
```

- [ ] **Step 2: Run failing tests**

```bash
uv run pytest tests/ingest/sources/test_slack_models.py -v
```

Expected: ImportError on the new symbols.

- [ ] **Step 3: Append MCP models to `slack.py`**

Append to `src/work_assistant/ingest/sources/slack.py`:

```python


from typing import ClassVar

from work_assistant.mcp.client import MCPRequest, MCPResponse


class SlackChannel(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    is_member: bool = False
    is_archived: bool = False
    is_im: bool = False
    is_mpim: bool = False


class SlackMessage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    ts: str
    user: str | None = None
    text: str = ""
    thread_ts: str | None = None
    subtype: str | None = None


class SlackUser(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    id: str
    name: str
    real_name: str | None = None
    email: str | None = None


class _ResponseMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    next_cursor: str | None = None


# --- conversations.list ---


class ConversationsListRequest(MCPRequest):
    tool_name: ClassVar[str] = "conversations_list"

    types: str = "public_channel,private_channel,im,mpim"
    limit: int = 1000
    exclude_archived: bool = True


class ConversationsListResponse(MCPResponse):
    channels: list[SlackChannel]
    response_metadata: _ResponseMetadata | None = None


# --- conversations.history ---


class ConversationsHistoryRequest(MCPRequest):
    tool_name: ClassVar[str] = "conversations_history"

    channel: str
    oldest: str
    limit: int = 200


class ConversationsHistoryResponse(MCPResponse):
    messages: list[SlackMessage]
    has_more: bool = False
    response_metadata: _ResponseMetadata | None = None


# --- conversations.replies ---


class ConversationsRepliesRequest(MCPRequest):
    tool_name: ClassVar[str] = "conversations_replies"

    channel: str
    ts: str
    limit: int = 200


class ConversationsRepliesResponse(MCPResponse):
    messages: list[SlackMessage]
    has_more: bool = False


# --- users.info ---


class UsersInfoRequest(MCPRequest):
    tool_name: ClassVar[str] = "users_info"

    user: str


class UsersInfoResponse(MCPResponse):
    user: SlackUser


# --- chat.getPermalink ---


class GetPermalinkRequest(MCPRequest):
    tool_name: ClassVar[str] = "chat_get_permalink"

    channel: str
    message_ts: str


class GetPermalinkResponse(MCPResponse):
    permalink: str


# --- auth.test ---


class AuthTestRequest(MCPRequest):
    tool_name: ClassVar[str] = "auth_test"


class AuthTestResponse(MCPResponse):
    user_id: str
    team_id: str
```

- [ ] **Step 4: Run tests pass**

```bash
uv run pytest tests/ingest/sources/test_slack_models.py -v
uv run ruff check src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_models.py
uv run ruff format --check src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_models.py
```

Expected: all pass; lint + format clean.

If `ruff format` reformats anything, run `uv run ruff format src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_models.py` and stage the result.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_models.py
git commit -m "feat(slack): add domain types and MCP request/response models"
```

---

## Task 4: `_SlackUserCache`

**Files:**
- Modify: `src/work_assistant/ingest/sources/slack.py` (append)
- Test: `tests/ingest/sources/test_slack_user_cache.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/sources/test_slack_user_cache.py`:

```python
"""Tests for the in-DB Slack user cache."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from work_assistant.ingest.context import SqliteDbFactory
from work_assistant.ingest.sources.slack import SlackUser, _SlackUserCache
from tests.ingest.fakes import FakeClock


def _cache(initialized_db: Path, *, now: datetime | None = None) -> _SlackUserCache:
    clock = FakeClock(now or datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC))
    db = SqliteDbFactory(db_path=initialized_db)
    return _SlackUserCache(db=db, clock=clock)


def test_get_returns_none_for_missing_user(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    assert cache.get("U_MISSING") is None


def test_upsert_then_get_returns_user(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    user = SlackUser(id="U1", name="alice", real_name="Alice", email="alice@example.com")
    cache.upsert(user, fetched_at=cache._clock.now_unix())
    found = cache.get("U1")
    assert found is not None
    assert found.email == "alice@example.com"
    assert found.name == "alice"


def test_get_returns_none_for_stale_entry(initialized_db: Path) -> None:
    """Stored entry older than 7 days should be treated as missing."""
    cache = _cache(initialized_db)
    user = SlackUser(id="U1", name="alice", real_name=None, email=None)
    eight_days_ago = cache._clock.now_unix() - 8 * 86400
    cache.upsert(user, fetched_at=eight_days_ago)
    assert cache.get("U1") is None


def test_upsert_replaces_existing_row(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    cache.upsert(
        SlackUser(id="U1", name="alice", real_name="Alice", email=None),
        fetched_at=cache._clock.now_unix(),
    )
    cache.upsert(
        SlackUser(id="U1", name="alice", real_name="Alice Smith", email="alice@example.com"),
        fetched_at=cache._clock.now_unix(),
    )
    found = cache.get("U1")
    assert found is not None
    assert found.real_name == "Alice Smith"
    assert found.email == "alice@example.com"
```

- [ ] **Step 2: Run failing tests**

```bash
uv run pytest tests/ingest/sources/test_slack_user_cache.py -v
```

Expected: ImportError on `_SlackUserCache`.

- [ ] **Step 3: Append `_SlackUserCache` to `slack.py`**

Append to `src/work_assistant/ingest/sources/slack.py`:

```python


from work_assistant.ingest.clock import Clock
from work_assistant.ingest.context import DbFactory

USER_CACHE_TTL_SECONDS = 7 * 86400


class _SlackUserCache:
    """SQLite-backed cache of `users.info` results.

    Wraps the `slack_users` table from migration 0002. Returns `None` for
    rows older than `USER_CACHE_TTL_SECONDS` so callers refetch.
    """

    def __init__(self, db: DbFactory, clock: Clock) -> None:
        self._db = db
        self._clock = clock

    def get(self, user_id: str) -> SlackUser | None:
        with self._db.open() as conn:
            row = conn.execute(
                "SELECT user_id, email, display_name, fetched_at "
                "FROM slack_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        if row is None:
            return None
        if self.is_stale(row["fetched_at"]):
            return None
        return SlackUser(
            id=row["user_id"],
            name=row["display_name"],
            real_name=None,
            email=row["email"],
        )

    def upsert(self, user: SlackUser, fetched_at: int) -> None:
        with self._db.open() as conn:
            conn.execute(
                "INSERT INTO slack_users(user_id, email, display_name, fetched_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET "
                " email = excluded.email,"
                " display_name = excluded.display_name,"
                " fetched_at = excluded.fetched_at",
                (user.id, user.email, user.real_name or user.name, fetched_at),
            )

    def is_stale(self, fetched_at: int) -> bool:
        return (self._clock.now_unix() - fetched_at) > USER_CACHE_TTL_SECONDS
```

Note: `_SlackUserCache.get` flattens `SlackUser` so the returned `name` is what was stored as `display_name` (real_name preferred, falling back to handle). `real_name` set to `None` since we don't store it separately. This is a deliberate simplification — the only consumer of the cache is mention rewriting (uses display name) and actor resolution (uses email). If `real_name` is needed later, add a column.

- [ ] **Step 4: Run tests pass**

```bash
uv run pytest tests/ingest/sources/test_slack_user_cache.py -v
uv run ruff check src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_user_cache.py
uv run ruff format --check src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_user_cache.py
```

Expected: 4 passed, lint + format clean.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_user_cache.py
git commit -m "feat(slack): add _SlackUserCache with 7-day TTL"
```

---

## Task 5: `_thread_eligible` + `_normalize_message` helpers + body normalization

**Files:**
- Modify: `src/work_assistant/ingest/sources/slack.py` (append)
- Test: `tests/ingest/sources/test_slack_helpers.py` (NEW)

These two helpers are the deterministic transform layer. Test them before wiring the source.

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/sources/test_slack_helpers.py`:

```python
"""Tests for _thread_eligible and _normalize_message helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from work_assistant.ingest.context import SqliteDbFactory
from work_assistant.ingest.sources.slack import (
    SlackChannel,
    SlackMessage,
    SlackUser,
    _normalize_message,
    _SlackUserCache,
    _thread_eligible,
)
from tests.ingest.fakes import FakeClock


def _cache(initialized_db: Path) -> _SlackUserCache:
    clock = FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC))
    return _SlackUserCache(db=SqliteDbFactory(db_path=initialized_db), clock=clock)


# --- _thread_eligible ---


def test_thread_eligible_top_level_authored_by_user() -> None:
    top = SlackMessage(ts="100.000", user="U_OWN", text="hello")
    assert _thread_eligible(top, replies=None, own_user_id="U_OWN") is True


def test_thread_eligible_user_mentioned_in_top_level() -> None:
    top = SlackMessage(ts="100.000", user="U_OTHER", text="hey <@U_OWN> thoughts?")
    assert _thread_eligible(top, replies=None, own_user_id="U_OWN") is True


def test_thread_not_eligible_when_user_absent() -> None:
    top = SlackMessage(ts="100.000", user="U_OTHER", text="lunch?")
    assert _thread_eligible(top, replies=None, own_user_id="U_OWN") is False


def test_thread_eligible_user_authored_a_reply() -> None:
    top = SlackMessage(ts="100.000", user="U_OTHER", text="anyone?")
    replies = [
        SlackMessage(ts="100.000", user="U_OTHER", text="anyone?"),
        SlackMessage(ts="101.000", user="U_OWN", text="here"),
    ]
    assert _thread_eligible(top, replies=replies, own_user_id="U_OWN") is True


def test_thread_eligible_user_mentioned_in_a_reply() -> None:
    top = SlackMessage(ts="100.000", user="U_OTHER", text="anyone?")
    replies = [
        SlackMessage(ts="100.000", user="U_OTHER", text="anyone?"),
        SlackMessage(ts="101.000", user="U_3RD", text="<@U_OWN> what about you"),
    ]
    assert _thread_eligible(top, replies=replies, own_user_id="U_OWN") is True


# --- _normalize_message ---


def test_normalize_message_builds_event(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    channel = SlackChannel(
        id="C1", name="general", is_member=True, is_archived=False,
        is_im=False, is_mpim=False,
    )
    msg = SlackMessage(ts="100.123", user="U_AUTHOR", text="hello world")
    event = _normalize_message(
        msg=msg, channel=channel, cache=cache, own_user_id="U_OWN", source_name="slack",
    )
    assert event.source == "slack"
    assert event.source_id == "C1:100.123"
    assert event.kind == "message"
    assert event.body == "hello world"
    assert event.body_truncated is False
    assert event.thread_key == "100.123"  # not threaded -> ts
    assert event.actor == "U_AUTHOR"  # cache miss -> raw user_id
    assert event.metadata.kind == "slack"
    assert event.metadata.channel_id == "C1"
    assert event.metadata.channel_name == "general"
    assert event.metadata.is_mention is False
    assert event.occurred_at == 100  # int(float(ts))


def test_normalize_message_threaded_uses_thread_ts(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    channel = SlackChannel(id="C1", name="general", is_member=True, is_archived=False,
                          is_im=False, is_mpim=False)
    msg = SlackMessage(ts="200.000", user="U1", text="reply text", thread_ts="100.000")
    event = _normalize_message(msg=msg, channel=channel, cache=cache, own_user_id="U_OWN",
                               source_name="slack")
    assert event.thread_key == "100.000"


def test_normalize_message_truncates_long_body(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    channel = SlackChannel(id="C1", name="general", is_member=True, is_archived=False,
                          is_im=False, is_mpim=False)
    long_text = "a" * 200_000
    msg = SlackMessage(ts="100.000", user="U1", text=long_text)
    event = _normalize_message(msg=msg, channel=channel, cache=cache, own_user_id="U_OWN",
                               source_name="slack")
    assert len(event.body) <= 100_000
    assert event.body_truncated is True


def test_normalize_message_rewrites_mention_using_cache(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    cache.upsert(
        SlackUser(id="U_BOB", name="bob", real_name="Bob Roberts", email="bob@example.com"),
        fetched_at=cache._clock.now_unix(),
    )
    channel = SlackChannel(id="C1", name="general", is_member=True, is_archived=False,
                          is_im=False, is_mpim=False)
    msg = SlackMessage(ts="100.000", user="U1", text="hey <@U_BOB> ping")
    event = _normalize_message(msg=msg, channel=channel, cache=cache, own_user_id="U_OWN",
                               source_name="slack")
    assert "@Bob Roberts" in event.body or "@bob" in event.body
    assert "<@U_BOB>" not in event.body


def test_normalize_message_keeps_unknown_mention_literal(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    channel = SlackChannel(id="C1", name="general", is_member=True, is_archived=False,
                          is_im=False, is_mpim=False)
    msg = SlackMessage(ts="100.000", user="U1", text="hey <@U_UNKNOWN>")
    event = _normalize_message(msg=msg, channel=channel, cache=cache, own_user_id="U_OWN",
                               source_name="slack")
    assert "<@U_UNKNOWN>" in event.body


def test_normalize_message_marks_mention_metadata(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    channel = SlackChannel(id="C1", name="general", is_member=True, is_archived=False,
                          is_im=False, is_mpim=False)
    msg = SlackMessage(ts="100.000", user="U1", text="hey <@U_OWN> question")
    event = _normalize_message(msg=msg, channel=channel, cache=cache, own_user_id="U_OWN",
                               source_name="slack")
    assert event.metadata.is_mention is True
```

- [ ] **Step 2: Run failing tests**

```bash
uv run pytest tests/ingest/sources/test_slack_helpers.py -v
```

Expected: ImportError on `_thread_eligible` and `_normalize_message`.

- [ ] **Step 3: Append helpers to `slack.py`**

Append to `src/work_assistant/ingest/sources/slack.py`:

```python


import hashlib
import re

from work_assistant.ingest.models import NormalizedEvent, SlackMetadata, SourceName

_MENTION_RE = re.compile(r"<@([A-Z0-9]+)>")
BODY_MAX_BYTES = 100_000


def _thread_eligible(
    top: SlackMessage,
    *,
    replies: list[SlackMessage] | None,
    own_user_id: str,
) -> bool:
    """Return True iff the user authored or was @-mentioned at any depth."""
    own_marker = f"<@{own_user_id}>"
    if top.user == own_user_id:
        return True
    if own_marker in top.text:
        return True
    if replies is None:
        return False
    for reply in replies:
        if reply.user == own_user_id:
            return True
        if own_marker in reply.text:
            return True
    return False


def _rewrite_mentions(text: str, cache: _SlackUserCache) -> str:
    def _replace(match: re.Match[str]) -> str:
        user_id = match.group(1)
        cached = cache.get(user_id)
        if cached is None:
            return match.group(0)  # leave literal
        return f"@{cached.name}"

    return _MENTION_RE.sub(_replace, text)


def _truncate_body(body: str) -> tuple[str, bool]:
    """Return `(body, truncated)`. UTF-8 byte-bounded to avoid mid-codepoint cut."""
    encoded = body.encode("utf-8")
    if len(encoded) <= BODY_MAX_BYTES:
        return body, False
    truncated_bytes = encoded[:BODY_MAX_BYTES]
    # Drop trailing incomplete codepoint (cheap: decode with errors='ignore').
    return truncated_bytes.decode("utf-8", errors="ignore"), True


def _normalize_message(
    *,
    msg: SlackMessage,
    channel: SlackChannel,
    cache: _SlackUserCache,
    own_user_id: str,
    source_name: SourceName,
) -> NormalizedEvent:
    """Build a NormalizedEvent from a Slack message + channel + actor cache."""
    rewritten = _rewrite_mentions(msg.text, cache)
    body, truncated = _truncate_body(rewritten)
    own_marker = f"<@{own_user_id}>"
    is_mention = own_marker in msg.text
    thread_key = msg.thread_ts or msg.ts
    source_id = f"{channel.id}:{msg.ts}"
    occurred_at = int(float(msg.ts))
    payload = f"{source_name}:{source_id}:{body}".encode()
    content_hash = hashlib.sha256(payload).hexdigest()

    metadata = SlackMetadata(
        channel_id=channel.id,
        channel_name=channel.name,
        is_im=channel.is_im,
        is_mpim=channel.is_mpim,
        is_dm=channel.is_im,
        is_mention=is_mention,
        reactions_json="[]",
        files_json="[]",
    )

    return NormalizedEvent(
        source=source_name,
        source_id=source_id,
        source_link=None,  # permalink fetched lazily for mentions/DMs in fetch()
        content_hash=content_hash,
        occurred_at=occurred_at,
        actor=msg.user,  # cache lookup for email happens in resolve_actor; raw user_id here
        thread_key=thread_key,
        kind="message",
        title=body[:80] if body else None,
        body=body,
        body_truncated=truncated,
        metadata=metadata,
    )
```

- [ ] **Step 4: Run tests pass**

```bash
uv run pytest tests/ingest/sources/test_slack_helpers.py -v
uv run ruff check src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_helpers.py
uv run ruff format --check src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_helpers.py
```

Expected: 10 passed, lint + format clean.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_helpers.py
git commit -m "feat(slack): add _thread_eligible, _normalize_message, body truncation"
```

---

## Task 6: `SlackSource.fetch` + happy-path tests

**Files:**
- Modify: `src/work_assistant/ingest/sources/slack.py` (append `SlackSource`)
- Modify: `src/work_assistant/ingest/sources/__init__.py` (import slack to register)
- Test: `tests/ingest/sources/test_slack_source.py`

This is the largest task. `SlackSource.fetch` is an async generator that yields one Batch per channel.

- [ ] **Step 1: Add a Slack-aware FakeMCPClient harness helper**

Create `tests/ingest/sources/_helpers.py` (test-internal — kept out of `tests/ingest/fakes.py` to avoid circular import: `fakes.py` is imported widely; importing slack from there couples every test to the slack module):

```python
"""Test helpers for Slack source tests. Not for use outside tests/ingest/sources/."""

from __future__ import annotations

from tests.ingest.fakes import ScriptedReply
from work_assistant.ingest.sources.slack import (
    AuthTestResponse,
    ConversationsHistoryResponse,
    ConversationsListResponse,
    ConversationsRepliesResponse,
    UsersInfoResponse,
)


def slack_script(
    *,
    own_user_id: str = "U_OWN",
    team_id: str = "T_TEAM",
    list_channels: list[ConversationsListResponse] | None = None,
    histories: dict[str, list[ConversationsHistoryResponse]] | None = None,
    replies: dict[str, list[ConversationsRepliesResponse]] | None = None,
    users: dict[str, list[UsersInfoResponse]] | None = None,
) -> dict[str, list[ScriptedReply]]:
    """Build a FakeMCPClient script for typical Slack flows.

    Keys map to MCPRequest subclass __name__. Values are scripted replies
    consumed in order. Use FakeMCPClient(script=slack_script(...)) directly.
    """
    script: dict[str, list[ScriptedReply]] = {}
    script["AuthTestRequest"] = [
        ScriptedReply(response=AuthTestResponse(user_id=own_user_id, team_id=team_id)),
    ]
    script["ConversationsListRequest"] = [
        ScriptedReply(response=r) for r in (list_channels or [])
    ]
    if histories:
        # ConversationsHistoryRequest is keyed by request class name only;
        # FakeMCPClient pops them in invocation order, so order the values
        # by the order channels appear in the test scenario.
        script["ConversationsHistoryRequest"] = [
            ScriptedReply(response=r) for chan_id, replies_list in histories.items()
            for r in replies_list
        ]
    if replies:
        script["ConversationsRepliesRequest"] = [
            ScriptedReply(response=r) for thread_ts, replies_list in replies.items()
            for r in replies_list
        ]
    if users:
        script["UsersInfoRequest"] = [
            ScriptedReply(response=r) for u_id, replies_list in users.items()
            for r in replies_list
        ]
    return script
```

This helper centralizes scripted-reply construction across the source tests.

- [ ] **Step 2: Write the failing tests**

Create `tests/ingest/sources/test_slack_source.py`:

```python
"""Tests for SlackSource.fetch and cursor_from_timestamp."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
import structlog

from work_assistant.ingest.context import IngestContext, SqliteDbFactory
from work_assistant.ingest.sources.slack import (
    ConversationsHistoryResponse,
    ConversationsListResponse,
    ConversationsRepliesResponse,
    SlackChannel,
    SlackCursor,
    SlackMessage,
    SlackSource,
)
from tests.ingest.fakes import FakeClock, FakeMCPClient, ScriptedReply
from tests.ingest.sources._helpers import slack_script


def _ctx(initialized_db: Path, mcp: FakeMCPClient) -> IngestContext:
    return IngestContext(
        db=SqliteDbFactory(db_path=initialized_db),
        mcp=mcp,
        logger=structlog.get_logger("test"),
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)),
    )


def _channel(channel_id: str, name: str, *, is_member: bool = True, is_archived: bool = False,
             is_im: bool = False, is_mpim: bool = False) -> SlackChannel:
    return SlackChannel(
        id=channel_id, name=name, is_member=is_member, is_archived=is_archived,
        is_im=is_im, is_mpim=is_mpim,
    )


def _msg(ts: str, user: str = "U_OTHER", text: str = "hi", thread_ts: str | None = None) -> SlackMessage:
    return SlackMessage(ts=ts, user=user, text=text, thread_ts=thread_ts)


# --- happy path ---


@pytest.mark.asyncio
async def test_fetch_two_channels_each_three_messages(initialized_db: Path) -> None:
    mcp = FakeMCPClient(script=slack_script(
        list_channels=[ConversationsListResponse(channels=[_channel("C1", "general"), _channel("C2", "random")])],
        histories={
            "C1": [ConversationsHistoryResponse(
                messages=[_msg("100.000"), _msg("101.000"), _msg("102.000")],
                has_more=False,
            )],
            "C2": [ConversationsHistoryResponse(
                messages=[_msg("200.000"), _msg("201.000"), _msg("202.000")],
                has_more=False,
            )],
        },
    ))
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    assert len(batches) == 2
    assert all(len(b.events) == 3 for b in batches)
    # Final batch's cursor reflects both channels.
    final = batches[-1].next_cursor
    assert isinstance(final, SlackCursor)
    assert final.lookup("C1") is not None
    assert final.lookup("C2") is not None
    assert final.lookup("C1").last_seen_ts == "102.000"
    assert final.lookup("C2").last_seen_ts == "202.000"


@pytest.mark.asyncio
async def test_fetch_skips_archived_and_non_member_channels(initialized_db: Path) -> None:
    channels = [
        _channel("C_OK", "general"),
        _channel("C_ARCHIVED", "old", is_archived=True),
        _channel("C_NOT_MEMBER", "other", is_member=False),
    ]
    mcp = FakeMCPClient(script=slack_script(
        list_channels=[ConversationsListResponse(channels=channels)],
        histories={
            "C_OK": [ConversationsHistoryResponse(messages=[_msg("100.000")], has_more=False)],
        },
    ))
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    assert len(batches) == 1
    assert batches[0].events[0].metadata.channel_id == "C_OK"


@pytest.mark.asyncio
async def test_fetch_empty_channel_yields_zero_event_batch(initialized_db: Path) -> None:
    mcp = FakeMCPClient(script=slack_script(
        list_channels=[ConversationsListResponse(channels=[_channel("C1", "general")])],
        histories={"C1": [ConversationsHistoryResponse(messages=[], has_more=False)]},
    ))
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    assert len(batches) == 1
    assert batches[0].events == []
    # Cursor unchanged for empty channel (no last_seen_ts to advance).
    assert batches[0].next_cursor.lookup("C1") is None or \
        batches[0].next_cursor.lookup("C1").last_seen_ts != ""


# --- threading ---


@pytest.mark.asyncio
async def test_fetch_eligible_thread_pulls_replies(initialized_db: Path) -> None:
    top = _msg("100.000", user="U_OWN", text="anyone?", thread_ts="100.000")
    reply = _msg("101.000", user="U_OTHER", text="me", thread_ts="100.000")
    mcp = FakeMCPClient(script=slack_script(
        list_channels=[ConversationsListResponse(channels=[_channel("C1", "general")])],
        histories={"C1": [ConversationsHistoryResponse(messages=[top], has_more=False)]},
        replies={"100.000": [ConversationsRepliesResponse(messages=[top, reply])]},
    ))
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    # Top + 1 reply (parent dedup'd from replies list).
    assert len(batches[0].events) == 2


@pytest.mark.asyncio
async def test_fetch_ineligible_thread_does_not_call_replies(initialized_db: Path) -> None:
    top = _msg("100.000", user="U_OTHER", text="random", thread_ts="100.000")
    mcp = FakeMCPClient(script=slack_script(
        list_channels=[ConversationsListResponse(channels=[_channel("C1", "general")])],
        histories={"C1": [ConversationsHistoryResponse(messages=[top], has_more=False)]},
    ))
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    assert len(batches[0].events) == 1
    # Verify replies tool was never called.
    replies_calls = [c for c in mcp.calls if type(c.request).__name__ == "ConversationsRepliesRequest"]
    assert replies_calls == []


# --- cursor seeding ---


@pytest.mark.asyncio
async def test_fetch_new_channel_seeds_from_backfill_window(initialized_db: Path) -> None:
    """New channel not in cursor: seeded at clock.now_unix() - 30d * 86400."""
    mcp = FakeMCPClient(script=slack_script(
        list_channels=[ConversationsListResponse(channels=[_channel("C_NEW", "fresh")])],
        histories={"C_NEW": [ConversationsHistoryResponse(messages=[_msg("100.000")], has_more=False)]},
    ))
    src = SlackSource(_ctx(initialized_db, mcp))
    [b async for b in src.fetch(SlackCursor())]
    history_call = next(
        c for c in mcp.calls if type(c.request).__name__ == "ConversationsHistoryRequest"
    )
    expected_oldest = src.ctx.clock.now_unix() - 30 * 86400
    assert history_call.request.oldest == str(expected_oldest)


@pytest.mark.asyncio
async def test_cursor_from_timestamp_returns_empty_cursor(initialized_db: Path) -> None:
    """Per spec §4.4: synthesized cursor is empty; fetch() seeds at runtime."""
    mcp = FakeMCPClient(script={})
    src = SlackSource(_ctx(initialized_db, mcp))
    cursor = src.cursor_from_timestamp(1_700_000_000)
    assert isinstance(cursor, SlackCursor)
    assert cursor.channels == []


@pytest.mark.asyncio
async def test_fetch_resumes_from_existing_channel_cursor(initialized_db: Path) -> None:
    """If cursor has C1 at ts=500, fetch should pass oldest=500."""
    mcp = FakeMCPClient(script=slack_script(
        list_channels=[ConversationsListResponse(channels=[_channel("C1", "general")])],
        histories={"C1": [ConversationsHistoryResponse(messages=[_msg("600.000")], has_more=False)]},
    ))
    src = SlackSource(_ctx(initialized_db, mcp))
    cursor = SlackCursor(channels=[
        # Use the cursor accessor: external code would build this by name.
        # Inline construction via the public type.
    ])
    from work_assistant.ingest.sources.slack import ChannelCursor
    cursor = SlackCursor(channels=[ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="500.000")])
    [b async for b in src.fetch(cursor)]
    history_call = next(c for c in mcp.calls if type(c.request).__name__ == "ConversationsHistoryRequest")
    assert history_call.request.oldest == "500.000"
```

- [ ] **Step 3: Run failing tests**

```bash
uv run pytest tests/ingest/sources/test_slack_source.py -v
```

Expected: ImportError on `SlackSource`.

- [ ] **Step 4: Append `SlackSource` to `slack.py`**

Append to `src/work_assistant/ingest/sources/slack.py`:

```python


from collections.abc import AsyncIterator
from typing import ClassVar as _ClassVar

from work_assistant.ingest.models import Batch
from work_assistant.ingest.source import Source

BACKFILL_DAYS_DEFAULT = 30
PER_CHANNEL_LIMIT = 200


class SlackSource(Source):
    """Cron-driven Slack ingest. See docs/superpowers/specs/2026-06-05-slack-source-design.md."""

    name: _ClassVar[str] = "slack"
    mcp_server: _ClassVar[str] = "slack"

    def cursor_from_timestamp(self, ts: int) -> SlackCursor:
        """Per spec §4.4: returns empty SlackCursor; fetch() seeds each channel.

        We can't enumerate channels here (that requires an async MCP call and
        this method is sync per the Source ABC). Instead, the fetch loop
        treats cursor.lookup(channel_id) is None as "seed at backfill window
        OR caller-provided since_unix"; the worker is responsible for
        plumbing since_unix into fetch's seed-window calculation.
        """
        return SlackCursor()

    def normalize_body(self, raw: str) -> tuple[str, bool]:
        return _truncate_body(raw)

    async def resolve_actor(self, raw_actor: str) -> str | None:
        cache = _SlackUserCache(db=self.ctx.db, clock=self.ctx.clock)
        cached = cache.get(raw_actor)
        if cached is not None:
            return cached.email or cached.name
        # Cache miss: fetch + cache + return
        try:
            resp = await self.ctx.mcp.call(
                UsersInfoRequest(user=raw_actor), UsersInfoResponse
            )
        except Exception:
            return None
        cache.upsert(resp.user, fetched_at=self.ctx.clock.now_unix())
        return resp.user.email or resp.user.name

    async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
        slack_cursor = self._load_or_init_cursor(cursor)
        cache = _SlackUserCache(db=self.ctx.db, clock=self.ctx.clock)
        own_user_id = await self._resolve_own_user_id()

        list_resp = await self.ctx.mcp.call(
            ConversationsListRequest(), ConversationsListResponse
        )
        for channel in list_resp.channels:
            if channel.is_archived or not channel.is_member:
                continue
            ch_cursor = slack_cursor.lookup(channel.id)
            if ch_cursor is None:
                seed_ts = self.ctx.clock.now_unix() - BACKFILL_DAYS_DEFAULT * 86400
                ch_cursor = ChannelCursor(
                    channel_id=channel.id,
                    channel_name=channel.name,
                    last_seen_ts=str(seed_ts),
                )

            history = await self.ctx.mcp.call(
                ConversationsHistoryRequest(
                    channel=channel.id,
                    oldest=ch_cursor.last_seen_ts,
                    limit=PER_CHANNEL_LIMIT,
                ),
                ConversationsHistoryResponse,
            )

            events: list[NormalizedEvent] = []
            for msg in history.messages:
                events.append(_normalize_message(
                    msg=msg, channel=channel, cache=cache,
                    own_user_id=own_user_id, source_name="slack",
                ))
                if msg.thread_ts and _thread_eligible(msg, replies=None, own_user_id=own_user_id):
                    replies = await self.ctx.mcp.call(
                        ConversationsRepliesRequest(channel=channel.id, ts=msg.thread_ts),
                        ConversationsRepliesResponse,
                    )
                    if not _thread_eligible(msg, replies=replies.messages, own_user_id=own_user_id):
                        continue
                    for reply in replies.messages:
                        if reply.ts == msg.thread_ts:
                            continue  # parent already added
                        events.append(_normalize_message(
                            msg=reply, channel=channel, cache=cache,
                            own_user_id=own_user_id, source_name="slack",
                        ))

            new_last_seen = (
                max(m.ts for m in history.messages) if history.messages
                else ch_cursor.last_seen_ts
            )
            updated_ch = ChannelCursor(
                channel_id=channel.id,
                channel_name=channel.name,
                last_seen_ts=new_last_seen,
            )
            slack_cursor = slack_cursor.with_updated(updated_ch)

            yield Batch(events=events, next_cursor=slack_cursor, status="ok")

    def _load_or_init_cursor(self, cursor: Cursor | None) -> SlackCursor:
        """Phase 1 scaffold passes None; we read our own row.

        See docs/superpowers/specs/2026-06-05-slack-source-design.md §4.4.
        """
        if isinstance(cursor, SlackCursor):
            return cursor
        # Read raw text from ingest_cursors directly.
        with self.ctx.db.open() as conn:
            row = conn.execute(
                "SELECT cursor FROM ingest_cursors WHERE source = 'slack'"
            ).fetchone()
        if row is None or not row["cursor"]:
            return SlackCursor()
        return SlackCursor.model_validate_json(row["cursor"])

    async def _resolve_own_user_id(self) -> str:
        resp = await self.ctx.mcp.call(AuthTestRequest(), AuthTestResponse)
        return resp.user_id
```

- [ ] **Step 5: Populate `ingest/sources/__init__.py`**

Replace `src/work_assistant/ingest/sources/__init__.py` content with:

```python
"""Source implementations.

Importing this package registers each source via side-effect.
"""

from work_assistant.ingest.registry import SOURCES
from work_assistant.ingest.sources.slack import SlackSource

SOURCES["slack"] = SlackSource
```

- [ ] **Step 6: Run tests pass**

```bash
uv run pytest tests/ingest/sources/test_slack_source.py -v
uv run ruff check src/work_assistant/ingest/sources tests/ingest/sources
uv run ruff format --check src/work_assistant/ingest/sources tests/ingest/sources
```

Expected: 8 pass; lint + format clean. If any test fails, the most likely culprit is the FakeMCPClient script ordering — ConversationsHistoryRequest replies are popped in invocation order, so the test scenario must order channels consistently with the script.

- [ ] **Step 7: Commit**

```bash
git add src/work_assistant/ingest/sources/slack.py src/work_assistant/ingest/sources/__init__.py tests/ingest/sources/test_slack_source.py tests/ingest/sources/_helpers.py
git commit -m "feat(slack): SlackSource.fetch + per-channel batches + thread fan-out"
```

---

## Task 7: Error classification

**Files:**
- Modify: `src/work_assistant/ingest/sources/slack.py` (add error mapping)
- Test: `tests/ingest/sources/test_slack_errors.py`

The MCP layer surfaces Slack errors as exceptions or `error` fields in the JSON payload. We classify them per spec §5.

- [ ] **Step 1: Write the failing tests**

Create `tests/ingest/sources/test_slack_errors.py`:

```python
"""Tests for Slack error classification."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import structlog

from work_assistant.ingest.context import IngestContext, SqliteDbFactory
from work_assistant.ingest.errors import PermanentIngestError, TransientIngestError
from work_assistant.ingest.sources.slack import (
    AuthTestResponse,
    ConversationsListResponse,
    SlackChannel,
    SlackCursor,
    SlackSource,
    SlackError,
    map_slack_error,
)
from tests.ingest.fakes import FakeClock, FakeMCPClient, ScriptedReply
from tests.ingest.sources._helpers import slack_script


def _ctx(initialized_db: Path, mcp: FakeMCPClient) -> IngestContext:
    return IngestContext(
        db=SqliteDbFactory(db_path=initialized_db),
        mcp=mcp,
        logger=structlog.get_logger("test"),
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)),
    )


def test_map_rate_limited_to_transient() -> None:
    err = map_slack_error("rate_limited")
    assert isinstance(err, TransientIngestError)


def test_map_invalid_auth_to_permanent() -> None:
    for code in ("invalid_auth", "account_inactive", "not_authed", "token_revoked"):
        err = map_slack_error(code)
        assert isinstance(err, PermanentIngestError), f"{code} should be permanent"


def test_map_unknown_error_defaults_to_transient() -> None:
    err = map_slack_error("some_unknown_error")
    assert isinstance(err, TransientIngestError)


@pytest.mark.asyncio
async def test_fetch_skips_channel_on_channel_not_found(initialized_db: Path) -> None:
    """A `channel_not_found` SlackError on one channel should drop that channel
    and continue with siblings; the run still completes 'ok'."""
    channels = [SlackChannel(id="C_OK", name="ok", is_member=True),
                SlackChannel(id="C_GONE", name="gone", is_member=True)]
    mcp = FakeMCPClient(script={
        "AuthTestRequest": [ScriptedReply(response=AuthTestResponse(user_id="U_OWN", team_id="T1"))],
        "ConversationsListRequest": [ScriptedReply(response=ConversationsListResponse(channels=channels))],
        "ConversationsHistoryRequest": [
            # First call (C_OK) succeeds; second (C_GONE) raises channel_not_found.
            ScriptedReply(raises=SlackError("channel_not_found")),
        ],
    })
    # The first ConversationsHistoryRequest succeeds; we need a Response in front of the raise.
    # Adjust by inserting the success reply.
    from work_assistant.ingest.sources.slack import ConversationsHistoryResponse, SlackMessage
    mcp = FakeMCPClient(script={
        "AuthTestRequest": [ScriptedReply(response=AuthTestResponse(user_id="U_OWN", team_id="T1"))],
        "ConversationsListRequest": [ScriptedReply(response=ConversationsListResponse(channels=channels))],
        "ConversationsHistoryRequest": [
            ScriptedReply(response=ConversationsHistoryResponse(
                messages=[SlackMessage(ts="100.000", user="U1", text="hi")],
                has_more=False,
            )),
            ScriptedReply(raises=SlackError("channel_not_found")),
        ],
    })

    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    # Only C_OK produced events; C_GONE was skipped.
    assert len(batches) == 1
    assert batches[0].events[0].metadata.channel_id == "C_OK"


@pytest.mark.asyncio
async def test_fetch_raises_transient_on_rate_limited(initialized_db: Path) -> None:
    """rate_limited from any non-skippable call propagates as TransientIngestError."""
    channels = [SlackChannel(id="C1", name="general", is_member=True)]
    mcp = FakeMCPClient(script={
        "AuthTestRequest": [ScriptedReply(response=AuthTestResponse(user_id="U_OWN", team_id="T1"))],
        "ConversationsListRequest": [ScriptedReply(raises=SlackError("rate_limited"))],
    })
    src = SlackSource(_ctx(initialized_db, mcp))
    with pytest.raises(TransientIngestError):
        [b async for b in src.fetch(SlackCursor())]


@pytest.mark.asyncio
async def test_fetch_raises_permanent_on_invalid_auth(initialized_db: Path) -> None:
    mcp = FakeMCPClient(script={
        "AuthTestRequest": [ScriptedReply(raises=SlackError("invalid_auth"))],
    })
    src = SlackSource(_ctx(initialized_db, mcp))
    with pytest.raises(PermanentIngestError):
        [b async for b in src.fetch(SlackCursor())]
```

- [ ] **Step 2: Run failing tests**

Expected: ImportError on `SlackError` and `map_slack_error`.

- [ ] **Step 3: Append error mapping to `slack.py`**

Append to `src/work_assistant/ingest/sources/slack.py`:

```python


from work_assistant.ingest.errors import (
    IngestError,
    PermanentIngestError,
    TransientIngestError,
)

_PERMANENT_SLACK_ERRORS = frozenset({
    "invalid_auth",
    "account_inactive",
    "not_authed",
    "token_revoked",
})

_SKIPPABLE_PER_CHANNEL_ERRORS = frozenset({
    "channel_not_found",
    "not_in_channel",
})


class SlackError(Exception):
    """Raised by the MCP layer (or test fakes) carrying a Slack-style error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def map_slack_error(code: str) -> IngestError:
    """Map Slack error codes to our ingest exception hierarchy."""
    if code in _PERMANENT_SLACK_ERRORS:
        return PermanentIngestError(f"slack auth: {code}")
    if code == "rate_limited":
        return TransientIngestError(f"slack rate limited; persist cursor and exit: {code}")
    return TransientIngestError(f"slack: {code}")
```

Now wire `SlackSource.fetch` to catch `SlackError`. Update the `fetch` body so the `conversations.history` call per-channel is wrapped:

Find the inner `await self.ctx.mcp.call(ConversationsHistoryRequest(...))` block and replace with:

```python
            try:
                history = await self.ctx.mcp.call(
                    ConversationsHistoryRequest(
                        channel=channel.id,
                        oldest=ch_cursor.last_seen_ts,
                        limit=PER_CHANNEL_LIMIT,
                    ),
                    ConversationsHistoryResponse,
                )
            except SlackError as exc:
                if exc.code in _SKIPPABLE_PER_CHANNEL_ERRORS:
                    self.ctx.logger.warning(
                        "slack_channel_skipped",
                        channel_id=channel.id,
                        channel_name=channel.name,
                        code=exc.code,
                    )
                    continue
                raise map_slack_error(exc.code) from exc
```

Also wrap `_resolve_own_user_id` and the top-level `ConversationsListRequest` call so `SlackError` raised there propagates as the right `IngestError` subclass:

```python
    async def _resolve_own_user_id(self) -> str:
        try:
            resp = await self.ctx.mcp.call(AuthTestRequest(), AuthTestResponse)
        except SlackError as exc:
            raise map_slack_error(exc.code) from exc
        return resp.user_id
```

And in `fetch`:

```python
        try:
            list_resp = await self.ctx.mcp.call(
                ConversationsListRequest(), ConversationsListResponse
            )
        except SlackError as exc:
            raise map_slack_error(exc.code) from exc
```

Wrap the replies fetch with the same skip-or-raise logic (channel might disappear between history and replies):

```python
                if msg.thread_ts and _thread_eligible(msg, replies=None, own_user_id=own_user_id):
                    try:
                        replies = await self.ctx.mcp.call(
                            ConversationsRepliesRequest(channel=channel.id, ts=msg.thread_ts),
                            ConversationsRepliesResponse,
                        )
                    except SlackError as exc:
                        if exc.code in _SKIPPABLE_PER_CHANNEL_ERRORS:
                            self.ctx.logger.warning(
                                "slack_replies_skipped",
                                channel_id=channel.id, ts=msg.thread_ts, code=exc.code,
                            )
                            continue
                        raise map_slack_error(exc.code) from exc
                    if not _thread_eligible(msg, replies=replies.messages, own_user_id=own_user_id):
                        continue
                    for reply in replies.messages:
                        if reply.ts == msg.thread_ts:
                            continue
                        events.append(_normalize_message(
                            msg=reply, channel=channel, cache=cache,
                            own_user_id=own_user_id, source_name="slack",
                        ))
```

- [ ] **Step 4: Run tests pass**

```bash
uv run pytest tests/ingest/sources/test_slack_errors.py tests/ingest/sources/test_slack_source.py -v
uv run ruff check src/work_assistant/ingest/sources tests/ingest/sources
uv run ruff format --check src/work_assistant/ingest/sources tests/ingest/sources
```

Expected: error tests + happy-path tests pass; lint + format clean.

- [ ] **Step 5: Commit**

```bash
git add src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_errors.py
git commit -m "feat(slack): map slack errors to transient/permanent + skip channel-not-found"
```

---

## Task 8: Registry side-effect test

**Files:**
- Test: `tests/ingest/sources/test_slack_registry.py`

- [ ] **Step 1: Write the test**

Create `tests/ingest/sources/test_slack_registry.py`:

```python
"""Importing the sources package must register SlackSource."""

from __future__ import annotations


def test_importing_sources_registers_slack() -> None:
    # Ensure a clean SOURCES dict view: re-import is fine since registration is idempotent.
    from work_assistant.ingest import sources  # noqa: F401
    from work_assistant.ingest.registry import SOURCES
    from work_assistant.ingest.sources.slack import SlackSource
    assert SOURCES.get("slack") is SlackSource
```

- [ ] **Step 2: Run test**

```bash
uv run pytest tests/ingest/sources/test_slack_registry.py -v
```

Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/ingest/sources/test_slack_registry.py
git commit -m "test(slack): registry import side-effect registers SlackSource"
```

---

## Task 9: Worker rewires `BridgeMCPClient` per `mcp_server` group

**Files:**
- Modify: `src/work_assistant/ingest/worker.py` (drop `_NullMCPClient`; wire bridge groups)
- Modify: `src/work_assistant/ingest/cli.py` (import `sources` package so SOURCES is populated)
- Test: `tests/ingest/test_worker_slack.py` (NEW: end-to-end with FakeMCPClient injected)
- Modify: `tests/ingest/test_worker.py` (existing tests must keep working — `_NullMCPClient` test no longer applicable; replace with a stub that explicitly exercises the new path)

This task replaces the scaffold's `_NullMCPClient` with a real `BridgeMCPClient` per `mcp_server` group. Phase 1 left `IngestContext.settings=None` with a `# type: ignore`; this task drops the `# type: ignore` by passing a real `Config`.

- [ ] **Step 1: Inspect existing tests that depend on `_NullMCPClient`**

```bash
grep -n "_NullMCPClient\|NullMCPClient" tests/ src/ 2>/dev/null
```

If any test asserts `_NullMCPClient` behavior (it shouldn't — the placeholder was untested), update it. Most likely the `_DryRunDbFactory` test in `test_worker.py` is independent.

- [ ] **Step 2: Write the failing end-to-end test**

Create `tests/ingest/test_worker_slack.py`:

```python
"""End-to-end: real SlackSource registered, FakeMCPClient injected, run_worker drives it."""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from work_assistant.ingest.sources.slack import (
    AuthTestResponse,
    ConversationsHistoryResponse,
    ConversationsListResponse,
    SlackChannel,
    SlackMessage,
    SlackSource,
)
from work_assistant.ingest.worker import (
    EXIT_OK,
    WorkerOptions,
    run_worker,
)
from tests.ingest.fakes import FakeClock, FakeMCPClient, ScriptedReply


@pytest.fixture()
def slack_mcp_factory(monkeypatch: pytest.MonkeyPatch):
    """Patch the worker's bridge construction so SlackSource sees a FakeMCPClient."""
    from work_assistant.ingest import worker as worker_mod

    def fake_build_mcp_client(mcp_server: str, settings):  # type: ignore[no-untyped-def]
        if mcp_server == "slack":
            return FakeMCPClient(script={
                "AuthTestRequest": [
                    ScriptedReply(response=AuthTestResponse(user_id="U_OWN", team_id="T1"))
                ],
                "ConversationsListRequest": [
                    ScriptedReply(response=ConversationsListResponse(channels=[
                        SlackChannel(id="C1", name="general", is_member=True, is_archived=False,
                                     is_im=False, is_mpim=False),
                    ]))
                ],
                "ConversationsHistoryRequest": [
                    ScriptedReply(response=ConversationsHistoryResponse(
                        messages=[SlackMessage(ts="100.000", user="U_AUTHOR", text="hi")],
                        has_more=False,
                    ))
                ],
            })
        raise NotImplementedError(f"no fake for {mcp_server!r}")

    monkeypatch.setattr(worker_mod, "_build_mcp_client", fake_build_mcp_client)


@pytest.mark.asyncio
async def test_worker_runs_slack_end_to_end(
    initialized_db: Path, slack_mcp_factory
) -> None:
    opts = WorkerOptions(
        registry={"slack": SlackSource},
        sources_enabled=["slack"],
        clock=FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)),
        pid=os.getpid(),
        run_id="test-run",
    )
    code = await run_worker(opts)
    assert code == EXIT_OK
    with sqlite3.connect(initialized_db) as conn:
        n = conn.execute("SELECT count(*) FROM events WHERE source='slack'").fetchone()[0]
    assert n == 1
```

- [ ] **Step 3: Run failing test**

Expected: `_build_mcp_client` doesn't exist on worker yet; `AttributeError` from monkeypatch.

- [ ] **Step 4: Modify `worker.py`**

Replace `_NullMCPClient` and the `_build_sources` function in `src/work_assistant/ingest/worker.py`:

```python
from work_assistant import config as wa_config
from work_assistant.mcp.bridge import MCPBridge
from work_assistant.mcp.client import BridgeMCPClient


def _build_mcp_client(mcp_server: str, settings: wa_config.Config) -> MCPClient:
    """Spawn the MCP server process for `mcp_server` and wrap it.

    Tests monkeypatch this function to inject a FakeMCPClient.
    """
    if mcp_server == "slack":
        cmd = list(settings.mcp.slack_command)
    elif mcp_server == "todoist":
        cmd = list(settings.mcp.todoist_command)
    elif mcp_server == "workspace":
        cmd = list(settings.mcp.workspace_command)
    else:
        raise ValueError(f"unknown mcp_server: {mcp_server!r}")
    bridge = MCPBridge(command=cmd)
    return BridgeMCPClient(bridge=bridge)


def _build_sources(
    *,
    selected: dict[str, type[Source]],
    db_factory: DbFactory,
    clock: Clock,
    run_id: str,
    settings: wa_config.Config,
) -> list[Source]:
    """Build one Source instance per selected entry, sharing one MCPClient
    per `mcp_server` group."""
    clients_by_server: dict[str, MCPClient] = {}
    instances: list[Source] = []
    for name, cls in selected.items():
        server = cls.mcp_server
        if server not in clients_by_server:
            clients_by_server[server] = _build_mcp_client(server, settings)
        logger = bind_source_logger(source=name, run_id=run_id)
        ctx = IngestContext(
            db=db_factory,
            mcp=clients_by_server[server],
            logger=logger,
            settings=settings,
            clock=clock,
        )
        instances.append(cls(ctx))
    return instances
```

Update `run_worker` to load config and pass it through:

```python
async def run_worker(opts: WorkerOptions) -> int:
    if opts.clock is None:
        raise ValueError("WorkerOptions.clock is required")

    base_logger = bind_source_logger(source="-", run_id=opts.run_id)

    try:
        settings = wa_config.load()
    except wa_config.ConfigError as exc:
        base_logger.error("config_load_failed", detail=str(exc))
        return compute_exit_code([], lock_held=False, config_fatal=True)

    try:
        names = _resolve_enabled(opts)
        selected = select_sources(registry=opts.registry, requested=names if names else None)
    except UnknownSourceError as exc:
        base_logger.error("unknown_source", detail=str(exc))
        return EXIT_USAGE

    db_path = paths.db_path()
    db_factory: DbFactory = (
        _DryRunDbFactory(db_path=db_path) if opts.dry_run else SqliteDbFactory(db_path=db_path)
    )

    def _new_lock_conn() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    try:
        with closing(_new_lock_conn()) as lock_conn:
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
                settings=settings,
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
        with closing(_new_lock_conn()) as lock_conn:
            release_lock(lock_conn, pid=opts.pid)

    return compute_exit_code(results, lock_held=False, config_fatal=False)
```

Drop the `_NullMCPClient` class and the now-unused `MCPRequest`/`MCPResponse` imports if applicable.

- [ ] **Step 5: Wire CLI to import sources package**

Modify `src/work_assistant/ingest/cli.py`. Find the existing `from work_assistant.ingest.registry import SOURCES` line and add right below it:

```python
from work_assistant.ingest import sources as _sources  # noqa: F401  (registers SOURCES)
```

This import side-effect populates `SOURCES["slack"]` before the CLI reads it.

- [ ] **Step 6: Update existing worker tests for the new API**

The Phase 1 `test_worker.py` constructed `WorkerOptions(...)` and called `run_worker(opts)` without a config file. Now `run_worker` calls `wa_config.load()`. Tests must provide a valid config file via `isolated_home` fixture.

In `tests/ingest/test_worker.py`, add an autouse fixture at the top of the file that writes a minimal config (mirror the one already in `tests/ingest/test_cli.py`):

```python
import pytest
from pathlib import Path

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
    (isolated_home / ".work_assistant" / "config.toml").write_text(_MINIMAL_CONFIG, encoding="utf-8")
```

Also, the existing `test_run_worker_one_ok_source` and `test_isolation_one_source_failure_does_not_block_sibling` use `StubSource` with `mcp_server` attribute. `StubSource.make()` sets `mcp_server` from the kwarg; `_build_sources` will call `_build_mcp_client(stub_server)` which raises `ValueError` for unknown servers. Tests must monkey-patch `_build_mcp_client` to return a `FakeMCPClient` (or just any MCPClient — StubSource doesn't call MCP).

Add to those tests:

```python
@pytest.fixture(autouse=True)
def _stub_mcp(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace _build_mcp_client with a sentinel client for stub sources."""
    from work_assistant.ingest import worker as worker_mod
    from tests.ingest.fakes import FakeMCPClient
    monkeypatch.setattr(
        worker_mod, "_build_mcp_client", lambda server, settings: FakeMCPClient(script={})
    )
```

- [ ] **Step 7: Run all worker tests**

```bash
uv run pytest tests/ingest/test_worker.py tests/ingest/test_worker_slack.py -v
```

Expected: all pass. The end-to-end Slack test inserts 1 event under `source='slack'`.

- [ ] **Step 8: Lint + full suite**

```bash
uv run ruff check src/work_assistant/ingest/worker.py src/work_assistant/ingest/cli.py tests/ingest/test_worker.py tests/ingest/test_worker_slack.py
uv run ruff format --check src/work_assistant/ingest/worker.py src/work_assistant/ingest/cli.py tests/ingest/test_worker.py tests/ingest/test_worker_slack.py
uv run pytest
```

Expected: full suite (Phase 1 99 + new ~30) pass; lint + format clean.

- [ ] **Step 9: Commit**

```bash
git add src/work_assistant/ingest/worker.py src/work_assistant/ingest/cli.py tests/ingest/test_worker.py tests/ingest/test_worker_slack.py
git commit -m "feat(worker): wire BridgeMCPClient per mcp_server; drop _NullMCPClient"
```

---

## Task 10: `--since` plumbing

**Files:**
- Modify: `src/work_assistant/ingest/sources/slack.py` (consume `since_unix` from settings/opts at `fetch` seed step)
- Modify: `src/work_assistant/ingest/worker.py` or `_build_sources` to pass `since_unix` to source instances
- Modify: `src/work_assistant/ingest/source.py` to add an optional `since_unix` field on the Source ABC OR pass via context
- Test: `tests/ingest/sources/test_slack_source.py` (extend)

The cleanest plumbing path: extend `IngestContext` with `since_unix: int | None = None`, then `SlackSource.fetch` consults `self.ctx.since_unix` for the channel-seed timestamp instead of `now - backfill_days * 86400` when set.

- [ ] **Step 1: Write failing test**

Append to `tests/ingest/sources/test_slack_source.py`:

```python


@pytest.mark.asyncio
async def test_fetch_uses_since_unix_when_set(initialized_db: Path) -> None:
    """When ctx.since_unix is set, new channels seed at that ts instead of backfill window."""
    mcp = FakeMCPClient(script=slack_script(
        list_channels=[ConversationsListResponse(channels=[_channel("C_NEW", "fresh")])],
        histories={"C_NEW": [ConversationsHistoryResponse(messages=[], has_more=False)]},
    ))
    ctx = IngestContext(
        db=SqliteDbFactory(db_path=initialized_db),
        mcp=mcp,
        logger=structlog.get_logger("test"),
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)),
        since_unix=1_700_000_000,
    )
    src = SlackSource(ctx)
    [b async for b in src.fetch(SlackCursor())]
    history_call = next(c for c in mcp.calls if type(c.request).__name__ == "ConversationsHistoryRequest")
    assert history_call.request.oldest == "1700000000"
```

- [ ] **Step 2: Add `since_unix` to `IngestContext`**

Modify `src/work_assistant/ingest/context.py` `IngestContext` dataclass:

```python
@dataclass(frozen=True)
class IngestContext:
    """Constructed by the worker, one per source. Never shared. Never mutated."""

    db: DbFactory
    mcp: MCPClient
    logger: structlog.stdlib.BoundLogger
    settings: "Config"
    clock: Clock
    since_unix: int | None = None
```

- [ ] **Step 3: Plumb `since_unix` through worker**

Modify `_build_sources` in `worker.py`:

```python
def _build_sources(
    *,
    selected: dict[str, type[Source]],
    db_factory: DbFactory,
    clock: Clock,
    run_id: str,
    settings: wa_config.Config,
    since_unix: int | None,
) -> list[Source]:
    ...
    ctx = IngestContext(
        db=db_factory,
        mcp=clients_by_server[server],
        logger=logger,
        settings=settings,
        clock=clock,
        since_unix=since_unix,
    )
    ...
```

And update the `run_worker` call site:

```python
sources = _build_sources(
    selected=selected,
    db_factory=db_factory,
    clock=opts.clock,
    run_id=opts.run_id,
    settings=settings,
    since_unix=opts.since_unix,
)
```

- [ ] **Step 4: Update `SlackSource.fetch` to consult `ctx.since_unix`**

Replace the seed-timestamp computation in `fetch`:

```python
            ch_cursor = slack_cursor.lookup(channel.id)
            if ch_cursor is None:
                seed_ts = (
                    self.ctx.since_unix
                    if self.ctx.since_unix is not None
                    else self.ctx.clock.now_unix() - BACKFILL_DAYS_DEFAULT * 86400
                )
                ch_cursor = ChannelCursor(
                    channel_id=channel.id,
                    channel_name=channel.name,
                    last_seen_ts=str(seed_ts),
                )
```

When `since_unix` is set, also override the cursor row entirely (per spec §4.4 — `--since` replaces persisted state for that run):

```python
    def _load_or_init_cursor(self, cursor: Cursor | None) -> SlackCursor:
        if self.ctx.since_unix is not None:
            return SlackCursor()  # discard persisted state on --since runs
        if isinstance(cursor, SlackCursor):
            return cursor
        with self.ctx.db.open() as conn:
            row = conn.execute(
                "SELECT cursor FROM ingest_cursors WHERE source = 'slack'"
            ).fetchone()
        if row is None or not row["cursor"]:
            return SlackCursor()
        return SlackCursor.model_validate_json(row["cursor"])
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/ingest/sources/test_slack_source.py tests/ingest/test_worker_slack.py -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run pytest
```

Expected: all green; full suite still passes.

- [ ] **Step 6: Commit**

```bash
git add src/work_assistant/ingest/context.py src/work_assistant/ingest/worker.py src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_source.py
git commit -m "feat(slack): plumb --since through IngestContext and SlackSource"
```

---

## Task 11: Permalink fetch for mentions/DMs (lazy)

**Files:**
- Modify: `src/work_assistant/ingest/sources/slack.py` (lazy permalink for mentions/DMs only)
- Test: `tests/ingest/sources/test_slack_source.py` (extend)

Per spec §3.6: permalink fetched lazily for mentions/DMs to save API budget.

- [ ] **Step 1: Write failing tests**

Append to `tests/ingest/sources/test_slack_source.py`:

```python
from work_assistant.ingest.sources.slack import GetPermalinkResponse


@pytest.mark.asyncio
async def test_fetch_fills_permalink_for_mentions(initialized_db: Path) -> None:
    msg = _msg("100.000", user="U_OTHER", text="hey <@U_OWN>")
    mcp = FakeMCPClient(script={
        **slack_script(
            list_channels=[ConversationsListResponse(channels=[_channel("C1", "general")])],
            histories={"C1": [ConversationsHistoryResponse(messages=[msg], has_more=False)]},
        ),
        "GetPermalinkRequest": [
            ScriptedReply(response=GetPermalinkResponse(permalink="https://example.slack.com/archives/C1/p100000"))
        ],
    })
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    assert batches[0].events[0].source_link == "https://example.slack.com/archives/C1/p100000"


@pytest.mark.asyncio
async def test_fetch_skips_permalink_for_non_mention(initialized_db: Path) -> None:
    msg = _msg("100.000", user="U_OTHER", text="non-mention")
    mcp = FakeMCPClient(script=slack_script(
        list_channels=[ConversationsListResponse(channels=[_channel("C1", "general")])],
        histories={"C1": [ConversationsHistoryResponse(messages=[msg], has_more=False)]},
    ))
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    assert batches[0].events[0].source_link is None
    permalink_calls = [c for c in mcp.calls if type(c.request).__name__ == "GetPermalinkRequest"]
    assert permalink_calls == []
```

Add the missing import at the top of the test file:
```python
from tests.ingest.fakes import ScriptedReply
```

- [ ] **Step 2: Append permalink helper to `slack.py`**

Add a private async helper and call it conditionally in the message loop:

```python
    async def _maybe_permalink(
        self, *, channel: SlackChannel, msg: SlackMessage, own_user_id: str,
    ) -> str | None:
        """Fetch chat.getPermalink only for mentions or DMs."""
        own_marker = f"<@{own_user_id}>"
        is_mention = own_marker in msg.text
        if not (is_mention or channel.is_im):
            return None
        try:
            resp = await self.ctx.mcp.call(
                GetPermalinkRequest(channel=channel.id, message_ts=msg.ts),
                GetPermalinkResponse,
            )
        except SlackError as exc:
            self.ctx.logger.warning(
                "slack_permalink_failed", channel=channel.id, ts=msg.ts, code=exc.code,
            )
            return None
        return resp.permalink
```

In the `fetch` message loop, after building each event, replace:

```python
events.append(_normalize_message(...))
```

with:

```python
event = _normalize_message(
    msg=msg, channel=channel, cache=cache,
    own_user_id=own_user_id, source_name="slack",
)
permalink = await self._maybe_permalink(channel=channel, msg=msg, own_user_id=own_user_id)
if permalink is not None:
    event = event.model_copy(update={"source_link": permalink})
events.append(event)
```

`NormalizedEvent` is frozen; `model_copy(update=...)` returns a new instance.

Same treatment for replies: build, conditionally fetch permalink, append.

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/ingest/sources/test_slack_source.py -v
uv run ruff check src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_source.py
uv run ruff format --check src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_source.py
uv run pytest
```

Expected: new tests pass; full suite green.

- [ ] **Step 4: Commit**

```bash
git add src/work_assistant/ingest/sources/slack.py tests/ingest/sources/test_slack_source.py
git commit -m "feat(slack): lazy chat.getPermalink for mentions and DMs"
```

---

## Task 12: Final lint + full suite + branch finishing

**Files:** none created.

- [ ] **Step 1: Run full suite**

```bash
uv run pytest -v
```

Expected: all tests pass — Phase 1 baseline (99) + Slack additions (~30).

- [ ] **Step 2: Run ruff**

```bash
uv run ruff check
uv run ruff format --check
```

Expected: clean. Fix anything needed; commit as `chore: ruff cleanup`.

- [ ] **Step 3: Sanity-check the CLI**

```bash
uv run wa ingest --help
```

Expected: same four flags, no new options. `--source` now accepts `slack`.

If a real `~/.work_assistant/config.toml` and a Slack MCP server binary exist locally, exercise:

```bash
uv run wa ingest --source slack --dry-run --verbose
```

Expected: lock acquired, releases cleanly, exit 0; with `--dry-run`, zero rows in `events`.

- [ ] **Step 4: Push branch and open PR**

```bash
git push -u origin slack-source
gh pr create --base master --head slack-source --title "feat: slack source (cron incremental)" --body "$(cat <<'EOF'
## Summary

First concrete `Source` impl: cron-driven Slack ingest. Wires the real `BridgeMCPClient` into the worker (drops `_NullMCPClient` placeholder). Fan-out per channel, threads pulled only for user-participated/-mentioned conversations, lazy permalink for mentions/DMs.

Closes Phase 1 deferred follow-ups around cursor parsing, real-MCP wiring, `--since` plumbing, and `IngestContext.settings`.

## Test plan

- [x] All Slack unit tests pass.
- [x] End-to-end worker test with FakeMCPClient pass.
- [x] Phase 1 baseline 99 tests still pass.
- [x] Ruff check + format clean.
- [ ] Manual: `wa ingest --source slack --dry-run --verbose` against local Slack MCP.

## Out of scope

Webhook receiver, backfill via export, multi-workspace, reaction/edit/delete events, real-API smoke tests.
EOF
)"
```

---

## Self-review notes

**Spec coverage:**
- §2 architecture (single module, sources/__init__.py, worker integration) → Task 6 (module + registry), Task 9 (worker rewire).
- §3.1 cursor types → Task 2.
- §3.2 MCP request/response models → Task 3.
- §3.3 domain types → Task 3.
- §3.4 `_SlackUserCache` → Task 4.
- §3.5 `SlackSource` (fetch, normalize_body, resolve_actor, cursor_from_timestamp) → Task 6 + Task 10 + Task 11.
- §3.6 helpers (`_thread_eligible`, `_normalize_message`, lazy permalink) → Task 5 + Task 11.
- §3.7 migration → Task 1.
- §4.1–4.5 data flow (per-channel cap, first-run backfill, `--since`, mention rewrite) → Tasks 6, 10.
- §5 error handling → Task 7.
- §6 testing matrix → Tasks 2, 3, 4, 5, 6, 7, 8, 9, 11.
- §7 scaffold gaps closed → Task 9 (real MCP wiring + real settings) + Task 10 (`--since`).

**Type consistency:**
- `SlackCursor.with_updated(ch: ChannelCursor)` named consistently across tasks.
- `SlackSource.name`, `SlackSource.mcp_server` are `ClassVar[str]` — same shape as `Source` ABC.
- `_SlackUserCache.get/upsert/is_stale` signatures stable.
- `_normalize_message` keyword-only signature stable.
- `_thread_eligible(top, *, replies, own_user_id)` stable.

**Out-of-scope guards:**
- Webhook receiver, backfill via export, real-API smoke, multi-workspace, reactions/edits — all explicitly out of scope.
