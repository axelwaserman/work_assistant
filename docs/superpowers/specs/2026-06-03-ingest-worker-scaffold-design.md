# Ingest Worker Scaffold — Design Spec

**Date:** 2026-06-03
**Status:** Approved (sections 1–6)
**Phase:** 1 (read-only ingestion + chat)
**Scope:** Worker entrypoint, lock model, cursor/batch transaction model, `Source` ABC, `IngestContext`, test harness, CLI flags, failure handling. Per-source implementations (Slack, Gmail, Calendar, Todoist read-back) follow as separate specs and plans.

---

## 1. Architecture

A short-lived `wa ingest` process per invocation. Triggered by launchd/systemd on a cadence (5 min default for Phase 1). No long-running ingest daemon.

```
launchd → wa ingest [flags]
            │
            ├─ acquire worker_locks row ('ingest')
            ├─ load enabled sources from settings
            ├─ group sources by mcp_server  →  start one MCPBridge per group
            ├─ asyncio.TaskGroup: one task per source
            │     └─ source.fetch(cursor) → batches → INSERT events + UPDATE cursor (one tx)
            ├─ release worker_locks row
            └─ exit 0/1/2/3/4
```

Three MCP bridges max (Slack, Workspace, Todoist). Sources sharing a server share the bridge. Per-source isolation: one source's failure does not block siblings; cursor only advances on a successful batch commit.

## 2. Source contract

`Source` is an ABC. We own all implementations and need shared concrete helpers (body normalization, content hashing, actor resolution). ABC chosen over `Protocol` per `dignified-python` skill `references/advanced/interfaces.md` — runtime validation at instantiation, reliable `isinstance()`, shared methods baked in.

```python
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict


class Cursor(BaseModel):
    """Source-specific cursor state. Each source subclasses with its fields."""
    model_config = ConfigDict(frozen=True)


class NormalizedEvent(BaseModel):
    """Shape that lands in the events table."""
    model_config = ConfigDict(frozen=True)

    source: str
    source_id: str
    actor: str
    happened_at: str           # ISO8601 UTC
    body: str
    body_truncated: bool
    content_hash: str
    raw_meta: "RawMeta"        # discriminated union per source


class Batch(BaseModel):
    model_config = ConfigDict(frozen=True)

    events: list[NormalizedEvent]
    next_cursor: Cursor
    status: Literal["ok", "partial"]


class Source(ABC):
    name: ClassVar[str]                  # 'slack' | 'gmail' | 'calendar' | 'todoist'
    mcp_server: ClassVar[str]            # 'slack' | 'workspace' | 'todoist'

    def __init__(self, ctx: "IngestContext") -> None:
        self.ctx = ctx

    @abstractmethod
    def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
        """Yield batches. Worker wraps each batch in a SQLite transaction."""
        ...

    # Concrete shared helpers
    def normalize_body(self, raw: str) -> tuple[str, bool]:
        """Strip / truncate / canonicalize. Returns (body, truncated)."""
        ...

    def content_hash(self, source_id: str, body: str) -> str:
        """Stable hash for second-layer dedup (Todoist footer)."""
        ...

    async def resolve_actor(self, raw_actor: str) -> str:
        """Map provider id to canonical actor string."""
        ...
```

Each `Cursor` subclass is a frozen pydantic model carrying source-shape state (Slack: per-channel `oldest` ts map; Gmail: `historyId`; Calendar: `syncToken`; Todoist: `sync_token`).

`raw_meta` is a discriminated union (one pydantic variant per source). No `Any`. Validation happens at parse boundary.

## 3. Worker run flow

Entrypoint: `wa ingest [--source X] [--dry-run] [--verbose] [--since ISO8601]`. One process per invocation. Exit 0 if all sources succeed, nonzero otherwise.

```
1. Validate db migration version (mismatch → exit 4).
2. Acquire worker_locks row for 'ingest':
     INSERT OR REPLACE WHERE expires_at < now() OR row missing.
   Held by live worker → exit 3.
3. Load enabled sources from Settings (subset if --source).
4. Group sources by mcp_server. Start one MCPBridge per group.
   Bridge boot failure → mark all sources on that bridge as errored, continue siblings.
5. For each source: instantiate with IngestContext (db factory, bridge, logger, clock).
6. asyncio.TaskGroup → one task per source. Per-task try/except absorbs source errors so siblings continue.
7. Per-source task:
     a. SELECT cursor, last_status FROM ingest_cursors WHERE source = ?
     b. async for batch in source.fetch(cursor):
            with db.transaction():
                for event in batch.events:
                    INSERT OR IGNORE INTO events (...)
                UPDATE ingest_cursors
                   SET cursor = ?, last_status = 'ok',
                       last_run_at = now(), error = NULL
                 WHERE source = ?
            log {source, inserted, ignored, next_cursor}
     c. On exception:
            with db.transaction():
                UPDATE ingest_cursors
                   SET last_status = 'error: <msg>',
                       error       = <full traceback>,
                       last_run_at = now()
                 WHERE source = ?
            (cursor itself NOT advanced)
            break source loop, siblings continue.
8. Release worker_lock. Close bridges.
9. Exit code per failure matrix.
```

**Invariants:**
- Cursor advances only inside a successful event-insert tx. Crash mid-batch → cursor unchanged → next run replays. `INSERT OR IGNORE` makes replay idempotent.
- Source loop iterates batches; source decides batch size and stop condition.
- Lock TTL: configurable, default 10 minutes. Expired lock reclaimable (single-machine assumption).
- Clock injection (`ctx.clock.now()`) for testability.

## 4. IngestContext

Per-source context, frozen dataclass. Constructed by worker, never shared, never mutated.

```python
from dataclasses import dataclass

import structlog
from pydantic import BaseModel


@dataclass(frozen=True)
class IngestContext:
    db: DbFactory                    # ABC: open() -> Connection
    bridge: MCPBridge                # ABC; pre-started by worker
    logger: structlog.BoundLogger    # bound: source=name, run_id=<uuid>
    settings: Settings               # frozen pydantic
    clock: Clock                     # ABC: now() -> datetime (UTC)
```

- `db` is a factory not a connection. Each batch tx opens a fresh connection. Avoids cross-task sharing.
- `bridge` already started by worker. Source calls `bridge.call(tool, args)`.
- `logger` pre-bound so source code never re-binds source name.
- `clock` and `db` are ABCs, not Protocols. Test impls (`FakeClock`, `InMemoryDbFactory`) inherit. Per repo convention: ABC always, no Protocol.
- `settings` carries OAuth/API credentials and source-specific tunables.
- `run_id` minted by worker, threaded through logger for cross-source correlation.

No singletons. No module-level state.

## 5. Test harness

Per-source: fake bridge + JSON fixture payloads + in-memory db.

```
tests/
├── conftest.py                   # tmp_db, fake_clock, fake_bridge factories
├── ingest/
│   ├── test_worker.py            # worker loop, lock, isolation, exit codes
│   ├── test_source_contract.py   # ABC compliance per impl
│   └── sources/
│       ├── test_slack.py
│       ├── test_gmail.py
│       ├── test_calendar.py
│       └── test_todoist.py
└── fixtures/
    ├── slack/
    │   ├── conversations_history_page1.json
    │   ├── conversations_history_page2.json
    │   └── empty.json
    ├── gmail/
    ├── calendar/
    └── todoist/
```

**FakeBridge** — typed, no `Any`. Discriminated union of replies per tool:

```python
@dataclass(frozen=True)
class ScriptedReply:
    payload: BridgeReply | None = None        # success
    raises: BridgeError | None = None         # injected failure


class BridgeReply(BaseModel):
    """Discriminated union over MCP tool reply shapes (one variant per tool)."""
    model_config = ConfigDict(frozen=True)
    # variants: SlackHistoryReply, GmailHistoryReply, CalendarEventsReply,
    # TodoistSyncReply — each with `kind: Literal[...]` discriminator.


@dataclass(frozen=True)
class BridgeCall:
    tool: str
    args: BridgeArgs                          # pydantic union per-tool


class FakeBridge(MCPBridge):
    def __init__(self, script: dict[str, list[ScriptedReply]]) -> None:
        self._script = script
        self.calls: list[BridgeCall] = []

    async def call(self, tool: str, args: BridgeArgs) -> BridgeReply:
        self.calls.append(BridgeCall(tool=tool, args=args))
        reply = self._script[tool].pop(0)
        if reply.raises is not None:
            raise reply.raises
        return reply.payload
```

`MCPBridge.call` signature: `args: BridgeArgs` (pydantic union), returns `BridgeReply`. Fixtures load via `BridgeReply.model_validate_json(path.read_text(encoding="utf-8"))` — bad fixture fails loudly at load.

**Per-source tests:**
- happy path: cursor=None → first page → INSERT events → cursor advances
- pagination: cursor=mid → next page → final page → stop
- replay/dedup: same fixture twice → INSERT OR IGNORE → no dupes, cursor advances
- error mid-batch: page 2 raises → page 1 events committed, cursor at page-1, last_status='error'
- empty result: no events, cursor unchanged (or advances per source semantics)
- malformed payload: fail loudly, log, no partial commit

**Worker-level tests** (no real source needed):
- lock acquisition / contention: second invocation bails with exit 3
- TaskGroup isolation: one source raises, siblings continue, exit 1
- `--source X` filter
- migration version mismatch: exit 4
- bridge boot failure: affected sources marked error, others continue

No real MCP processes in unit tests. Integration tier deferred (out of scope this spec).

## 6. CLI flags + failure matrix

**CLI** (click):

```
wa ingest                          # all enabled sources
wa ingest --source slack           # one source
wa ingest --source slack,gmail     # subset (comma-separated)
wa ingest --dry-run                # fetch + normalize, NO db writes, log diff
wa ingest --verbose                # DEBUG-level structured logs
wa ingest --since ISO8601          # override cursor (manual backfill — dangerous)
```

`--source` validated against registry; unknown name → exit 2 (usage error).

`--once` not a flag (always once for now). Scheduling lives in launchd plist, not CLI.

**Exit codes:**

| code | meaning |
|------|---------|
| 0 | all sources ok |
| 1 | one or more sources errored; others may have succeeded |
| 2 | usage error (bad flag, unknown source) |
| 3 | lock held by another worker, bailed clean |
| 4 | fatal: db migration mismatch, missing settings, bridge boot failure |
| 130 | KeyboardInterrupt |

**Failure matrix:**

| failure | scope | action | cursor | exit |
|---------|-------|--------|--------|------|
| MCP bridge fails to start | shared bridge | abort sources on that bridge, others continue | unchanged for affected | 1 |
| Source raises mid-batch | one source | rollback open tx, set last_status='error: …' + full traceback | unchanged | 1 |
| Single event normalize fails | one batch | drop event, log + counter, continue batch | advances | 0 (per source) |
| INSERT OR IGNORE constraint | one event | silent ignore (dedup as designed) | advances | 0 |
| db locked / busy_timeout exceeded | one source | bubble as error, last_status set | unchanged | 1 |
| Settings missing OAuth token | one source | source skipped at startup, last_status='error: not configured' | unchanged | 1 |
| Worker lock held | worker | bail before any work | n/a | 3 |
| Migration version mismatch | worker | bail before any work | n/a | 4 |
| BaseException (KeyboardInterrupt) | worker | release lock, propagate | unchanged | 130 |

Default policy: drop-bad-event over fail-batch. Single bad payload should not stall a source. A source can override if shape demands strict.

`last_status` not length-capped — full traceback stored. SQLite TEXT is unbounded; no premature storage optimization.

## Out of scope (this spec)

- Per-source implementations (Slack, Gmail, Calendar, Todoist read-back) — each gets its own design spec + plan.
- Webhook ingestion (Slack Events API). Phase 1 is poll-only; webhook deferred per `docs/07-open-decisions.md`.
- Backfill horizon policy (how far back on first run). Per-source decision; deferred.
- Embedding generation. Out of Phase 1 ingest path; lives in advisor pipeline.
- Long-running daemon mode. Explicitly excluded by repo convention.

## References

- `docs/01-architecture.md` — repo layout, component inventory.
- `docs/02-data-model.md` — events, ingest_cursors, worker_locks schema.
- `docs/04-ingestion-pipelines.md` — per-source MCP/API + cursor type.
- `docs/05-phased-plan.md` — Phase 1 deliverables and exit criteria.
- `docs/06-failure-modes.md` — failure-mode catalog this spec implements against.
- `dignified-python` skill `references/advanced/interfaces.md` — ABC vs Protocol decision.
- `CLAUDE.md` — repo conventions (ABC always, no `Any`, pathlib, absolute imports).
