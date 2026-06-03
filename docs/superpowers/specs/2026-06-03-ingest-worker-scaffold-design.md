# Ingest Worker Scaffold — Design Spec

**Date:** 2026-06-03
**Status:** Revised after adversarial review (rev 2)
**Phase:** 1 (read-only ingestion + chat)
**Scope:** Worker entrypoint, lock model, cursor/batch transaction model, `Source` ABC, `MCPClient` adapter ABC, `IngestContext`, test harness, CLI flags, failure handling. Per-source implementations (Slack, Gmail, Calendar, Todoist read-back) follow as separate specs and plans.

**Rev 2 changelog (adversarial review fixes):**
- §2 `NormalizedEvent` aligned with `events` schema (`occurred_at: int`, full field set, typed `metadata` per source).
- §2 abstract/concrete split corrected: per-source helpers moved to abstract methods; only `compute_content_hash` stays concrete.
- §2.1 introduces `MCPClient` ABC adapter wrapping the existing concrete `MCPBridge` — no rewrite of `bridge.py`. `Any` is contained inside the adapter.
- §3 isolation rewritten: `asyncio.gather(..., return_exceptions=True)` instead of `TaskGroup` (TaskGroup cancels siblings on uncaught exception). Status-write failures handled with best-effort retry + log-only fallback.
- §3 cursor-advance rule tightened: `inserted == 0 AND batch.events != []` for two consecutive batches → fail the source (footgun fix). Calendar cursor is per-calendar map.
- §3 lock policy unified with `02-data-model.md`: `INSERT OR IGNORE`, reclaim only when row's PID is dead OR TTL expired. Heartbeat updates `acquired_at` so long batches don't get reclaimed.
- §3 SQLite WAL writer-serialization acknowledged: per-source tasks fan out reads in parallel but writes serialize. Acceptable for Phase 1 volume.
- §6 exit code 4 narrowed (worker-global config); per-source missing config stays exit 1. Exit 5 added for "all errors transient." Exit-code precedence rule defined. Failure matrix expanded with network timeout, MCP hang, OOM, schema drift, NTP skew, KeyboardInterrupt-mid-batch.
- §5 FakeBridge becomes FakeMCPClient (matches new adapter), descriptive errors on under-scripted calls.

---

## 1. Architecture

A short-lived `wa ingest` process per invocation. Triggered by launchd/systemd on a cadence (5 min default for Phase 1). No long-running ingest daemon.

```
launchd → wa ingest [flags]
            │
            ├─ acquire worker_locks row ('ingest')   ─── §3.1 (PID-alive + TTL)
            ├─ start heartbeat task                  ─── refresh acquired_at
            ├─ load enabled sources from settings
            ├─ group sources by mcp_server  →  start one MCPBridge per group
            │                              →  wrap each in BridgeMCPClient (§2.1)
            ├─ asyncio.gather(*coros, return_exceptions=True)
            │     where each coro = _run_source_safely(source) (never raises)
            │     └─ source.fetch(cursor) → batches → INSERT events + UPDATE cursor (one tx)
            ├─ cancel heartbeat
            ├─ release worker_locks row (DELETE WHERE pid = self.pid)
            └─ exit per §6.2 (0/1/2/3/4/5/130)
```

Three MCP bridges max (Slack, Workspace, Todoist). Sources sharing a server share the bridge. Per-source isolation: one source's failure does not block siblings; cursor only advances on a successful batch commit.

## 2. Source contract

`Source` is an ABC. We own all implementations. ABC chosen over `Protocol` per repo convention (`CLAUDE.md`) — runtime validation at instantiation, reliable `isinstance()`, no structural-typing surprises. Per-source logic that genuinely differs (body normalization, actor resolution) is **abstract**, not concrete shared. Only the canonical content-hash function is concrete.

### 2.1 `MCPClient` adapter ABC

`MCPBridge` (`src/work_assistant/mcp/bridge.py`) is a concrete class returning the MCP SDK's `CallToolResult` with `Any` payloads. We wrap it in an `MCPClient` ABC so source code never touches `Any`:

```python
from abc import ABC, abstractmethod

from pydantic import BaseModel, ConfigDict


class MCPRequest(BaseModel):
    """Per-tool typed request. Subclassed per (server, tool)."""
    model_config = ConfigDict(frozen=True)


class MCPResponse(BaseModel):
    """Per-tool typed response. Subclassed per (server, tool)."""
    model_config = ConfigDict(frozen=True)


class MCPClient(ABC):
    """Source-facing adapter. Hides MCPBridge + CallToolResult parsing."""

    @abstractmethod
    async def call(
        self,
        request: MCPRequest,
        response_model: type[MCPResponse],
    ) -> MCPResponse:
        """Dispatch tool, parse `CallToolResult` into the typed response."""
```

The production impl, `BridgeMCPClient(MCPClient)`, holds an `MCPBridge` instance, derives `(tool_name, arguments_dict)` from `request` (each `MCPRequest` subclass declares `tool_name: ClassVar[str]` + `model_dump()` for arguments), calls `bridge.call_tool(...)`, and parses `CallToolResult` into `response_model`. `Any` lives only inside `BridgeMCPClient`.

`FakeMCPClient(MCPClient)` (test impl, §5) inherits the same ABC.

### 2.2 Cursor, NormalizedEvent, Batch

```python
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict


class Cursor(BaseModel):
    """Source-specific cursor state. Each source subclasses with its fields.

    Concrete shapes:
      SlackCursor:    channels: dict[str, str]      # channel_id -> oldest ts
      GmailCursor:    history_id: str
      CalendarCursor: per_calendar: dict[str, str]  # calendar_id -> sync_token
      TodoistCursor:  sync_token: str
    """
    model_config = ConfigDict(frozen=True)


# Per-source `metadata_json` payloads — discriminated union.
class SlackMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["slack"] = "slack"
    channel_id: str
    channel_name: str
    is_im: bool
    is_mpim: bool
    is_dm: bool
    is_mention: bool
    reactions_json: str               # parsed slack reactions array, re-serialized
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
    start: str                        # ISO8601
    end: str                          # ISO8601
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


EventMetadata = SlackMetadata | GmailMetadata | CalendarMetadata | TodoistMetadata


class NormalizedEvent(BaseModel):
    """Aligned with `events` table in docs/02-data-model.md."""
    model_config = ConfigDict(frozen=True)

    source: str                       # 'slack' | 'gmail' | 'calendar' | 'todoist'
    source_id: str                    # stable per-source ID
    source_link: str | None
    content_hash: str                 # sha256(source + ":" + source_id + ":" + body)
    occurred_at: int                  # unix ts (seconds), UTC
    actor: str | None
    thread_key: str | None
    kind: Literal["message", "email", "meeting", "doc", "task_state"]
    title: str | None
    body: str | None
    body_truncated: bool              # NOT a column; worker uses to set metadata flag
    metadata: EventMetadata           # serialized to events.metadata_json on write


class Batch(BaseModel):
    model_config = ConfigDict(frozen=True)

    events: list[NormalizedEvent]
    next_cursor: Cursor
    status: Literal["ok", "partial"]


class Source(ABC):
    name: ClassVar[str]               # 'slack' | 'gmail' | 'calendar' | 'todoist'
    mcp_server: ClassVar[str]         # 'slack' | 'workspace' | 'todoist'

    def __init__(self, ctx: IngestContext) -> None:
        self.ctx = ctx

    @abstractmethod
    async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
        """Async generator. Yield batches. Worker wraps each batch in a tx.

        Implementations: `async def fetch(...) -> AsyncIterator[Batch]: yield ...`.
        """

    @abstractmethod
    def normalize_body(self, raw: str) -> tuple[str, bool]:
        """Per-source: strip / decode HTML / truncate to 100 KB. Returns (body, truncated)."""

    @abstractmethod
    async def resolve_actor(self, raw_actor: str) -> str | None:
        """Per-source: map provider id to email when possible; else best-effort id."""

    def compute_content_hash(self, source_id: str, body: str) -> str:
        """Concrete. Canonical hash per docs/04-ingestion-pipelines.md §Common normalization.

        Returns sha256(self.name + ':' + source_id + ':' + body) hex digest.
        """

    @abstractmethod
    def cursor_from_timestamp(self, ts: int) -> Cursor:
        """For `--since` (§6.1): synthesize a one-shot cursor at this unix ts.
        Per-source spec defines mapping to its native cursor shape.
        """
```

Per-source `Cursor` subclasses, `metadata` variants, and `MCPRequest`/`MCPResponse` pairs ship in each per-source spec.

No `Any` in any source-facing signature. `metadata_json` storage is a pydantic `model_dump_json()` at write time.

## 3. Worker run flow

Entrypoint: `wa ingest [--source X] [--dry-run] [--verbose] [--since ISO8601]`. One process per invocation. Exit per §6 failure matrix.

### 3.1 Lock model

`worker_locks` schema (`docs/02-data-model.md`): `(name, pid, acquired_at)`. Reclaim policy:

```
INSERT OR IGNORE INTO worker_locks (name, pid, acquired_at)
  VALUES ('ingest', :pid, :now);

-- if INSERT was a no-op (row exists), examine existing row:
SELECT pid, acquired_at FROM worker_locks WHERE name = 'ingest';

reclaim if:
  (existing.pid is not alive on this machine)         -- crashed predecessor
  OR (now - existing.acquired_at > LOCK_TTL_SECONDS)  -- stuck/zombie predecessor

reclaim is: DELETE the row, then re-attempt INSERT OR IGNORE.

else: exit 3 (lock held by live worker).
```

`LOCK_TTL_SECONDS` default 1800 (30 min). PID-alive check: `os.kill(pid, 0)` (`ProcessLookupError` → dead).

**Heartbeat.** While the worker runs, a background task updates `acquired_at = now()` every 60s. Long-running batches (e.g. backfill) are not reclaimed by a sibling cron tick. Heartbeat task cancelled on shutdown.

**Release.** On clean shutdown: `DELETE FROM worker_locks WHERE name = 'ingest' AND pid = :pid`.

### 3.2 Per-source isolation

`asyncio.TaskGroup` is **not** used: it cancels every sibling on any uncaught exception. We use `asyncio.gather(*coros, return_exceptions=True)`, where each `coro` is an internally-wrapped runner that never raises:

```python
async def _run_source_safely(source: Source) -> SourceRunResult:
    """Returns SourceRunResult; never raises."""
    try:
        return await _run_source(source)
    except BaseException as exc:                 # incl. CancelledError, propagate KeyboardInterrupt
        if isinstance(exc, KeyboardInterrupt | asyncio.CancelledError):
            raise
        # Best-effort status-write; if THAT fails, log and continue.
        try:
            _persist_source_error(source.name, exc)
        except Exception as inner:
            ctx.logger.error(
                "status_write_failed",
                source=source.name,
                primary=repr(exc),
                inner=repr(inner),
            )
        return SourceRunResult(name=source.name, status="error", exc=exc)
```

`SourceRunResult` is a frozen dataclass: `(name, status: Literal["ok","error","skipped"], inserted, ignored, exc)`. The worker collects results and computes exit code per §6.

### 3.3 Run flow

```
1. Validate db migration version (mismatch → exit 4).
2. Acquire 'ingest' worker_locks row per §3.1. Held by live worker → exit 3.
3. Start heartbeat task.
4. Load enabled sources from Settings (subset if --source).
5. Validate per-source config (OAuth tokens, etc.). Missing config →
   SourceRunResult(status='skipped', error='not configured'). Source not run.
6. Group remaining sources by mcp_server. For each group:
     a. Instantiate MCPBridge + BridgeMCPClient.
     b. On bridge __aenter__ failure: every source on that bridge gets
        SourceRunResult(status='error', exc=<bridge boot exc>).
7. Build IngestContext per source (db factory, mcp_client, logger, settings, clock).
8. results = await asyncio.gather(
       *(_run_source_safely(s) for s in sources_to_run),
       return_exceptions=False,                  # _run_source_safely never raises
   )
9. Cancel heartbeat. Close bridges (each via async-exit). Release worker_lock.
10. Compute exit code per §6 precedence rules.
```

### 3.4 Per-source fetch loop

```
a. SELECT cursor_json, last_status FROM ingest_cursors WHERE source = ?
   Parse cursor_json into the source's Cursor subclass (or None on first run).
b. consecutive_zero_insert_batches = 0
   async for batch in source.fetch(cursor):
       with db.transaction():
           inserted = 0; ignored = 0
           for event in batch.events:
               # row count check via cursor.rowcount on each insert:
               cur.execute("INSERT OR IGNORE INTO events (...) VALUES (...)", ...)
               if cur.rowcount == 1:
                   inserted += 1
               else:
                   ignored += 1
           UPDATE ingest_cursors
              SET cursor = :next_cursor_json,
                  last_status = 'ok',
                  updated_at = :now
            WHERE source = :name
       ctx.logger.info("batch_committed",
           source=name, inserted=inserted, ignored=ignored,
           events=len(batch.events))

       # Footgun guard: all-duplicates with non-empty input.
       if batch.events and inserted == 0:
           consecutive_zero_insert_batches += 1
           ctx.logger.warning("zero_insert_batch",
               source=name, ignored=ignored,
               consecutive=consecutive_zero_insert_batches)
           if consecutive_zero_insert_batches >= 2:
               raise SourceStallError(
                   f"{name}: 2 consecutive batches inserted 0 / ignored "
                   f"{ignored}+ events. Likely pagination or dedup-key bug."
               )
       else:
           consecutive_zero_insert_batches = 0
c. On exception inside the async-for or the tx body: tx auto-rolls-back via
   contextmanager exit. _run_source_safely catches and writes last_status.
   The cursor is NOT advanced for this batch. Source loop terminates.
```

### 3.5 Invariants

- **Cursor advances only inside a successful event-insert tx.** Crash mid-batch → cursor unchanged → next run replays. `INSERT OR IGNORE` makes replay idempotent.
- **Empty-batch cursor advance.** Allowed: a batch with `events=[]` and a new `next_cursor` advances the cursor (Gmail historyId case where there are no relevant changes but the cursor must move forward to avoid expiry).
- **Two-zero-insert-batches guard.** Surfaces silent data loss when fixtures or APIs degenerate to all-duplicates.
- **SQLite WAL writers serialize.** Async fan-out parallelizes reads (network I/O on bridges); writes serialize on the single SQLite file. With four sources at Phase 1 volume (~minutes-cadence batches of ≤200 events each), the bottleneck is network, not write contention. We accept this. `busy_timeout=5000` (per `02-data-model.md`) covers transient contention.
- **Connection lifecycle.** Lock + heartbeat hold one short-lived connection (open per heartbeat tick, not held idle). Per-source tasks open a fresh connection per batch tx via the `db` factory. No cross-task connection sharing.
- **Clock injection** (`ctx.clock.now()`) used in worker code (heartbeat, lock TTL math, log timestamps). Lock SQL uses `:now` parameter from `clock.now()`, not `strftime('%s','now')`, so `FakeClock` covers TTL paths.

## 4. IngestContext

Per-source context, frozen dataclass. Constructed by worker, never shared between sources, never mutated.

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class IngestContext:
    db: DbFactory                    # ABC: open() -> ContextManager[Connection]
    mcp: MCPClient                   # ABC adapter (§2.1) over MCPBridge
    logger: structlog.BoundLogger    # bound: source=name, run_id=<uuid>
    settings: Settings               # frozen pydantic
    clock: Clock                     # ABC: now() -> datetime (UTC)
```

- `db: DbFactory` — ABC. Each call to `db.open()` returns a context manager yielding a fresh `sqlite3.Connection` with WAL pragmas applied. No cross-task sharing.
- `mcp: MCPClient` — ABC adapter (§2.1). The shared `MCPBridge` is held by the adapter, not directly by the source. Source code calls `await ctx.mcp.call(request, ResponseModel)`.
- `logger` — concrete `structlog.BoundLogger` from a third party. CLAUDE.md "no `Any`" carve-out applies. Pre-bound by worker with `source=name, run_id=<uuid4>` so source code never re-binds.
- `clock: Clock` — ABC, abstract `now() -> datetime` (UTC). Test impl: `FakeClock`. Production impl: `SystemClock`.
- `settings: Settings` — frozen pydantic. Carries OAuth/API credentials and source-specific tunables (Slack channel allowlist, Calendar IDs, etc.).
- `run_id` — minted once per worker invocation as `uuid.uuid4().hex`, threaded through every source's logger for cross-source correlation.

`Clock`, `DbFactory`, `MCPClient` are all ABC, not `Protocol` — repo convention.

## 5. Test harness

Per-source: `FakeMCPClient` + JSON fixture payloads + in-memory or temp-file db.

```
tests/
├── conftest.py                   # tmp_db, fake_clock, fake_mcp_client factories
├── ingest/
│   ├── test_worker.py            # worker loop, lock, isolation, exit codes
│   ├── test_lock_reclaim.py      # PID-alive vs TTL-expired reclaim, heartbeat
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

`FakeMCPClient` inherits the `MCPClient` ABC (§2.1). No `MCPBridge` involvement at the unit level:

```python
@dataclass(frozen=True)
class ScriptedReply:
    response: MCPResponse | None = None       # success case
    raises: BaseException | None = None       # injected failure


@dataclass(frozen=True)
class RecordedCall:
    request: MCPRequest                       # the actual typed request


class FakeMCPClientError(RuntimeError):
    """Raised by FakeMCPClient on misuse (under-scripted, type mismatch, etc.)."""


class FakeMCPClient(MCPClient):
    def __init__(self, script: dict[str, list[ScriptedReply]]) -> None:
        # Key is the MCPRequest subclass __name__ for clarity in errors.
        self._script = {k: list(v) for k, v in script.items()}
        self.calls: list[RecordedCall] = []

    async def call(
        self,
        request: MCPRequest,
        response_model: type[MCPResponse],
    ) -> MCPResponse:
        self.calls.append(RecordedCall(request=request))
        key = type(request).__name__
        if key not in self._script or not self._script[key]:
            raise FakeMCPClientError(
                f"FakeMCPClient: unexpected call to {key} with "
                f"args={request.model_dump()!r}. "
                f"Scripted keys: {list(self._script)!r}"
            )
        scripted = self._script[key].pop(0)
        if scripted.raises is not None:
            raise scripted.raises
        if not isinstance(scripted.response, response_model):
            raise FakeMCPClientError(
                f"FakeMCPClient: scripted response for {key} is "
                f"{type(scripted.response).__name__}, expected {response_model.__name__}"
            )
        return scripted.response
```

Fixtures load via `<ResponseModel>.model_validate_json(path.read_text(encoding="utf-8"))` — bad fixture fails loudly at load, before the test runs.

### 5.1 Per-source tests

- happy path: `cursor=None` → first page → INSERT events → cursor advances
- pagination: `cursor=mid` → next page → final page → loop exits
- replay/dedup: same fixture twice → `INSERT OR IGNORE` → no dupes, cursor advances normally on first run
- error mid-batch: page 2 raises injected error → page 1 events committed, cursor at page-1, `last_status='error: ...'`
- two-zero-insert-batches: fixtures emit only duplicates twice → `SourceStallError` raised → source marked error, siblings continue
- empty-result advance: source returns `events=[]` with new `next_cursor` → cursor advances (Gmail historyId)
- empty-result no advance: source returns `events=[]` with same `next_cursor` → cursor unchanged
- malformed payload: fixture fails to parse into `MCPResponse` subclass → loud failure at load
- per-source normalization: `normalize_body` strips/decodes/truncates correctly per source
- per-source actor resolution: `resolve_actor` yields email when resolvable, falls back per spec

### 5.2 Worker-level tests (no real source needed)

- lock acquisition / contention: second invocation with live PID → exit 3
- lock reclaim — dead PID: stale row with non-existent PID → reclaim succeeds, run proceeds
- lock reclaim — TTL expired: stale row with live PID but `acquired_at < now - TTL` → reclaim succeeds
- heartbeat keeps lock fresh: simulate long batch, sibling cron tick from another `FakeClock` advance → still gets exit 3
- isolation via `gather(return_exceptions=True)`: one source raises, sibling completes; exit 1
- status-write failure: monkeypatch the status-write to raise → primary error logged, sibling completes, exit 1
- `--source X` filter: only X runs
- `--source X,Y,Z` subset
- `--dry-run`: no rows in `events` after run; cursor unchanged
- migration version mismatch: bail before any source runs; exit 4
- bridge boot failure: sources on that bridge marked error, others on different bridges continue
- KeyboardInterrupt mid-batch: signal mid-batch → tx rolls back, cursor unchanged, lock released, exit 130

### 5.3 Integration tier

Deferred. A separate spec covers a smoke test that boots one real MCP server (probably `Doist/todoist-mcp`, smallest surface) and runs one batch end-to-end against a tmp db. Not in this spec's scope.

## 6. CLI flags + failure matrix

### 6.1 CLI (click)

```
wa ingest                          # all enabled sources
wa ingest --source slack           # one source
wa ingest --source slack,gmail     # subset (comma-separated)
wa ingest --dry-run                # fetch + normalize, NO db writes, log per-source diff
wa ingest --verbose                # DEBUG-level structured logs
wa ingest --since ISO8601          # one-shot read-only override; cursor is NOT persisted
```

`--source`: validated against registry; unknown name → exit 2 (usage error).

`--since` semantics: each `Source` implements `cursor_from_timestamp(ts: int) -> Cursor` (per-source spec defines it). The worker calls this for each affected source and runs *this fetch only* with the synthetic cursor. The persisted `ingest_cursors` row is **not overwritten** — `--since` is read-replay, not write-through. Existing `INSERT OR IGNORE` keeps idempotency. Use case: manual replay of a window without losing forward progress.

`--once` is not a flag — every invocation is a single run. Scheduling lives in the launchd plist.

### 6.2 Exit codes

| code | meaning | retry-on-tick? |
|------|---------|----------------|
| 0 | all sources ok | n/a |
| 1 | one or more sources had a transient error (network, rate-limit, busy db); others may have succeeded | yes |
| 2 | usage error (bad flag, unknown source) | no |
| 3 | lock held by live worker, bailed clean | yes (next tick will likely succeed) |
| 4 | fatal worker-global config: db migration mismatch, missing/corrupt settings file, bridge SDK init failure (not server boot) | no — alert |
| 5 | one or more sources had a permanent error (auth revoked, account disabled, schema-incompatible) | no — alert |
| 130 | KeyboardInterrupt | n/a |

Per-source missing OAuth token is exit 1 (the source is skipped, not the worker — `wa doctor` will surface the misconfig). Worker-global missing settings is exit 4 (worker cannot run any source).

**Exit-code precedence** (highest wins, applied across all `SourceRunResult`s + worker-level outcomes):

```
4 > 5 > 1 > 3 > 2 > 0
```

Rationale: a config-fatal failure (4) beats per-source permanent (5); permanent failures (5) need attention even if some sources succeeded; transient (1) is the broadest bucket. A worker exit code of 3 only happens when *no* source ran (lock held), so it never collides with 1/5.

### 6.3 Failure matrix

| failure | scope | action | cursor | exit |
|---------|-------|--------|--------|------|
| MCP bridge SDK init failure | worker | bail before any source runs | n/a | 4 |
| MCP server subprocess fails to boot (stdio_client raises) | sources on that server | mark each as error, continue siblings on other bridges | unchanged for affected | 1 |
| MCP server hangs / no response within `MCP_CALL_TIMEOUT_S` (default 60) | one source's call | `MCPClient.call` raises `MCPTimeoutError` → source error | unchanged | 1 |
| Bridge stderr file write fails | shared bridge | log to structured logger; continue (stderr is diagnostic, not load-bearing) | n/a | unchanged |
| Network timeout / 5xx from upstream | one source | bubble as error, `last_status='error: <msg>'` | unchanged | 1 |
| Rate-limit (429) | one source | bubble as error; `last_status='error: rate-limited'` | unchanged | 1 |
| Auth revoked / 401 / scope dropped | one source | bubble as error; `last_status='error: auth: <msg>'` | unchanged | 5 |
| Source raises during fetch loop | one source | rollback open tx, set `last_status='error: …'` + full traceback | unchanged | 1 |
| Two consecutive zero-insert non-empty batches | one source | raise `SourceStallError` → source error | unchanged | 1 |
| Single event fails to normalize | one batch | drop event, log, increment `dropped_events` counter, continue | advances | 0 (per source) |
| `INSERT OR IGNORE` constraint hit | one event | silent ignore (counted in `ignored`) | advances | 0 |
| db locked / `busy_timeout` exceeded | one source's batch | bubble as error, `last_status` set; on status-write retry, retry up to 3× w/ jitter | unchanged | 1 |
| db schema drift detected mid-run (unexpected column) | worker | abort gracefully, release lock | unchanged | 4 |
| OOM / batch size too large | one source | source raises, tx rolls back; per-source spec sets a max-batch-events constant | unchanged | 1 |
| Settings file corrupt at startup | worker | bail before any work | n/a | 4 |
| Per-source OAuth token missing in settings | one source | source skipped at startup, `last_status='error: not configured'` | unchanged | 1 |
| Worker lock held by live PID | worker | bail before any work | n/a | 3 |
| Migration version mismatch | worker | bail before any work | n/a | 4 |
| NTP backward jump > LOCK_TTL during run | worker | benign for cursor (cursors are source-defined strings, not our clock); heartbeat refreshes `acquired_at` from the new clock; log warning | unchanged | per other rules |
| Status-write itself fails | one source | log primary + inner error; result still recorded as error | unchanged | 1 (or 5 if primary was permanent) |
| KeyboardInterrupt mid-batch | worker | tx rolls back via `with` exit, heartbeat cancelled, lock released | unchanged | 130 |

Defaults:
- **Drop-bad-event over fail-batch.** Single bad payload should not stall a source. Per-source spec can override if shape demands strict.
- **`last_status` not length-capped.** SQLite TEXT is unbounded; full traceback stored.
- **`MCP_CALL_TIMEOUT_S` default 60.** Per-source spec can lower for chatty tools.

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
