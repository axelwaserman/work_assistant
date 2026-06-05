# Slack Source — Design Spec

**Date:** 2026-06-05
**Phase:** Phase 1 follow-on (ingest worker scaffold landed in `phase1` PR #3).
**Scope:** First concrete `Source` impl for the ingest worker. Cron-driven incremental Slack pull only. Webhook receiver and one-shot backfill-via-export are out of scope (defer to Phase 2 / separate specs).

---

## 1. Goal

Implement `SlackSource` so `wa ingest --source slack` pulls Slack messages every 15 min from every channel the user is a member of, normalizes them into the `events` table, advances per-channel cursors, and respects the contracts already defined by the Phase 1 worker scaffold (`Source` ABC, `Batch` shape, two-zero-insert stall guard, single-tx batch persist, exit-code precedence).

This is also the first concrete consumer of the `BridgeMCPClient` adapter — until now, the worker plugged in a `_NullMCPClient` placeholder.

---

## 2. Architecture

**Single module:** `src/work_assistant/ingest/sources/slack.py` (~500 lines target, well under the 800-line ceiling).

**New package:** `src/work_assistant/ingest/sources/__init__.py`. Imports `slack` so registry side-effect runs.

**Module surface:**
- Public: `SlackSource`, `SlackCursor`, `ChannelCursor`.
- Private: `_SlackUserCache`, `_thread_eligible`, `_normalize_message`, plus all per-tool `MCPRequest`/`MCPResponse` subclasses.

**Migration:** `src/work_assistant/db/migrations_sql/0002_slack_users.sql` adds the actor cache.

**Worker integration:** removes `_NullMCPClient`. `worker._build_sources` groups selected sources by `mcp_server`, instantiates one `MCPBridge` per group, wraps it in a `BridgeMCPClient`, and shares that client across sibling sources in the group. Slack is the only group member for now; the design admits a second source on the same MCP server cleanly (e.g. if Slack ever exposes additional tools we wrap as a separate source).

**Registry side-effect:** importing `work_assistant.ingest.sources.slack` registers `SOURCES["slack"] = SlackSource`. `wa ingest` already imports `SOURCES` lazily; adding the package import in `ingest/sources/__init__.py` is the wiring point. The `cli.py` change is a single new import line.

---

## 3. Components

### 3.1 Cursor types

```python
class ChannelCursor(BaseModel):
    model_config = ConfigDict(frozen=True)
    channel_id: str
    channel_name: str
    last_seen_ts: str   # Slack ts string e.g. "1717420800.123456"


class SlackCursor(Cursor):
    channels: list[ChannelCursor] = Field(default_factory=list)

    def lookup(self, channel_id: str) -> ChannelCursor | None: ...
    def with_updated(self, ch: ChannelCursor) -> "SlackCursor": ...
```

`SlackCursor` is a frozen pydantic model (subclass of the existing `Cursor` base). `with_updated` returns a new instance with the named channel replaced (or appended if new). `dict[str, str]` is deliberately avoided — `ChannelCursor` documents the field semantics.

### 3.2 MCP request / response models

One pair per Slack tool we call. Each is a frozen pydantic subclass of `MCPRequest` / `MCPResponse` per the Phase 1 contract.

| Tool | Request | Response shape |
|---|---|---|
| `slack_list_channels` | `ListChannelsRequest(types: str)` | `channels: list[SlackChannel]` |
| `slack_conversations_history` | `ConversationsHistoryRequest(channel: str, oldest: str, limit: int)` | `messages: list[SlackMessage]`, `has_more: bool`, `response_metadata: SlackPagination \| None` |
| `slack_conversations_replies` | `ConversationsRepliesRequest(channel: str, ts: str)` | `messages: list[SlackMessage]` |
| `slack_users_info` | `UsersInfoRequest(user: str)` | `user: SlackUser` |
| `slack_get_permalink` | `GetPermalinkRequest(channel: str, message_ts: str)` | `permalink: str` |
| `slack_auth_test` | `AuthTestRequest()` | `user_id: str`, `team_id: str` (used once per run to resolve own user_id) |

Tool name strings reflect the **Official Slack MCP** naming convention; final tool names confirmed in the implementation plan after spinning up the MCP server.

### 3.3 Domain types

`SlackChannel`, `SlackMessage`, `SlackUser`, `SlackPagination` — frozen pydantic structs that the MCP response models contain. Parsing seam: raw `dict[str, object]` from MCP → typed struct inside `BridgeMCPClient`'s reply path. Per CLAUDE.md, `Any` only at the parse boundary.

`SlackMessage` fields used: `ts`, `thread_ts | None`, `user | None`, `text`, `subtype | None` (so we can skip `channel_join`, `bot_message` if desired), `reactions: list | None`, `files: list | None`.

### 3.4 `_SlackUserCache`

Wraps the `slack_users` table. Bound to `IngestContext.db` and `IngestContext.clock`.

API:
```python
class _SlackUserCache:
    def __init__(self, db: DbFactory, clock: Clock) -> None: ...
    def get(self, user_id: str) -> SlackUser | None:
        """Return cached user iff fresh (<7 days). Else None."""
    def upsert(self, user: SlackUser, fetched_at: int) -> None: ...
    def is_stale(self, fetched_at: int) -> bool: ...
```

All SQL parameterized. `INSERT OR REPLACE` for upsert; freshness check uses `clock.now_unix() - fetched_at > 7*86400`.

### 3.5 `SlackSource`

```python
class SlackSource(Source):
    name: ClassVar[str] = "slack"
    mcp_server: ClassVar[str] = "slack"

    async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]: ...
    def normalize_body(self, raw: str) -> tuple[str, bool]: ...
    async def resolve_actor(self, raw_actor: str) -> str | None: ...
    def cursor_from_timestamp(self, ts: int) -> Cursor: ...
```

Concrete behavior detailed in §4.

### 3.6 Helpers

- `_thread_eligible(top_level: SlackMessage, replies: list[SlackMessage] | None, own_user_id: str) -> bool` — True if top-level user is own_user_id, OR any reply user is own_user_id, OR any message text contains `<@own_user_id>`. Applied with optional pre-fetched reply sample to avoid extra MCP round-trip when possible (top-level check + mention scan first).
- `_normalize_message(msg, channel, cache, own_user_id) -> NormalizedEvent` — body normalize, mention rewrite, content_hash via `Source.compute_content_hash`, permalink fetch only for mentions/DMs (lazy to save API budget).

### 3.7 Migration `0002_slack_users.sql`

```sql
CREATE TABLE slack_users (
  user_id      TEXT PRIMARY KEY,
  email        TEXT,
  display_name TEXT NOT NULL,
  fetched_at   INTEGER NOT NULL
);

CREATE INDEX idx_slack_users_fetched_at ON slack_users(fetched_at);
```

Numbered migration runs through existing `db.migrations.apply()` machinery from Phase 0.

---

## 4. Data flow

### 4.1 Per cron tick

```
1. Worker.run_worker(opts):
   - Acquire lock + heartbeat (existing scaffold).
   - Build BridgeMCPClient over MCPBridge spawning `mcp.slack_command`.
   - Build IngestContext(db, mcp, logger, settings, clock).
   - Instantiate SlackSource(ctx). Register imported via SOURCES dict.
   - run_source_safely(slack_source).

2. SlackSource.fetch(cursor):
   - cursor: SlackCursor parsed from existing row OR built fresh via cursor_from_timestamp(now - backfill_days_slack)
   - own_user_id = mcp.call(AuthTestRequest, AuthTestResponse).user_id
   - channels = mcp.call(ListChannelsRequest(types="public_channel,private_channel,im,mpim"), ...).channels
   - For each channel where channel.is_member and not channel.is_archived:
     a. ch_cursor = cursor.lookup(channel.id) OR ChannelCursor(id, name, now-backfill_days_slack)
     b. resp = mcp.call(ConversationsHistoryRequest(channel=ch.id, oldest=ch_cursor.last_seen_ts, limit=200))
     c. events: list[NormalizedEvent] = []
     d. For each top-level message in resp.messages:
        - event = _normalize_message(msg, channel, cache, own_user_id)
        - events.append(event)
        - if msg.thread_ts and _thread_eligible(msg, None, own_user_id):
            replies = mcp.call(ConversationsRepliesRequest(channel=ch.id, ts=msg.thread_ts)).messages
            if not _thread_eligible(msg, replies, own_user_id): continue  # double-check
            for reply in replies (skip ts==thread_ts to dedup parent):
              events.append(_normalize_message(reply, channel, cache, own_user_id))
     e. new_last_seen_ts = max(m.ts for m in resp.messages) if resp.messages else ch_cursor.last_seen_ts
     f. updated_cursor = cursor.with_updated(ChannelCursor(id, name, new_last_seen_ts))
     g. yield Batch(events=events, next_cursor=updated_cursor, status='ok')
     h. cursor = updated_cursor   # cumulative across channels

3. Runner._persist_batch (existing) wraps each yield in single SQLite txn:
   - INSERT OR IGNORE per event keyed on (source='slack', source_id=channel_id:ts)
   - UPSERT ingest_cursors row with cursor.model_dump_json()
   - COMMIT
   - Cursor advances iff all events for that channel committed.

4. Worker.compute_exit_code returns 0 on success; release lock.
```

### 4.2 Per-channel cap

200 messages per channel per run (Slack default page size). Channel with `has_more=true` simply waits for the next cron tick. Per docs/04: prevents one busy channel from starving siblings.

### 4.3 First-run backfill

No cursor row → fetch returns empty cursor text → SlackSource builds initial `SlackCursor()` → for each member channel, `cursor.lookup(channel_id)` returns None → ChannelCursor seeded at `now - backfill_days_slack` (default 30d per docs/04). First run pulls up to 200 messages per channel from that point. Subsequent runs incremental.

### 4.4 `--since` override

`SlackSource.cursor_from_timestamp(unix_ts)` returns an **empty `SlackCursor()` keyed on `unix_ts` semantics**: zero channel entries. `fetch()` then populates per-channel entries at runtime via `cursor.lookup(channel.id) or ChannelCursor(id, name, last_seen_ts=str(unix_ts))`. This avoids needing an MCP call inside the sync `cursor_from_timestamp` method (per `Source` ABC, that method is sync). Worker plumbs `opts.since_unix` through to this method. When `--since` set, the synthesized empty cursor replaces whatever's in the DB row, and end-of-run the cursor row is overwritten with the populated state (documented manual override).

### 4.5 Mention resolution in body

`<@U01ABC>` literals in `text` rewritten to `@<display_name>` using `_SlackUserCache`. Cache miss → fetch `users.info`, upsert, then rewrite. Hard miss (`users.info` returns error) → leave `<@U01ABC>` literal in body, don't fail the message. Logged at debug.

---

## 5. Error handling

### 5.1 MCP errors

Classified inside `BridgeMCPClient` reply path (or in `SlackSource.fetch` if the bridge returns the raw error):

| Error | Mapping | Exit code |
|---|---|---|
| HTTP 429 / Slack `error="rate_limited"` | `TransientIngestError` | 1 |
| `invalid_auth`, `account_inactive`, `not_authed`, `token_revoked` | `PermanentIngestError` | 5 |
| `channel_not_found`, `not_in_channel` (single channel) | log warn, drop channel from this run, continue siblings | 0 (run still ok) |
| Any other unexpected `error` | `TransientIngestError` (default classify) | 1 |
| MCP transport timeout / broken stdio | already `TransientIngestError` | 1 |

429: cursor preserved because runner only writes cursor inside `_persist_batch`, which never executes when `fetch` raises before yielding a batch.

### 5.2 Cache failures

- `users.info` failure for a single user during normalization → log, fall back to raw `<@U01ABC>` for that one mention, continue. Don't fail the batch.
- `slack_users` SQL error → bubble up, runner rolls back batch txn, classified.

### 5.3 Partial channel

`has_more=true` after 200 messages: do NOT paginate within a run. Cursor advances to `max(ts)` of fetched. Next run continues. Channel "drained slowly" — accepted per docs/04.

### 5.4 Stall guard

Phase 1 runner trips `SourceStallError` after 2 consecutive batches with `events != [] and inserted == 0`. For Slack should never fire in normal operation. If it does → permalink/source_id collision or cursor bug → exit 1 + log.

### 5.5 `--since` collision

If user passes `--since` AND a cursor row exists, `--since` wins for THAT run; cursor row is overwritten by the resulting state at end-of-run. Documented as a manual override.

### 5.6 Heartbeat

Long first-run backfill could exceed 60s. Heartbeat (already in Phase 1 scaffold) refreshes lock every 60s. No work needed here.

### 5.7 KeyboardInterrupt

Existing scaffold path: KI through `gather` → exit 130 → finally release lock. Cursor at last committed batch boundary. Re-run resumes cleanly.

---

## 6. Testing

All tests use `FakeMCPClient` (scripted Slack responses) and `initialized_db` fixture. No real Slack token required for the Phase 1 spec.

### 6.1 Test files

| File | Purpose | Target tests |
|---|---|---|
| `tests/ingest/sources/test_slack_models.py` | `SlackCursor` / `ChannelCursor` round-trip, `cursor_from_timestamp` | 4 |
| `tests/ingest/sources/test_slack_user_cache.py` | `_SlackUserCache` against real SQLite | 4 |
| `tests/ingest/sources/test_slack_source.py` | `SlackSource.fetch` against `FakeMCPClient` | 10+ |
| `tests/ingest/sources/test_slack_errors.py` | Error classification (rate limit, auth, missing channel) | 4 |
| `tests/ingest/sources/test_slack_registry.py` | Importing `slack` registers `SOURCES["slack"]` | 1 |
| `tests/ingest/test_worker_slack.py` | End-to-end with real `SlackSource` + `FakeMCPClient` injected | 1 |

### 6.2 `SlackSource.fetch` coverage matrix

- Happy path: 2 channels × 3 messages each, no threads → 2 Batches × 3 events.
- Empty channel: history returns 0 messages → Batch with `events=[]`, cursor unchanged. Stall does NOT fire.
- Thread eligible (user authored top-level): replies fetched and included.
- Thread eligible (user @-mentioned in a reply): replies fetched.
- Thread NOT eligible: `conversations.replies` not called (assert via `FakeMCPClient.calls`).
- New channel mid-run: not in cursor → seeded at `now - backfill_days_slack` → fetched.
- Archived channel: skipped, no API call.
- Mention rewrite in body: `<@U_OTHER>` → `@bob` when cache has bob.
- Body truncation: 200KB input → 100KB stored, `body_truncated=True`.
- Multiple channels: cursor batches preserve previous channels' updated state (cumulative).
- `cursor_from_timestamp(ts)`: returns empty `SlackCursor` (per §4.4); driven by `fetch()` to seed each channel at `str(ts)` on first lookup.

### 6.3 Coverage targets

- `SlackSource.fetch` branches above: all 10.
- `_thread_eligible`: top-level by user, mention by user, neither.
- `_normalize_message`: truncation, HTML strip, mention rewrite, missing actor.
- `_SlackUserCache`: hit, miss, stale, refresh.
- Error classification: 4 distinct paths.

### 6.4 Real-API smoke (deferred)

A separate later spec adds `tests/integration/test_slack_real.py` marked `@pytest.mark.real_api` for hand-runs with `WA_SLACK_TOKEN`. CI does not run. **Out of scope for this spec.**

---

## 7. Scaffold gaps closed by this spec

This source closes several Phase 1 deferred follow-ups:

1. **Cursor parsing**: `SlackSource.fetch` parses `existing_cursor_text` via `SlackCursor.model_validate_json` (overrides Phase 1 scaffold's "always pass None" path).
2. **Real `BridgeMCPClient`**: worker stops using `_NullMCPClient` for Slack; the bridge is grouped by `mcp_server`.
3. **`--since` plumbing**: worker passes `opts.since_unix` through to `SlackSource.cursor_from_timestamp`.
4. **`IngestContext.settings`**: drops the `# type: ignore` once worker passes the real `Config`.

The other Phase 1 follow-ups (`heartbeat_managed` unused; heartbeat sqlite3.Error silent swallow; `_persist_error_status` empty-cursor sentinel) are unrelated to this source and tracked separately.

---

## 8. Open decisions

- **Slack MCP server binary**: spec assumes "Official Slack MCP" per docs/01-architecture.md. Final tool name strings (e.g. `slack_conversations_history` vs `conversations_history`) confirmed at the start of the implementation plan after spinning up the MCP server locally. Plan will pin the binary version.
- **Bot/system messages**: spec defaults to ingesting all subtypes. If noise is a problem in practice, add a denylist in `config.toml`. Defer to operational feedback.
- **`reactions_json` / `files_json` shape**: stored as raw JSON strings per existing `SlackMetadata` model. No further normalization in this spec.

---

## 9. Out of scope

- Webhook receiver (`localhost:8787/slack/events`). Phase 2 orchestrator owns long-running HTTP per docs/05.
- Backfill via Slack export ZIP. Manual one-time op, separate spec.
- Multi-workspace support. Single workspace assumed.
- Reaction events as separate `events` rows. Not modeled in current schema.
- Editing/deletion replay. Slack `message_changed` / `message_deleted` not handled in Phase 1; cursor only moves forward.
