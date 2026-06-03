# 02 — Data model

The spine is a single SQLite file at `~/.work_assistant/db/spine.sqlite`, opened with `journal_mode=WAL` and `synchronous=NORMAL`. All tables here. Migrations live in `src/work_assistant/db/migrations/` as numbered SQL files.

## Pragmas (set on every connection open)

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA wal_autocheckpoint = 1000;
PRAGMA busy_timeout = 5000;
```

## Schema

### `events` — every observed external signal

```sql
CREATE TABLE events (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  source          TEXT NOT NULL,           -- 'slack' | 'gmail' | 'calendar' | 'drive' | 'todoist' | 'gemini_notes'
  source_id       TEXT NOT NULL,           -- stable per-source ID
  source_link     TEXT,                    -- canonical URL to the artifact
  content_hash    TEXT NOT NULL,           -- sha256 of normalized body
  occurred_at     INTEGER NOT NULL,        -- unix ts of source event
  ingested_at     INTEGER NOT NULL,
  actor           TEXT,                    -- email or per-source user id
  thread_key      TEXT,                    -- slack thread_ts | gmail thread_id | gcal event_id
  kind            TEXT NOT NULL,           -- 'message' | 'email' | 'meeting' | 'doc' | 'task_state'
  title           TEXT,
  body            TEXT,
  metadata_json   TEXT,                    -- source-specific extras
  UNIQUE(source, source_id)
);
CREATE INDEX idx_events_occurred ON events(occurred_at DESC);
CREATE INDEX idx_events_thread   ON events(thread_key);
CREATE INDEX idx_events_actor    ON events(actor);
CREATE INDEX idx_events_source_kind ON events(source, kind);
```

The `UNIQUE(source, source_id)` index is the **first dedup layer** — re-ingesting the same Slack message is an `INSERT OR IGNORE` no-op.

### `events_fts` — full-text search mirror

```sql
CREATE VIRTUAL TABLE events_fts USING fts5(
  title, body,
  content=events,
  content_rowid=id,
  tokenize='porter unicode61'
);

-- Triggers keep FTS in sync
CREATE TRIGGER events_ai AFTER INSERT ON events BEGIN
  INSERT INTO events_fts(rowid, title, body) VALUES (new.id, new.title, new.body);
END;
CREATE TRIGGER events_ad AFTER DELETE ON events BEGIN
  INSERT INTO events_fts(events_fts, rowid, title, body) VALUES('delete', old.id, old.title, old.body);
END;
CREATE TRIGGER events_au AFTER UPDATE ON events BEGIN
  INSERT INTO events_fts(events_fts, rowid, title, body) VALUES('delete', old.id, old.title, old.body);
  INSERT INTO events_fts(rowid, title, body) VALUES (new.id, new.title, new.body);
END;
```

### `ingest_cursors` — per-source incremental position

```sql
CREATE TABLE ingest_cursors (
  source       TEXT PRIMARY KEY,
  cursor       TEXT NOT NULL,              -- slack: oldest ts; gmail: historyId; calendar: syncToken; todoist: sync_token
  updated_at   INTEGER NOT NULL,
  last_status  TEXT                        -- 'ok' | 'partial' | 'error: ...'
);
```

A worker that fails mid-batch persists the last successfully-processed cursor and sets `last_status='partial'`. The next run resumes from there.

### `embeddings` — Phase 4

```sql
CREATE TABLE embeddings (
  event_id     INTEGER PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
  model        TEXT NOT NULL,
  vector       BLOB NOT NULL,              -- raw float32 bytes; sqlite-vec extension
  created_at   INTEGER NOT NULL
);
```

Use the `sqlite-vec` extension. Batch-embed nightly only on `body IS NOT NULL AND length(body) > 200` to stay cheap.

### `advisor_memory` — per-advisor private store

```sql
CREATE TABLE advisor_memory (
  advisor      TEXT NOT NULL,              -- 'manager-coaching' | 'stakeholder-comms' | 'task-hygiene'
  key          TEXT NOT NULL,              -- advisor-defined namespace
  value_json   TEXT NOT NULL,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (advisor, key)
);
```

Access is wrapped by the orchestrator's `memory.read` / `memory.write` tools, which inject `advisor = self.name` automatically. No advisor can read another's memory.

### `proposals` — audit log

```sql
CREATE TABLE proposals (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  advisor         TEXT NOT NULL,
  trigger_event   INTEGER REFERENCES events(id),
  proposal_json   TEXT NOT NULL,
  decision        TEXT NOT NULL,           -- 'committed' | 'rejected' | 'deferred'
  decision_reason TEXT,
  todoist_id      TEXT,                    -- if committed and resulted in a task
  created_at      INTEGER NOT NULL
);
CREATE INDEX idx_proposals_advisor ON proposals(advisor, created_at DESC);
CREATE INDEX idx_proposals_decision ON proposals(decision, created_at DESC);
```

Every advisor proposal — accepted, rejected, or deferred — appends a row. This is the auditability backbone.

### `todoist_dedup` — second dedup layer

```sql
CREATE TABLE todoist_dedup (
  content_hash    TEXT PRIMARY KEY,
  todoist_id      TEXT NOT NULL,
  seen_at         INTEGER NOT NULL
);
```

Populated from two sources:
1. Every successful Todoist write writes its hash here.
2. Todoist read-back parses the description footer and upserts any `ws:hash:` it finds, so manually-created tasks (or restored backups) are honored.

### `worker_locks` — advisory locking

```sql
CREATE TABLE worker_locks (
  name         TEXT PRIMARY KEY,           -- 'ingest-worker' | 'hygiene-worker' | 'notes-fetcher:<event_id>'
  pid          INTEGER NOT NULL,
  acquired_at  INTEGER NOT NULL
);
```

A worker `INSERT OR IGNORE`s its row. If the existing row's PID is alive, exit cleanly. On clean shutdown, delete the row.

### `notes_fetch_queue` — pending Gemini notes fetches

```sql
CREATE TABLE notes_fetch_queue (
  calendar_event_id  TEXT PRIMARY KEY,
  scheduled_for      INTEGER NOT NULL,    -- unix ts of fetch attempt
  attempts           INTEGER NOT NULL DEFAULT 0,
  status             TEXT NOT NULL,       -- 'pending' | 'fetched' | 'unavailable' | 'not_organizer'
  last_error         TEXT
);
```

The calendar ingest writes rows here when meetings end. The notes-fetcher worker drains them.

### `review_queue` — low-confidence proposals awaiting human approval

```sql
CREATE TABLE review_queue (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  proposal_id     INTEGER NOT NULL REFERENCES proposals(id),
  enqueued_at     INTEGER NOT NULL,
  reviewed_at     INTEGER,
  decision        TEXT,                    -- 'approve' | 'reject' | 'edit'
  edited_payload  TEXT
);
```

Surfaced via `wa proposals` CLI and inline in chat.

## Todoist taxonomy

### Projects

Project paths are routing signals. Suggested baseline:

- `inbox` — default landing zone for proposals without a clear project.
- `direct-reports/<name>` — one per direct report.
- `peers/<name>` — selectively, for peers with high-context follow-up volume.
- `leadership/<name>` — for stakeholder-comms-relevant interactions.
- `personal` — non-work commitments routed off the system's hot path.
- `ops` — recurring operational tasks the system manages (e.g. weekly review).

The dispatcher uses the first path segment to route advisors:

| Project prefix | Default advisor |
|---|---|
| `direct-reports/` | manager-coaching |
| `peers/` or `leadership/` | stakeholder-comms |
| anything else | LLM fallback |

### Labels

Labels are **semantic, not state**. Todoist's own due-date and priority handle state.

- `@advisor/manager-coaching`, `@advisor/stakeholder-comms`, `@advisor/task-hygiene` — explicit advisor pin (overrides project-based routing).
- `@source/slack`, `@source/gmail`, `@source/calendar`, `@source/notes` — origin.
- `@energy/deep`, `@energy/shallow` — used by hygiene worker for scheduling suggestions.
- `@waiting-on` — blocked, eligible for follow-up nudges by hygiene.

### Description footer convention

Every task created by the system ends its description with:

```
---
<!-- ws:hash:9f2c8a... ws:source:slack ws:source_id:1717123456.000200 ws:link:https://example.slack.com/archives/C123/p1717123456000200 -->
```

Rules:
- Footer is the last line (or block) of `description`. Strict regex: `<!-- ws:(\w+):(\S+)(?:\s+ws:\w+:\S+)* -->`.
- Parsed on every Todoist read-back; values upserted into `todoist_dedup`.
- If a user manually edits the task and breaks the footer, treat as user-owned: skip dedup checks and never auto-modify.
- The hash is `sha256(normalized_content)[:16]` where `normalized_content = strip_whitespace(lowercase(content + "\n" + first_line_of_description))`.

## Source-link convention

Every `events.source_link` and every Todoist task's `ws:link:` footer entry is a canonical URL the user can click to reach the originating artifact.

| Source | URL shape |
|---|---|
| Slack | `https://<workspace>.slack.com/archives/<channel_id>/p<ts_no_dot>` |
| Gmail | `https://mail.google.com/mail/u/0/#inbox/<message_id>` |
| Calendar | `https://calendar.google.com/calendar/u/0/r/eventedit/<base64_event_id>` |
| Drive doc | `https://docs.google.com/document/d/<doc_id>` |
| Todoist | `https://todoist.com/showTask?id=<task_id>` |
