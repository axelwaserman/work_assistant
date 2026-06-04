# 04 — Ingestion pipelines

One subsection per source. Each describes: trigger, MCP/API used, OAuth scopes, dedup key, transform-to-event shape, sharp edges.

The general pattern: **incremental cursor → fetch batch → normalize → INSERT OR IGNORE → commit cursor**. A failure mid-batch must leave the cursor at the last successfully-processed item so the next run resumes cleanly.

## Common normalization

Before writing to `events`, every record is normalized:
- `body`: stripped, decoded HTML/markdown to text, max 100 KB. If larger, store first 100 KB and set `metadata_json.truncated=true`.
- `actor`: resolved to email when possible. Slack `user_id` resolved via cached `users.info` (refreshed weekly).
- `occurred_at`: unix ts (seconds), UTC.
- `content_hash`: `sha256(source + ":" + source_id + ":" + normalized_body)`. Used by advisors to compute proposal dedup hashes deterministically.

## 4.1 Slack

| Field | Value |
|---|---|
| Trigger | Cron every 15 min for incremental channel pulls; webhook (Events API) for direct mentions and DMs |
| MCP / API | Slack MCP for batch reads; raw Events API webhook for real-time mentions (MCP doesn't deliver push) |
| OAuth scopes | `channels:history`, `groups:history`, `im:history`, `mpim:history`, `users:read`, `app_mentions:read` |
| Dedup key | `(slack, <channel_id>:<ts>)` |
| Cursor | Per-channel `oldest` ts. Stored as JSON map in `ingest_cursors.cursor` for `source='slack'`. |

### Event shape

```
kind          = 'message'
source_link   = permalink (slack API: chat.getPermalink)
thread_key    = thread_ts if threaded, else ts
actor         = resolved email (fallback: user_id)
title         = first 80 chars of text
body          = full text, with @mentions resolved to display names
metadata_json = {
  channel: { id, name, is_im, is_mpim },
  reactions: [...],
  files: [...],
  is_dm: bool,
  is_mention: bool,        // true if user is @-mentioned
}
```

### Sharp edges

- **Post-May-2025 history limit**: non-Marketplace apps see ~90 days of channel history. Backfill once via Slack export (manual workspace export), then run incremental forward only.
- **Per-channel pull cap**: 200 messages per run. Channels with more get the next batch on the following run. Prevents single-channel runs from starving others.
- **User resolution**: cache `users.info` weekly. New users may take up to a week to resolve to an email — fall back to `user_id` in the meantime.
- **Threads**: `conversations.history` returns top-level messages. Thread replies require `conversations.replies` per parent. For Phase 1, fetch replies only for threads where the user participated (cuts cost ~80%).
- **Rate limits**: Tier 3 (~50/min) for `conversations.history`. Implement a token-bucket limiter per workspace; on 429, persist cursor and exit cleanly.

### Webhook path (mentions)

- Endpoint: `localhost:8787/slack/events`.
- Public ingress: Cloudflare Tunnel (decision in `07-open-decisions.md`).
- Verifies request signature with `SLACK_SIGNING_SECRET`.
- Inserts directly into `events` with `metadata_json.is_mention=true` and `metadata_json.via_webhook=true`.
- Slack retries on non-200 within 3s; idempotent because of `UNIQUE(source, source_id)`.

## 4.2 Gmail

| Field | Value |
|---|---|
| Trigger | Cron every 15 min |
| MCP / API | `taylorwilsdon/google_workspace_mcp` (preferred) or direct Gmail API |
| OAuth scopes | `gmail.readonly` only |
| Dedup key | `(gmail, <message_id>)` |
| Cursor | `historyId` from last successful sync, in `ingest_cursors.cursor` for `source='gmail'`. |

### Event shape

```
kind          = 'email'
source_link   = https://mail.google.com/mail/u/0/#inbox/<message_id>
thread_key    = thread_id
actor         = from address (lowercased)
title         = subject
body          = plain-text part (or HTML→text fallback)
metadata_json = {
  to: [...],
  cc: [...],
  labels: [...],          // gmail labels, e.g. INBOX, IMPORTANT
  has_attachments: bool,
  internal_date: int,
}
```

### Sharp edges

- **Cursor is `historyId`, not query-by-date.** Query-by-date misses edits and label changes. `historyId` gives a strict event log via `users.history.list`.
- **`historyId` expires** if the account hasn't synced for >7 days. On 404 from `users.history.list`, fall back to a 14-day full resync via `users.messages.list?q=newer_than:14d`, log a warning.
- **Scope discipline**: `gmail.readonly` only. The orchestrator never sends email; "respond later" tracking goes to Todoist.
- **MCP capability check (Phase 0)**: verify `taylorwilsdon/google_workspace_mcp` exposes `historyId`-based incremental fetching. If not, drop to direct Gmail API for this source only.

## 4.3 Google Calendar

| Field | Value |
|---|---|
| Trigger | Cron every 15 min for `syncToken`-based pull; per-event one-shot timer for `meeting_end + 10min` (notes-fetcher) |
| MCP / API | Workspace MCP |
| OAuth scopes | `calendar.readonly` |
| Dedup key | `(calendar, <event_id>:<updated>)` |
| Cursor | `syncToken` per calendar, in `ingest_cursors.cursor` for `source='calendar'`. |

### Event shape

```
kind          = 'meeting'
source_link   = https://calendar.google.com/calendar/u/0/r/eventedit/<base64_id>
thread_key    = event_id
actor         = organizer email
title         = summary
body          = description (plain text)
metadata_json = {
  start: ISO8601,
  end: ISO8601,
  attendees: [{email, response_status}],
  hangout_link: str,
  attachments: [{title, file_id, file_url, mime_type}],   // includes Gemini notes doc when ready
  is_organizer: bool,
  conference_data: {...},
}
```

### Notes-fetcher queue

When a meeting event ends (detected at next ingest run after `now > end`), the calendar pipeline writes a row to `notes_fetch_queue`:

```sql
INSERT INTO notes_fetch_queue (calendar_event_id, scheduled_for, attempts, status)
VALUES (?, strftime('%s','now') + 600, 0, 'pending');
```

The notes-fetcher worker drains this queue.

### Sharp edges

- **`syncToken` invalidates after >7 days idle.** On 410 Gone, full resync with `timeMin = now - 30d`.
- **Multiple calendars**: store cursor per calendar. The user's primary plus any subscribed calendars they want ingested (configurable).
- **Recurring events**: event_id stays stable across instances; use `recurringEventId` in metadata to group.

## 4.4 Gemini meeting notes

| Field | Value |
|---|---|
| Trigger | Per-meeting one-shot timer at `meeting_end + 10min`, drained from `notes_fetch_queue` |
| MCP / API | Calendar MCP (refetch event for attachments) → Drive/Docs MCP (fetch content) |
| OAuth scopes | `drive.readonly`, `documents.readonly` |
| Dedup key | `(gemini_notes, <doc_id>:<revision_id>)` |
| Cursor | Not applicable (per-meeting one-shot) |

### Algorithm

```
1. Pop oldest pending row from notes_fetch_queue where scheduled_for <= now.
2. If user is not organizer of the calendar event:
     status = 'not_organizer'
     write stub event with metadata_json.notes_unavailable=true
     done.
3. Refetch calendar event. Inspect attachments for a Google Doc whose title starts with "Notes by Gemini" or matches the event summary.
4. If found:
     fetch doc content via Docs API.
     write event with kind='doc', thread_key=calendar_event_id.
     status = 'fetched'.
5. If not found and attempts < 2:
     reschedule for now + 20min.
     attempts += 1.
6. If not found and attempts >= 2:
     status = 'unavailable'.
     write stub event with metadata_json.notes_unavailable=true.
```

### Event shape

```
kind          = 'doc'
source_link   = https://docs.google.com/document/d/<doc_id>
thread_key    = calendar_event_id        // links notes back to the meeting
actor         = organizer email
title         = doc title
body          = doc content (plain text export)
metadata_json = {
  doc_id, revision_id,
  calendar_event_id,
  generated_by: 'gemini',
}
```

### Sharp edges

- **Only the organizer's Drive holds the canonical Doc.** If user wasn't the organizer, write a stub event so advisors know context exists but is unretrievable.
- **Notes can take 10–30 min to materialize.** The two-retry schedule (+10min, +30min) covers the typical case. Beyond that, give up.
- **Title heuristics**: Gemini's auto-generated docs typically start with "Notes by Gemini -" but this can be edited. Fall back to fuzzy match against event summary if no clean prefix match.

## 4.5 Todoist read-back

| Field | Value |
|---|---|
| Trigger | Cron every 15 min, plus immediately after any orchestrator write |
| MCP / API | `Doist/todoist-mcp`, Sync API with `sync_token` |
| OAuth scopes | `data:read_write` (write needed for orchestrator commits, not for read-back) |
| Dedup key | `(todoist, <item_id>:<updated>)` |
| Cursor | Todoist `sync_token` in `ingest_cursors.cursor` for `source='todoist'` |

### Event shape

```
kind          = 'task_state'
source_link   = https://todoist.com/showTask?id=<task_id>
thread_key    = task_id
actor         = collaborator email (for shared projects); else user
title         = task content
body          = task description (without footer for hashing; with footer stored in metadata)
metadata_json = {
  project_id, project_path,
  labels: [...],
  due: {date, datetime, recurring},
  priority: 1..4,
  completed: bool,
  parent_id: str | null,
  ws_footer: { hash, source, source_id, link }   // parsed from description
}
```

### Sharp edges

- **Webhooks fire-and-forget** with no replay. Do not rely on them. Sync polling is canonical.
- **Self-imposed budget**: 200 req/15min (well below Todoist's 450 ceiling) to leave headroom for orchestrator writes.
- **Footer parsing**: regex on description. If a `ws:hash:` is found, upsert into `todoist_dedup`. Tolerate variations in spacing.
- **User-edited tasks**: if hash in footer doesn't match recomputed content hash, treat as user-owned. Mark in `metadata_json.user_modified=true` and skip future auto-modifications.

## Backfill horizon

Phase 1 first run backfills with these defaults (configurable per source in `config.toml`):

| Source | Default backfill |
|---|---|
| Slack | 30 days (limited by post-May-2025 history caps for non-Marketplace apps; full backfill via export is a separate one-time op) |
| Gmail | 90 days |
| Calendar | 60 days |
| Todoist | full snapshot (Sync API gives this on a fresh `sync_token=*` call) |

Final values are an open decision in `07-open-decisions.md`.
