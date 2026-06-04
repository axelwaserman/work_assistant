# 06 — Failure modes & mitigations

The design absorbs each of these without manual intervention. Where mitigation requires code, the relevant phase is noted.

| Failure | Mitigation | Phase |
|---|---|---|
| Slack rate-limit 429 mid-run | Per-source token bucket in `ingest-worker`; on 429, persist last successful cursor and exit cleanly with `last_status='partial'`. Next run resumes. | 1 |
| Gmail `historyId` expired (>7-day outage) | Detect 404 on `users.history.list`, fall back to full resync of last 14d via `users.messages.list?q=newer_than:14d`, log warning. | 1 |
| Calendar `syncToken` invalidated (>7-day idle) | Same pattern: on 410 Gone, full resync with `timeMin = now - 30d`. | 1 |
| Todoist webhook missed | Don't use Todoist webhooks for state. Sync polling with `sync_token` is the canonical path. | 1 |
| Advisor proposes a duplicate task | Two-layer dedup: (1) SQLite `(source, source_id)` UNIQUE prevents duplicate events; (2) `todoist_dedup` table check before commit. If duplicate, downgrade proposal to `no_op`. | 2 |
| Advisor hallucinates a commitment that wasn't made | Mandatory `event_id` citation in `rationale`; orchestrator validates the cited row exists; reject otherwise. Plus confidence threshold + daily auto-commit budget. | 2 |
| User manually edits a Todoist task | Footer hash mismatch → mark `metadata_json.user_modified=true` and never auto-modify. User-owned tasks become immutable from the system's perspective. | 2 |
| MCP subprocess crashes | `mcp-bridge` supervises with restart-and-backoff. Mark `last_status='error: <reason>'` on the affected cursor. `wa doctor` surfaces. | 0 |
| OAuth token expires | Refresh on 401, retry once. If refresh fails, surface in `wa doctor` and the chat banner. Worker exits cleanly without advancing cursor. | 0 |
| SQLite WAL grows unbounded | `PRAGMA wal_autocheckpoint=1000` on connection open. Nightly `VACUUM` in hygiene worker. | 0 / 4 |
| Bedrock API outage | Chat returns `[advisor offline]` state. Cron writes still flow into SQLite (no LLM needed). Advisors retry next cycle. | 0 |
| Bedrock 403 on Opus when only Sonnet enabled | Surface clearly in `wa doctor`. Orchestrator falls back to Sonnet for any explicitly-Opus advisor call, logs warning. | 0 |
| Gemini notes never arrive | Two-retry schedule (+10min, +30min). After two failures, status=`unavailable`, write stub event, stop. | 3 |
| User isn't the meeting organizer | Skip fetch; status=`not_organizer`, write stub event with `notes_unavailable=true`. Advisors see context exists but is unretrievable. | 3 |
| Cron jobs overlap (long-running prior run) | SQLite advisory lock via `worker_locks`. Existing-PID check before acquire; if alive, exit cleanly. | 0 |
| Slack Events webhook replay | Idempotent ingest via `UNIQUE(source, source_id)`. Slack retries become no-ops. | 3 |
| Cloudflare Tunnel down | Slack Events delivery fails → Slack retries up to 1h → eventually drops. **Fallback**: 15-minute polling still ingests mentions, just with up-to-15-minute latency. Webhook is optimization, not dependency. | 3 |
| Advisor times out / errors | Orchestrator drops all proposals from that run, logs, continues. No partial commits. Proposal log row records the error. | 2 |
| Advisor returns malformed JSON | Structured-output retry once (Bedrock supports tool-use schemas). On second failure, drop. | 2 |
| `memory_writes` fail mid-batch | Entire proposal batch rolled back. Response logged with `decision='rejected'`, `decision_reason='memory_write_failed'`. | 2 |
| User loses access to Slack workspace / Google account | Worker fails fast with auth error. `wa doctor` flags the broken source. Other sources continue. | 0 |
| Power loss / unclean shutdown mid-write | SQLite WAL guarantees atomicity at the transaction boundary. Workers wrap each batch in a transaction; partial-batch state is impossible. | 0 |
| Disk fills up (logs / WAL / DB) | Rotating logs (size cap), nightly VACUUM, WAL autocheckpoint. `wa doctor` reports DB size. | 0 / 4 |
| User OAuth scope drift (e.g. Google revokes access after policy change) | Worker fails with auth error, `wa doctor` flags, user re-runs `wa auth google`. | 0 |
| Hash collisions on `dedup_hash` | Hash is `sha256[:16]` = 64 bits. Collision probability negligible at our volumes. If it ever happens, resulting `no_op` is the worst outcome (false-positive dedup). Acceptable. | 2 |
| User runs system on multiple machines simultaneously | Out of scope. SQLite is single-machine. Document this constraint clearly in setup. | 0 |
| Timezone confusion between sources | All timestamps stored as UTC unix seconds. Display time uses system local TZ. Document explicitly. | 0 |

## Observability that matters

Three metrics worth watching — pick a fourth only if it would change behavior.

1. **Ingestion failure count, last 24h, per source.** If non-zero for 24+ hours, something is wrong. Surface in `wa stats` and the daily digest.
2. **Advisor accept rate, rolling 7-day, per advisor.** Drop below 80% for 3+ days → that advisor's prompt or trigger rules need work. Visible in `wa stats`.
3. **Review-queue depth.** If consistently > 20, the auto-commit threshold or budget needs adjustment. Visible in `wa stats` and digest.

## What we deliberately don't measure

- Token usage per advisor — unless cost becomes a concern, this is observability theater.
- Per-tool latency — MCP calls are fast and bounded; doesn't cause action.
- Cache hit rates — no cache layer in this design.

## Operational runbook (terse)

| Symptom | First check | Second check |
|---|---|---|
| `wa chat` returns "advisor offline" | `wa doctor` Bedrock check | AWS console: model access in region |
| Tasks stop appearing in Todoist | `wa doctor` Todoist auth | `proposals` table — are advisors running? |
| Slack mentions taking >15 min | Cloudflare Tunnel status | Webhook signature failure in logs |
| Gemini notes not arriving | Are you the meeting organizer? | `notes_fetch_queue.last_error` |
| Worker not running | launchd `list` / systemd `status` | `worker_locks` for stuck row with dead PID |
| Disk filling up | `du -sh ~/.work_assistant/` | rotate logs, run nightly hygiene manually |
