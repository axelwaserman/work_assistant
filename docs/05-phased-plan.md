# 05 — Phased delivery plan

Five phases. Each delivers standalone value. The user can stop after any phase and have something useful. Effort is in **days for one engineer working part-time** (≈3–4 focused hours/day).

| Phase | Goal | Effort |
|---|---|---|
| 0 | Foundations: skeleton, MCP plumbing, SQLite spine | 3–5 d |
| 1 | Read-only ingestion + chat | 5–7 d |
| 2 | Advisors come online | 5–8 d |
| 3 | Automation triggers | 3–5 d |
| 4 | Polish & observability | 2–4 d |
| **Total** | | **18–29 d** |

## Phase 0 — Foundations

**Goal**: prove MCP plumbing and SQLite spine work end-to-end. Nothing user-facing yet.

**Deliverables**:
- Repo layout per `01-architecture.md`.
- `pyproject.toml` with pinned dependencies (Claude Agent SDK, Pydantic, sqlite-vec for later, Textual or prompt_toolkit, FastAPI for webhook).
- Config loader (`src/work_assistant/config.py`): reads `~/.work_assistant/config.toml`, looks up secrets in macOS Keychain (or `pass` on Linux).
- Bedrock provider config wired up: `CLAUDE_CODE_USE_BEDROCK=1`, `AWS_REGION`, model IDs/inference profiles in TOML. Smoke-test with a one-shot Sonnet call.
- `mcp-bridge` (`src/work_assistant/mcp/bridge.py`): subprocess manager that boots the three MCP servers (Doist Todoist, Slack, Workspace), captures stderr, supports clean shutdown.
- SQLite migrations for `events`, `events_fts`, `ingest_cursors`, `worker_locks` (other tables in later phases).
- `wa doctor` command: checks Bedrock auth, MCP server boot, SQLite open, OAuth tokens valid.
- Structured logging to `~/.work_assistant/logs/<process>.log`.

**Exit criteria**:
- `wa doctor` passes on a clean machine after a documented setup procedure.
- Can manually invoke `slack.list_channels` and `todoist.get_tasks` through the bridge from a Python REPL.
- `events` table accepts a hand-written row and FTS5 trigger populates `events_fts`.
- One Bedrock Sonnet round-trip completes (echo prompt).

**Top risks**:
1. **MCP server stdio quirks on macOS** — pin versions, capture stderr per server, verify on a fresh machine. Mitigation: include stderr tail in `wa doctor` output.
2. **Workspace MCP OAuth flow** — the multi-scope flow can be finicky. Do this first, on a clean weekend, before anything else. Mitigation: a one-time `wa auth google` command that walks through it interactively.
3. **Bedrock model access** — Sonnet must be enabled in your region. Verify in the AWS console before writing code. Mitigation: `wa doctor` includes a test invocation that surfaces 403s clearly.

## Phase 1 — Read-only ingestion + chat

**Goal**: useful even if you never add an advisor. Ingest Slack/Gmail/Calendar/Todoist into SQLite, query via terminal chat.

**Deliverables**:
- `ingest-worker` (`src/work_assistant/ingest/worker.py`) with all four sources implemented per `04-ingestion-pipelines.md`.
- All cursors honored; first run does the configured backfill, subsequent runs are incremental.
- Per-source dedup tested with 30+ fixture-based unit tests.
- launchd plist (or systemd unit) for 15-minute schedule. Documented in `scripts/launchd/`.
- `wa chat` (`src/work_assistant/orchestrator/chat.py`): Textual or prompt_toolkit REPL. Sonnet-backed agent with **read-only** SQLite tools (`events.search_fts`, `events.get_by_id`, `events.list_by_actor`, `events.list_by_thread`).
- Chat answers cross-source questions like "what did Jane say last week about the Q3 plan?" with citations to `source_link`.
- Citation enforcement in the orchestrator's system prompt: every claim must cite an `event.id`. Responses without citations are rejected and the agent is asked to retry.

**Exit criteria**:
- 7 days of continuous ingestion with **zero duplicate rows** in `events`.
- `wa chat` answers cross-source questions with at least one citation per claim.
- A crash mid-batch (kill -9 on the worker) leaves cursors in a recoverable state — next run resumes without dupes or gaps.

**Top risks**:
1. **Dedup correctness** — write fixture-based unit tests for every source's `(source, source_id)` key before going live. Test cases: same message twice, edited message, deleted message, retry after 429.
2. **Cursor drift if a run crashes mid-batch** — wrap each batch in a single transaction; only commit cursor after all rows in batch are written. Mark `last_status='partial'` on partial completion (e.g. rate-limit-induced early exit).
3. **Chat hallucinating across sources** — system prompt forces `event.id` citation per claim; orchestrator post-validates the cited rows exist; reject and re-prompt on missing citations.

## Phase 2 — Advisors come online

**Goal**: advisors propose tasks, orchestrator commits them to Todoist with dedup.

**Deliverables**:
- Migrations for `advisor_memory`, `proposals`, `todoist_dedup`, `review_queue`.
- `AdvisorRequest` / `AdvisorResponse` Pydantic models.
- `advisors/base.py`: tool-grant enforcement, namespaced memory access, structured-output retries.
- Three advisors implemented: `manager_coaching.py`, `stakeholder_comms.py`, `task_hygiene.py`. Each with a tight system prompt and a small advisor-specific test set.
- `dispatcher.py`: rules-first router with LLM fallback per `03-advisor-protocol.md`.
- `committer.py`: the only Todoist writer. Implements all eight commit rules (validate shape, citations, dedup, confidence gate, daily budget, audit log, memory writes).
- Footer-based dedup on Todoist writes. Read-back parses footers and updates `todoist_dedup`.
- `wa proposals` CLI: list pending review-queue items, approve/reject/edit them, see audit log.

**Exit criteria**:
- One week of advisor-driven Todoist tasks with **<5% duplication** and **<10% user-rejection rate**.
- Every committed task is traceable: `proposals → trigger_event → events → source_link`.
- No advisor can call a Todoist write tool — verified by attempting it in a test (should fail with `ToolNotGranted`).

**Top risks**:
1. **Advisor hallucination creating fake commitments** — confidence threshold + daily auto-commit budget + mandatory event citation in `rationale`. The orchestrator validates citations exist. Low-confidence proposals queue for review, not auto-commit.
2. **Footer dedup races if user edits manually** — if recomputed hash doesn't match footer hash, mark `metadata_json.user_modified=true` and stop reconciling. User-owned tasks are immutable from the system's perspective.
3. **Advisor scope creep** — review the tool whitelist in code review. Each advisor's allow-list is a code-level constant, not config. Adding a tool requires a deliberate edit.

## Phase 3 — Automation triggers

**Goal**: real-time response, not just polling.

**Deliverables**:
- Slack Events webhook receiver (FastAPI, embedded in orchestrator). Listens on `localhost:8787`, exposed via Cloudflare Tunnel.
- Public ingress setup documented (`scripts/cloudflare-tunnel.md`).
- Webhook signature verification.
- Slack app manifest with the right scopes and event subscriptions (`app_mention`, `message.im`).
- `notes-fetcher` worker: drains `notes_fetch_queue`, implements the algorithm in `04-ingestion-pipelines.md`.
- Calendar ingest enqueues notes-fetch rows after meeting end.
- Nightly hygiene pass at 03:00: stale `@waiting-on`, missing labels, orphaned tasks, weekly review prompt every Monday.
- Hygiene worker invokes `task-hygiene` advisor for nightly run.

**Exit criteria**:
- Slack mention → Todoist task within **60 seconds**.
- Meeting ends → notes ingested within **15 minutes** when user is the organizer.
- Two consecutive nightly runs of hygiene worker complete without errors.

**Top risks**:
1. **Cron drift / overlapping runs** — SQLite-backed advisory lock per worker name; if existing PID is alive, exit cleanly. Already supported by `worker_locks` schema.
2. **Webhook replay** — Slack retries on non-200. The `(source, source_id)` UNIQUE index makes ingest idempotent. Verified with a replay test.
3. **Tunnel uptime** — if Cloudflare Tunnel is down, mentions still get picked up by the next 15-minute poll. Webhook is an optimization, not a dependency.

## Phase 4 — Polish & observability

**Goal**: trust the system enough to leave it alone for a week.

**Deliverables**:
- `wa stats`: events/day per source, proposals/day per advisor, accept rate per advisor, last-run timestamp per worker.
- Local error sink: log aggregation to a single rotating file with structured JSON. Optional: forward to Sentry self-hosted if user wants.
- Daily digest email-to-self at 18:00: what the system did, what's queued for review, anomalies.
- sqlite-vec extension: nightly batch embedding of `events.body where length > 200`. New `events.search_semantic` tool for chat.
- `wa memory <advisor>`: dump advisor memory namespace, support edit/delete for grooming.
- Nightly `VACUUM` + WAL checkpoint in hygiene worker.

**Exit criteria**:
- User stops opening the SQLite file directly to debug.
- `wa stats` answers "is the system healthy?" at a glance.
- One week of the daily digest is informative without being noisy.

**Top risks**:
1. **Embedding cost** — batch nightly only on bodies > 200 chars; Bedrock Titan or Cohere embeddings via Bedrock are cheap. Cap at 5,000 events/night.
2. **Observability theater** — pick three metrics that would actually cause action: ingestion failure count, advisor accept rate trend, review-queue depth. Ignore the rest.

## Acceptance gates between phases

Before moving from Phase N to Phase N+1, the **exit criteria** for Phase N must be met for at least 7 consecutive days of real use. No exceptions: this is a personal system, but the value of phased delivery comes from actually living with each phase.
