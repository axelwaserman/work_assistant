# 01 — Architecture

## Architecture summary

- **One Python process per role**, scheduled by `launchd` (macOS) or `systemd` (Linux). One long-running orchestrator, three short-lived workers (ingest, hygiene, notes-fetcher). One SQLite file as shared spine. No Docker, no message bus.
- **Claude Agent SDK (Python)** as the agent runtime. Orchestrator runs a Sonnet-class model. Advisors are sub-agents with restricted toolsets and namespaced memory.
- **AWS Bedrock as the model provider.** Configured per-process via env vars; see `08-bedrock-provider.md`.
- **SQLite (WAL + FTS5)** is the ingestion system of record. Every external signal lands as a row in `events` with a stable content hash.
- **Advisors propose, orchestrator commits.** Advisors return structured `Proposal` objects. The orchestrator validates, dedupes, and is the sole caller of the Todoist MCP write tools. Boundary enforced by tool whitelist.
- **Rules-first routing with LLM fallback.** A small Python dispatcher inspects Todoist labels, project paths, and event source before involving the LLM.
- **Cron-driven pull, two webhook exceptions.** 15-minute incremental sync for Slack/Gmail/Calendar/Todoist, nightly hygiene at 03:00 local, calendar-event-end trigger at meeting_end + 10min for Gemini notes, Slack Events webhook for direct mentions only.
- **Two-layer dedup.** SQLite `(source, source_id)` UNIQUE for ingestion idempotency. Todoist description footer `<!-- ws:hash:... -->` for cross-run task dedup.

## Process topology

```
+-------------------------------------------------------------+
|                     User's machine                          |
|                                                             |
|  +-----------------+        +----------------------------+  |
|  |  TUI chat       |<------>|  Orchestrator (long-run)   |  |
|  |  `wa chat`      |        |  - chat REPL               |  |
|  +-----------------+        |  - webhook receiver         |  |
|                             |  - advisor dispatcher       |  |
|                             |  - Todoist writer (sole)    |  |
|                             +-------------+--------------+  |
|                                           |                 |
|         +---------------------------------+--------------+  |
|         |                                 |              |  |
|         v                                 v              v  |
|  +-------------+              +---------------------+  +-------------+
|  | mcp-bridge  |              | spine.sqlite        |  | Anthropic / |
|  | (subproc    |              | (WAL + FTS5)        |  | Bedrock API |
|  |  manager)   |              +----------+----------+  +-------------+
|  +------+------+                         ^                  ^
|         |                                |                  |
|   +-----+------+----------+              |                  |
|   v            v          v              |                  |
| Todoist MCP  Slack MCP  Workspace MCP    |                  |
| (stdio)      (stdio)    (stdio)          |                  |
|         (Doist)      (off.)  (taylorwilsdon or off.)        |
|                                          |                  |
|  Triggered by launchd/systemd:           |                  |
|                                          |                  |
|  +-------------------+   every 15 min    |                  |
|  | ingest-worker     |-------------------+                  |
|  +-------------------+                   |                  |
|  +-------------------+   nightly 03:00   |                  |
|  | hygiene-worker    |-------------------+----------------> + (advisor calls allowed)
|  +-------------------+                   |
|  +-------------------+   per-meeting +10m|
|  | notes-fetcher     |-------------------+
|  +-------------------+                                      |
+-------------------------------------------------------------+
```

## Component inventory

| Name | Responsibility | Runtime | Lives in | Talks to |
|---|---|---|---|---|
| `orchestrator` | Terminal chat REPL, webhook receiver, advisor dispatch, sole Todoist writer | Python 3.12, long-running | launchd/systemd unit | SQLite, Bedrock, Todoist MCP, advisors |
| `ingest-worker` | Cron-triggered pulls from each source, writes to `events` | Python 3.12, short-lived | Triggered every 15 min | SQLite, Slack MCP, Workspace MCP, Todoist MCP (read) |
| `hygiene-worker` | Nightly: stale tasks, label sanity, embedding refresh, vacuum | Python 3.12, short-lived | Triggered nightly at 03:00 | SQLite, Todoist MCP (read), Bedrock |
| `notes-fetcher` | Triggered N min after meetings end, retrieves Gemini notes | Python 3.12, short-lived | Per-meeting one-shot timer | Workspace MCP (Calendar+Drive+Docs), SQLite |
| `slack-webhook` | Receives Slack Events for mentions/DMs, enqueues to SQLite | FastAPI, embedded in orchestrator | Bound to localhost:8787 + tunnel | Slack Events API, SQLite |
| `advisors/*` | Specialist sub-agents | Sub-agents inside orchestrator process | `~/.work_assistant/advisors/*.py` | SQLite (read), advisor private memory |
| `mcp-bridge` | Wrapper that launches MCP servers as subprocesses, multiplexes stdio | Python | Inside orchestrator and workers | The three MCP servers |
| `db` | SQLite file with WAL + FTS5 | File | `~/.work_assistant/db/spine.sqlite` | Everyone |
| `config` | TOML config + secrets | Files + Keychain | `~/.work_assistant/config.toml` | Everyone |
| `tui-chat` | Textual or `prompt_toolkit` REPL | Python | Entry point: `wa chat` | Orchestrator |

## MCP server choices

| Server | Choice | Reason |
|---|---|---|
| Todoist | `Doist/todoist-mcp` (official, hosted at `ai.todoist.net/mcp` or run via `npx @doist/todoist-mcp`) | First-party, broad tool coverage, actively maintained |
| Slack | Official Slack MCP | First-party, minimal scope creep |
| Workspace (Gmail/Calendar/Drive/Docs) | `taylorwilsdon/google_workspace_mcp` | Single OAuth flow covers all four; alternative is four separate official servers (decision in `07-open-decisions.md`) |

Run all MCP servers as **stdio subprocesses** managed by `mcp-bridge`. Do **not** expose any over the network. Each worker process spawns its own bridge instance to avoid stdio contention.

## Repository layout

```
work_assistant/
├── docs/                           # this folder
├── pyproject.toml
├── src/
│   └── work_assistant/
│       ├── __init__.py
│       ├── cli.py                  # `wa` entry point (chat, doctor, ingest, etc.)
│       ├── config.py               # TOML loader, Keychain access
│       ├── db/
│       │   ├── schema.sql          # canonical DDL
│       │   ├── migrations/         # versioned .sql files
│       │   └── connection.py       # sqlite3 wrapper, WAL setup
│       ├── mcp/
│       │   ├── bridge.py           # subprocess manager
│       │   └── clients/            # thin typed wrappers per MCP
│       ├── orchestrator/
│       │   ├── app.py              # long-running entry
│       │   ├── chat.py             # TUI loop
│       │   ├── webhook.py          # Slack Events receiver
│       │   ├── dispatcher.py       # rules-first router
│       │   └── committer.py        # the only Todoist writer
│       ├── advisors/
│       │   ├── base.py             # protocol + tool-grant enforcement
│       │   ├── manager_coaching.py
│       │   ├── stakeholder_comms.py
│       │   └── task_hygiene.py
│       ├── ingest/
│       │   ├── worker.py           # cron entry
│       │   ├── slack.py
│       │   ├── gmail.py
│       │   ├── calendar.py
│       │   ├── notes.py            # Gemini-notes fetcher
│       │   └── todoist_readback.py
│       └── observability/
│           ├── logging.py
│           └── metrics.py
├── scripts/
│   ├── launchd/                    # plist templates
│   └── systemd/                    # unit templates
└── tests/
    ├── fixtures/                   # source payload samples
    └── ...
```

## Provider config (Bedrock)

See `08-bedrock-provider.md` for full detail. Summary:

- Set `CLAUDE_CODE_USE_BEDROCK=1` and `AWS_REGION` in every worker process.
- Use Bedrock model IDs or inference profile ARNs in `config.toml`, not Anthropic model slugs.
- Sonnet handles orchestrator + most advisors; Opus reserved for explicit deep-reasoning tasks (e.g. weekly review). Both models must be enabled in the chosen region.

## Process supervision

- macOS dev box: `launchd` plists in `~/Library/LaunchAgents/`. Templates in `scripts/launchd/`.
- Linux: `systemd --user` units. Templates in `scripts/systemd/`.
- Orchestrator: `KeepAlive=true`, restart on crash with backoff.
- Workers: `StartCalendarInterval` / `OnCalendar` triggers, `RunAtLoad=false`.
- All processes log to `~/.work_assistant/logs/<process>.log` with size-based rotation.

## Concurrency model

- One writer to SQLite at a time, enforced by SQLite's WAL semantics.
- Workers acquire a per-name advisory lock via `INSERT OR IGNORE INTO worker_locks(name, pid, acquired_at)`. If the row exists with a live PID, exit cleanly.
- Orchestrator and workers share read access freely (WAL allows concurrent readers).
- MCP subprocesses are per-process; never shared across orchestrator/worker boundaries.
