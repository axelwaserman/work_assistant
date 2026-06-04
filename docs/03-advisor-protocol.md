# 03 — Advisor protocol

The contract between the orchestrator and specialist sub-agents. The boundary defined here is the most load-bearing part of the system: it is what keeps advisors from hallucinating tasks straight into Todoist.

## Initial advisor roster

| Advisor | Domain | Triggers |
|---|---|---|
| `manager-coaching` | Direct-report 1:1 prep, growth conversations, feedback follow-up, performance signals | Events tied to `direct-reports/*` projects, calendar events with reports, Slack DMs from reports |
| `stakeholder-comms` | Cross-functional updates, exec asks, partner threads, narrative framing | Events tied to `peers/*` or `leadership/*` projects, emails from VP+, calendar with execs |
| `task-hygiene` | Stale tasks, missing labels, duplicate detection, snooze suggestions, weekly review | Nightly hygiene run, Todoist `task_state` events |

Adding an advisor is a deliberate choice, not an emergent feature. Each new advisor must justify itself against the routing rules below.

## Tool grants

The boundary is enforced by an explicit allow-list passed when the orchestrator spawns the advisor sub-agent. The advisor's runtime cannot call tools outside this list.

| Tool | manager-coaching | stakeholder-comms | task-hygiene |
|---|---|---|---|
| `sqlite.read` (parameterized SELECT against `events`, `events_fts`, `proposals`) | yes | yes | yes |
| `memory.read` (own namespace only) | yes | yes | yes |
| `memory.write` (own namespace only) | yes | yes | yes |
| `todoist.*` writes | **no** | **no** | **no** |
| `slack.*`, `gmail.*`, `calendar.*` writes | **no** | **no** | **no** |
| `bedrock.invoke_other_advisor` | no | no | no |

Advisors **never** get write access to external systems. Period. Every external write goes through the orchestrator.

## Input contract — `AdvisorRequest`

```python
# Pseudocode shape; canonical Pydantic model lives in src/work_assistant/advisors/base.py
AdvisorRequest = {
  "advisor": "manager-coaching",
  "request_id": "uuid-v4",                 # for tracing
  "trigger": {
    "kind": "event" | "chat" | "schedule",
    "event_id": int | None,                # set when kind == 'event'
    "user_message": str | None,            # set when kind == 'chat'
    "schedule_name": str | None,           # set when kind == 'schedule', e.g. 'nightly-hygiene'
  },
  "context": {
    "recent_events": [event_id, ...],      # orchestrator pre-fetches a windowed slice
    "related_tasks": [todoist_id, ...],    # tasks the dispatcher considers relevant
    "advisor_memory_keys": [key, ...],     # orchestrator pre-loads these; advisor can request more via memory.read
  },
  "tool_grants": ["sqlite.read", "memory.read", "memory.write"],
  "budget": {
    "max_tokens": 8000,                    # hard cap
    "max_proposals": 5,                    # cap on proposals returned
  },
}
```

Pre-fetching context in the orchestrator is deliberate: it bounds advisor cost, keeps decisions auditable, and prevents advisors from "exploring" the database unboundedly.

## Output contract — `AdvisorResponse`

```python
AdvisorResponse = {
  "request_id": "uuid-v4",                 # echo of input
  "summary": str,                          # 1-3 sentences, shown in chat
  "proposals": [
    {
      "kind": "create_task" | "update_task" | "comment" | "no_op",
      "project": "direct-reports/jane",    # required for create_task
      "todoist_id": "...",                 # required for update_task / comment
      "content": "1:1 prep — discuss Q3 ramp",
      "description": "...",                # without the footer; orchestrator appends
      "labels": ["@advisor/manager-coaching", "@source/calendar"],
      "due": "2026-06-03",                 # ISO date or null
      "source_links": ["https://..."],     # at least one required for create_task
      "dedup_hash": "9f2c8a...",           # advisor computes; orchestrator verifies
      "confidence": 0.0..1.0,
      "rationale": "why this proposal exists, with cited event_ids",
    }
  ],
  "memory_writes": [
    {"key": "report:jane:last-1on1-themes", "value": {...}}
  ],
  "needs_human": bool,                     # if true, orchestrator surfaces in chat instead of auto-committing
}
```

### Required fields in `rationale`

The rationale string must:
- Cite at least one `event_id` from the request's `recent_events` (or one fetched via `sqlite.read`). The orchestrator validates the citation exists.
- State the chain of inference in plain English. No jargon, no "vibes".

If `rationale` lacks a citation, the proposal is auto-rejected with `decision_reason='missing_citation'`.

## Routing — orchestrator dispatcher

```
event arrives  →  ingest-worker writes to events table
                  ↓
                  orchestrator dispatcher (rules-first):
                    1. Todoist label `@advisor/X` present on a related task?  → route to X
                    2. Project path matches `direct-reports/*`               → manager-coaching
                    3. Project path matches `peers/*` or `leadership/*`      → stakeholder-comms
                    4. Source = todoist (state change)                       → task-hygiene
                    5. Schedule trigger = nightly-hygiene                    → task-hygiene
                    6. else: ask Sonnet to classify (cheap structured-output call, 3-way pick)
                  ↓
                  spawn advisor sub-agent with AdvisorRequest
```

LLM classification fallback:
- Single Bedrock call with a 3-option structured output (one of the three advisors, or `none`).
- If `none`, the event is logged and dropped — no advisor invoked.
- This call uses the cheapest available Sonnet tier; cap at 256 output tokens.

## Commit rules — orchestrator-enforced

For every proposal returned by an advisor:

1. **Validate shape.** Required fields per `kind`. Reject malformed proposals with `decision='rejected'`, `decision_reason='malformed'`.
2. **Validate citations.** Every cited `event_id` in `rationale` must exist in `events`. Reject otherwise.
3. **Verify dedup hash.** Recompute `sha256(normalized_content)[:16]` and compare to advisor's `dedup_hash`. If mismatch, recompute and use orchestrator's value (don't trust the advisor's).
4. **Check `todoist_dedup`.** If hash already present, downgrade to `no_op` with `decision_reason='already_exists'`.
5. **Confidence gate.** If `confidence < threshold` (default 0.6, configurable), or `needs_human == true`, enqueue in `review_queue` instead of committing.
6. **Daily auto-commit budget.** If today's `committed` count exceeds the configured ceiling (default 20), enqueue in `review_queue`.
7. **Commit.** For `create_task`: build description = `proposal.description + footer`, call Todoist MCP create. For `update_task`: validate user hasn't manually edited (footer intact), then call update. For `comment`: append a comment, never modifying the task body.
8. **Audit.** Append to `proposals` with `decision`, `decision_reason`, and resulting `todoist_id` if any.
9. **Persist memory writes.** `memory_writes` from the response are applied after a successful commit, atomically with the proposal row insert.

## Memory namespacing

- Storage: `advisor_memory(advisor TEXT, key TEXT, value_json TEXT, updated_at INTEGER)`. Composite PK `(advisor, key)`.
- Access tools (`memory.read`, `memory.write`) automatically inject `WHERE advisor = ?` with the advisor's name. Implemented in `advisors/base.py`; advisors never see raw SQL.
- Suggested key conventions (per advisor):
  - `manager-coaching`: `report:<name>:last-1on1-themes`, `report:<name>:growth-areas`, `report:<name>:open-feedback`.
  - `stakeholder-comms`: `stakeholder:<email>:context`, `thread:<gmail_thread_id>:status`.
  - `task-hygiene`: `pattern:stale-pattern-v1`, `last-weekly-review-at`.

## Failure semantics

- An advisor that times out or errors: orchestrator logs, drops all its proposals from the run, and continues. No partial proposals are committed.
- An advisor that returns malformed JSON: structured-output retry once, then drop.
- A `memory.write` that fails: the entire proposal batch is rolled back; the response is logged with `decision='rejected'`, `decision_reason='memory_write_failed'`.

## Observability

- Every `AdvisorRequest` / `AdvisorResponse` pair logged to `~/.work_assistant/logs/advisors.jsonl`, one JSON object per line.
- `wa proposals` CLI: list pending review queue items, recent committed proposals, per-advisor accept rate.
- `wa memory <advisor>` CLI: dump the advisor's memory namespace for inspection.
