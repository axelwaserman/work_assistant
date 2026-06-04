# 00 — Overview

## Purpose

A single-user, local-first agentic productivity system. It ingests signals from the user's working environment (Slack, Gmail, Google Calendar, Gemini meeting notes), proposes and writes tasks to Todoist, and exposes specialist advisor agents (manager-coaching, stakeholder-communications, task-hygiene) consultable via a terminal chat.

## Goals

- **Todoist is the single system of record for tasks.** Every commitment the user tracks lives there. The agent layer surrounds Todoist; it never replaces it.
- **SQLite is the system of record for ingested events.** All external signals normalize into one local store. The agent reads from SQLite, not from APIs, on the hot path.
- **Advisors propose, orchestrator commits.** Specialist sub-agents return structured proposals. Only the orchestrator can write to Todoist. This boundary is enforced via tool whitelisting.
- **Local-first, private by default.** All data and credentials stay on the user's machine. No cloud state, no shared backend.
- **Useful at every phase.** Phase 1 (read-only ingestion + chat) is valuable on its own. Phase 2 (advisors) layers on top. Each phase ships standalone value.

## Non-goals

- **Not a team product.** No multi-user support, no shared state, no auth surface beyond OAuth to source APIs.
- **Not a Todoist replacement.** Task UX stays in Todoist proper (mobile, web, desktop). The agent is an automation and consultation layer.
- **Not real-time for everything.** 15-minute polling is the default cadence. Webhooks are used only where latency demonstrably matters (Slack mentions, post-meeting notes fetch).
- **Not a general-purpose assistant.** Scope is bounded to the four sources listed and the three initial advisors. Adding more is a deliberate design choice, not an emergent feature.

## High-level shape

```
User (terminal chat)
       |
       v
Orchestrator (Claude Sonnet via Bedrock)
   |          \
   |           +--> Advisors (sub-agents, sandboxed)
   |                  - manager-coaching
   |                  - stakeholder-communications
   |                  - task-hygiene
   |
   +--> Todoist MCP  (sole writer)
   +--> SQLite spine (read on hot path)

Background workers
   - ingest-worker   (every 15 min)
   - hygiene-worker  (nightly 03:00)
   - notes-fetcher   (per-meeting, +10 min)
   |
   +--> SQLite spine (writes)
   +--> Slack MCP / Workspace MCP / Todoist MCP (reads)
```

## Glossary

- **Orchestrator** — long-running Python process. Hosts the terminal chat, dispatches advisors, performs all Todoist writes, owns the webhook receiver.
- **Advisor** — a Claude sub-agent with a narrow domain (e.g. manager coaching). Has read-only access to SQLite and namespaced private memory. Cannot write to Todoist.
- **Spine** — `~/.work_assistant/db/spine.sqlite`. Single SQLite file with WAL + FTS5. Holds events, ingest cursors, embeddings, advisor memory, proposal audit log, and the Todoist dedup index.
- **Event** — one row in `events` representing a single observed external signal (a Slack message, an email, a meeting, a doc, a Todoist state change).
- **Proposal** — an advisor's structured suggestion: create task, update task, add comment, or no-op. Validated and committed (or rejected) by the orchestrator.
- **Footer** — a machine-managed line at the end of every Todoist task description: `<!-- ws:hash:... ws:source:... ws:source_id:... ws:link:... -->`. Used for dedup and provenance.
- **MCP server** — Model Context Protocol server. A subprocess that exposes external APIs as tools to the agent. We use three: Doist Todoist, Slack, Workspace (Gmail/Calendar/Drive/Docs).

## Reading order

1. `00-overview.md` (this file) — context.
2. `01-architecture.md` — components and process topology.
3. `02-data-model.md` — SQLite schema and Todoist taxonomy.
4. `03-advisor-protocol.md` — the contract between orchestrator and advisors.
5. `04-ingestion-pipelines.md` — per-source ingestion specs.
6. `05-phased-plan.md` — what gets built in what order.
7. `06-failure-modes.md` — what goes wrong and how the design absorbs it.
8. `07-open-decisions.md` — questions to answer before Phase 1.
9. `08-bedrock-provider.md` — provider config for AWS Bedrock.
