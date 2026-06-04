# Architecture documents

Reference set for a single-user, local-first agentic productivity system. Todoist is the system of record for tasks; SQLite is the system of record for ingested events; Claude Agent SDK on AWS Bedrock provides the agent runtime; specialist advisor sub-agents propose tasks and the orchestrator commits them.

These documents are inputs to the superpower plan writer. Read in numeric order; each builds on the prior.

## Index

| # | Document | Purpose |
|---|---|---|
| 00 | [`00-overview.md`](00-overview.md) | Goals, non-goals, glossary, high-level shape. Start here. |
| 01 | [`01-architecture.md`](01-architecture.md) | Components, process topology, MCP server choices, repo layout, supervision model. |
| 02 | [`02-data-model.md`](02-data-model.md) | SQLite schema (full DDL), Todoist project/label taxonomy, footer dedup convention. |
| 03 | [`03-advisor-protocol.md`](03-advisor-protocol.md) | Advisor request/response contracts, routing rules, commit rules, tool-grant boundary. |
| 04 | [`04-ingestion-pipelines.md`](04-ingestion-pipelines.md) | Per-source ingestion specs: Slack, Gmail, Calendar, Gemini notes, Todoist read-back. |
| 05 | [`05-phased-plan.md`](05-phased-plan.md) | Phase 0–4 deliverables, exit criteria, risks, effort estimates. |
| 06 | [`06-failure-modes.md`](06-failure-modes.md) | Failure-mode table with mitigations, observability metrics, terse runbook. |
| 07 | [`07-open-decisions.md`](07-open-decisions.md) | Six concrete decisions the plan writer must resolve before Phase 1. |
| 08 | [`08-bedrock-provider.md`](08-bedrock-provider.md) | AWS Bedrock config: env, model IDs, inference profiles, cost expectations. |

## Reading order for the plan writer

1. **00 → 01 → 02** — establish the shape, the moving parts, and the data model.
2. **03 → 04** — understand the agent boundary and the data flowing into it.
3. **07** — resolve open decisions before planning Phase 0/1 work in detail.
4. **05** — translate decisions into concrete phase plans.
5. **06 → 08** — incorporate failure modes and provider config into operational tasks.

## Load-bearing decisions already made

These are fixed inputs. Challenge only with strong reason; do not re-litigate as part of plan writing.

- **Framework**: Claude Agent SDK (Python). Fallback: PydanticAI if Anthropic lock-in becomes a hard constraint.
- **Provider**: AWS Bedrock with inference profiles. See `08-bedrock-provider.md`.
- **System of record**: Todoist for tasks, SQLite for events. Boundary enforced.
- **Advisor boundary**: advisors propose; orchestrator commits. Tool whitelisting is the enforcement mechanism.
- **MCP servers**: official `Doist/todoist-mcp`, official Slack MCP, `taylorwilsdon/google_workspace_mcp` (decision pending in `07-open-decisions.md` #3).
- **Ingestion shape**: cron-driven pull into SQLite as the spine. Webhooks only for Slack mentions and post-meeting notes fetch.
- **Dedup**: two-layer — `(source, source_id)` UNIQUE in SQLite, `<!-- ws:hash:... -->` footer on every Todoist task.

## Out of scope

- Multi-user, shared state, team auth.
- Replacing Todoist's UX (mobile, web, desktop). Task UX stays in Todoist.
- Real-time-everything. 15-minute polling is the default cadence.
- Anything beyond the four sources and three advisors listed in `00-overview.md`. Adding either is a deliberate, documented expansion.
