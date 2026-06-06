# Slack source — UAT shelved

**Date:** 2026-06-06
**Status:** Implementation merged (PR #4, branch `slack-source`). Real-API UAT deferred indefinitely.

## Why shelved

Slack work-workspace requires admin-installed app to mint a `xoxp-` user OAuth token. User does not have admin rights on the target workspace. Creating a personal app + install was blocked.

Tried alternatives:
- Slack desktop app token extraction — violates ToS + sandboxed.
- `xapp-` app-level token — wrong protocol (Socket Mode only, no Web API).
- `xoxb-` bot token — only sees channels the bot is invited to; defeats coverage goal.
- Workspace export ZIP — admin-only, one-shot, not the realtime path.

## What's already in place

- Spec: `docs/superpowers/specs/2026-06-05-slack-source-design.md`.
- Plan: `docs/superpowers/plans/2026-06-05-slack-source.md`.
- Code: `src/work_assistant/ingest/sources/slack.py` (584 lines).
- Migration: `0002_slack_users.sql`.
- Tests: 12 source + 4 cache + 11 helpers + 6 errors + 1 registry + 1 end-to-end worker. 152 total in suite, all green.
- UAT script: `scripts/slack_uat.sh` (probe + dry-run + real run).
- Final code review: APPROVED FOR MERGE.

## What's deferred

- Tool name verification against actual Slack MCP server (`korotovsky/slack-mcp-server` or Official Slack MCP).
  - Hard-coded tool names: `conversations_list`, `conversations_history`, `conversations_replies`, `users_info`, `chat_get_permalink`, `auth_test`.
  - Will likely need adjustment to match server. Probe via `scripts/slack_uat.sh --probe-only` once token available.
- Schema-mismatch fixes (e.g. extra fields, renamed properties).
- MCPBridge env plumbing: currently passes `env=None`; subprocess inherits `os.environ`. May need explicit env injection from keyring; deferred until real token tests it.

## How to resume

When admin grants app install OR when targeting a different workspace:

```bash
# 1. Get xoxp- token via slack app install flow (see scripts/slack_uat.sh comments).
# 2. Probe first:
./scripts/slack_uat.sh --token xoxp-XXX --probe-only

# 3. If 6 tools present, full run:
./scripts/slack_uat.sh --token xoxp-XXX
```

If tool names mismatch, patch `src/work_assistant/ingest/sources/slack.py` constants
(`tool_name: ClassVar[str] = ...`) on a `slack-uat-fixes` branch.

## Why not just delete the code

- Slack source is correctly typed, fully tested with FakeMCPClient, and lint-clean.
- The contract (Source ABC, MCP request/response models) closes 4 Phase 1 deferred follow-ups (cursor parsing, real bridge wiring, --since plumbing, real settings).
- Reusable scaffolding for the next source (Todoist) lands on the same code paths.
- Cost of carrying = 0 (no real MCP wired in production yet).

## Next

Pivot to Todoist source. Personal Doist account, official Doist MCP, no admin gate.
