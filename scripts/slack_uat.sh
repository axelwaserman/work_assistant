#!/usr/bin/env bash
# Slack source UAT.
#
# Usage:
#   scripts/slack_uat.sh --token xoxp-XXXXX [--workspace-id T01ABC] [--keep-db]
#
# Flow:
#   1. Probe MCP server: list tools + schemas (no Slack API calls).
#   2. Stage minimal config.toml in $HOME (backed up if exists).
#   3. Store token in keyring under work-assistant/slack_mcp_xoxp_token.
#   4. Apply migrations (idempotent).
#   5. Run `wa ingest --source slack --dry-run --verbose`.
#   6. Run `wa ingest --source slack --verbose` (real writes).
#   7. Print event counts + tail log.
#
# Requires: uv, npx, jq, sqlite3.

set -Eeuo pipefail

TOKEN=""
WORKSPACE_ID=""
KEEP_DB=0
PROBE_ONLY=0

usage() {
  cat <<USAGE
Usage: $0 --token xoxp-XXXX [--workspace-id T01ABC] [--keep-db] [--probe-only]

  --token         Slack User OAuth token (xoxp-...). Required.
  --workspace-id  Slack team/workspace id (some MCP builds need it).
  --keep-db       Skip the DB reset prompt; keep existing rows.
  --probe-only    Run step 1 (tool probe) and exit.
USAGE
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --token) TOKEN="$2"; shift 2;;
    --workspace-id) WORKSPACE_ID="$2"; shift 2;;
    --keep-db) KEEP_DB=1; shift;;
    --probe-only) PROBE_ONLY=1; shift;;
    -h|--help) usage;;
    *) echo "unknown arg: $1"; usage;;
  esac
done

[[ -z "$TOKEN" ]] && { echo "ERROR: --token required"; usage; }

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

step() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
ok()   { printf '  \033[32m✓\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*"; exit 1; }

for bin in uv npx jq sqlite3 python3; do
  command -v "$bin" >/dev/null || fail "missing: $bin"
done

# --- Step 1: probe MCP tool schema -------------------------------------------
step "Step 1: probe Slack MCP tool surface"

PROBE_PY=$(mktemp /tmp/probe_slack_mcp.XXXXXX.py)
trap 'rm -f "$PROBE_PY"' EXIT

cat > "$PROBE_PY" <<'PY'
import asyncio, json, os, sys
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main() -> None:
    env = {
        "PATH": os.environ["PATH"],
        "HOME": os.environ.get("HOME", ""),
        "SLACK_MCP_XOXP_TOKEN": os.environ["SLACK_MCP_XOXP_TOKEN"],
    }
    if os.environ.get("SLACK_MCP_TEAM_ID"):
        env["SLACK_MCP_TEAM_ID"] = os.environ["SLACK_MCP_TEAM_ID"]

    params = StdioServerParameters(
        command="npx",
        args=["-y", "slack-mcp-server@latest"],
        env=env,
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.list_tools()
            payload = [
                {
                    "name": t.name,
                    "description": (t.description or "")[:140],
                    "input_properties": list((t.inputSchema or {}).get("properties", {}).keys()),
                }
                for t in result.tools
            ]
            json.dump(payload, sys.stdout, indent=2)
            print()


asyncio.run(main())
PY

PROBE_ENV=(SLACK_MCP_XOXP_TOKEN="$TOKEN")
[[ -n "$WORKSPACE_ID" ]] && PROBE_ENV+=(SLACK_MCP_TEAM_ID="$WORKSPACE_ID")

PROBE_OUT=$(mktemp /tmp/probe_slack_out.XXXXXX.json)
if env "${PROBE_ENV[@]}" uv run python "$PROBE_PY" > "$PROBE_OUT" 2>/tmp/probe_slack.err; then
  ok "MCP server reachable; tools listed."
  jq -r '.[].name' "$PROBE_OUT" | sed 's/^/    /'
else
  cat /tmp/probe_slack.err >&2
  fail "MCP probe failed. See /tmp/probe_slack.err."
fi

# Verify tools we hard-coded actually exist.
REQUIRED=(conversations_list conversations_history conversations_replies users_info chat_get_permalink auth_test)
MISSING=()
ALL_TOOLS=$(jq -r '.[].name' "$PROBE_OUT")
for t in "${REQUIRED[@]}"; do
  grep -qx "$t" <<<"$ALL_TOOLS" || MISSING+=("$t")
done

if (( ${#MISSING[@]} > 0 )); then
  warn "tools not exposed by MCP server: ${MISSING[*]}"
  warn "    full list above. ingest may fail until tool_name strings are adjusted in src/work_assistant/ingest/sources/slack.py"
  warn "    abort + open issue, OR continue if you want to see the failure mode"
  read -r -p "    continue anyway? [y/N] " ans
  [[ "$ans" =~ ^[Yy]$ ]] || exit 1
else
  ok "all 6 expected tools present."
fi

(( PROBE_ONLY == 1 )) && { ok "probe-only mode; exiting."; exit 0; }

# --- Step 2: stage config.toml -----------------------------------------------
step "Step 2: stage ~/.work_assistant/config.toml"

mkdir -p "$HOME/.work_assistant"
CFG="$HOME/.work_assistant/config.toml"
if [[ -f "$CFG" ]]; then
  BACKUP="$CFG.bak.$(python3 -c 'import time;print(int(time.time()))')"
  cp "$CFG" "$BACKUP"
  ok "existing config backed up: $BACKUP"
fi

# Build slack_command honoring optional workspace id.
SLACK_CMD='["npx","-y","slack-mcp-server@latest"]'

cat > "$CFG" <<TOML
[bedrock]
region = "eu-west-1"
aws_profile = "wa"

[bedrock.models]
sonnet = "anthropic.claude-3-5-sonnet-20241022-v2:0"
opus   = "anthropic.claude-3-opus-20240229-v1:0"
haiku  = "anthropic.claude-3-haiku-20240307-v1:0"

[mcp]
todoist_command   = ["true"]
slack_command     = $SLACK_CMD
workspace_command = ["true"]

[ingest]
backfill_days_slack    = 7
backfill_days_gmail    = 90
backfill_days_calendar = 60
sources_enabled = ["slack"]
TOML
ok "config.toml written (backfill = 7d for shorter UAT)."

# --- Step 3: store token in keyring ------------------------------------------
step "Step 3: store token in macOS keyring"

uv run python - <<PY
import keyring
keyring.set_password("work-assistant", "slack_mcp_xoxp_token", "$TOKEN")
print("    stored: service=work-assistant, name=slack_mcp_xoxp_token")
PY
ok "token stored."

# --- Step 4: apply migrations ------------------------------------------------
step "Step 4: apply DB migrations"

if (( KEEP_DB == 0 )) && [[ -f "$HOME/.work_assistant/db/spine.sqlite" ]]; then
  read -r -p "    spine.sqlite exists. Reset DB before run? [y/N] " ans
  if [[ "$ans" =~ ^[Yy]$ ]]; then
    rm -f "$HOME/.work_assistant/db/spine.sqlite"*
    ok "DB removed."
  fi
fi

uv run python - <<'PY'
from pathlib import Path
from work_assistant import paths
from work_assistant.db import migrations
paths.ensure_dirs()
repo_root = Path.cwd()
migrations.apply(repo_root / "src" / "work_assistant" / "db" / "migrations_sql")
print("    migrations applied to:", paths.db_path())
PY
ok "schema ready."

# --- Step 5: gate on slack token plumbing -------------------------------------
# Phase 1 worker doesn't fetch the keyring token yet (deferred follow-up).
# We pass token through env for this UAT; this is the cheapest exit ramp.

step "Step 5: export env so MCPBridge subprocess sees the token"

export SLACK_MCP_XOXP_TOKEN="$TOKEN"
[[ -n "$WORKSPACE_ID" ]] && export SLACK_MCP_TEAM_ID="$WORKSPACE_ID"
warn "this run inherits SLACK_MCP_XOXP_TOKEN from the shell env."
warn "MCPBridge passes env=None today, which means the SDK relies on os.environ inheritance."
warn "if the subprocess can't see the token, follow-up commit needed: pass env to MCPBridge in worker._build_mcp_client."

# --- Step 6: dry-run ---------------------------------------------------------
step "Step 6: dry-run"

if uv run wa ingest --source slack --dry-run --verbose; then
  ok "dry-run completed."
else
  warn "dry-run returned non-zero. Inspect log: ~/.work_assistant/logs/wa-ingest.log"
fi

DRY_COUNT=$(sqlite3 "$HOME/.work_assistant/db/spine.sqlite" "SELECT count(*) FROM events WHERE source='slack'")
if [[ "$DRY_COUNT" == "0" ]]; then
  ok "dry-run wrote 0 events (correct)."
else
  warn "dry-run wrote $DRY_COUNT events — _DryRunDbFactory may not be intercepting commits!"
fi

# --- Step 7: real run --------------------------------------------------------
step "Step 7: real ingest"

if uv run wa ingest --source slack --verbose; then
  ok "real ingest completed."
else
  warn "non-zero exit. Inspect log."
fi

# --- Step 8: report -----------------------------------------------------------
step "Step 8: report"

sqlite3 "$HOME/.work_assistant/db/spine.sqlite" <<'SQL'
.headers on
.mode column
SELECT count(*) AS total_slack_events FROM events WHERE source='slack';
SELECT json_extract(metadata_json, '$.channel_name') AS channel,
       count(*) AS msgs
  FROM events WHERE source='slack'
 GROUP BY channel ORDER BY msgs DESC LIMIT 20;
SELECT cursor FROM ingest_cursors WHERE source='slack';
SQL

echo
ok "tail of log:"
tail -20 "$HOME/.work_assistant/logs/wa-ingest.log" | jq -c '{ts,level,event,source,channel_name,inserted,ignored,code,detail}' 2>/dev/null \
  || tail -20 "$HOME/.work_assistant/logs/wa-ingest.log"

echo
ok "UAT done."
