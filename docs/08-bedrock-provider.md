# 08 — Bedrock provider

The system runs on **AWS Bedrock**, not the direct Anthropic API. This section is the operational guide.

## Why this works without code changes

The Claude Agent SDK supports Bedrock as a first-class provider, the same way Claude Code does. MCP, sub-agents, tool use, structured outputs, streaming, prompt caching, and extended thinking all behave identically — you swap the auth and the model identifier, and everything else is provider-agnostic.

## Required environment per process

Set these in every process: orchestrator, ingest-worker, hygiene-worker, notes-fetcher.

```bash
CLAUDE_CODE_USE_BEDROCK=1
AWS_REGION=eu-west-1                           # or your region of choice
# AWS credentials via one of:
#   AWS_PROFILE=<profile-name>                 # recommended for dev
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  # for service deployments
#   IAM role (when running on EC2/ECS/Lambda)
```

Set in the launchd plist (or systemd unit) so workers inherit them. Don't rely on shell rc files — launchd doesn't source them.

## Model selection

Bedrock requires Bedrock model IDs or **inference profile ARNs**, not Anthropic model slugs.

```toml
# ~/.work_assistant/config.toml
[bedrock]
region = "eu-west-1"

[bedrock.models]
# Orchestrator + most advisors
sonnet = "arn:aws:bedrock:eu-west-1:<account>:application-inference-profile/<profile-id>"
# Heavy-lift work: weekly review, deep manager-coaching analysis
opus   = "arn:aws:bedrock:eu-west-1:<account>:application-inference-profile/<profile-id>"
# Routing classifier (cheap)
haiku  = "arn:aws:bedrock:eu-west-1:<account>:inference-profile/eu.anthropic.claude-haiku-4-5-v1:0"

[bedrock.embeddings]
# Phase 4
model = "amazon.titan-embed-text-v2:0"
```

### Why inference profiles, not raw model IDs

- Cross-region failover (`eu.anthropic.claude-sonnet-4-...` automatically fans out across enabled EU regions).
- Centralized cost tracking via the profile.
- Easier to swap model versions without touching code.

For the orchestrator and primary advisors, **always** use an inference profile ARN. Raw model IDs are fine for one-off scripts.

## Per-process model assignment

| Process | Model | Why |
|---|---|---|
| `orchestrator` (chat) | sonnet | General reasoning, fast enough for chat |
| `dispatcher` (LLM router fallback) | haiku | Cheap structured output, 3-way classification |
| `manager-coaching` | sonnet | Default; opus for explicit deep-dive runs |
| `stakeholder-comms` | sonnet | Default; opus for high-stakes drafts |
| `task-hygiene` | sonnet | Default; haiku for trivial cleanup runs |
| `hygiene-worker` (weekly review) | opus | Once a week, worth the cost |
| Embeddings | titan-embed | Cheap, sufficient for FTS-augmented search |

The model is selected per call from `config.toml`, not hardcoded. Advisors can request a different tier in their `AdvisorRequest.model_override`, capped by orchestrator policy.

## Region and quota gotchas

1. **Sonnet *and* Opus must be enabled in the chosen region.** Opus availability is region-spotty. Verify in AWS console (`Bedrock → Model access`) before writing code. `wa doctor` includes a one-shot test invocation per configured model.
2. **Cross-region inference profiles** are worth setting up if you want failover. The user is already on one (per the model ARN in the runtime). Keep using it.
3. **Bedrock quotas come from your AWS account**, not Anthropic. Check `Service Quotas → Amazon Bedrock` for `requests-per-minute` and `tokens-per-minute` for each model. Sonnet's default RPM is generous for personal use; Opus is tighter — request quota increase if you hit limits.
4. **Prompt caching**: supported on Bedrock for Claude 4.x as of mid-2025. Enable via the SDK's standard cache-control headers. Useful for the orchestrator's long system prompt and the SQLite-tool definitions.
5. **Extended thinking**: supported on Bedrock for Sonnet 4.x / Opus 4.x. Reserve for advisor calls where reasoning depth matters (e.g. weekly review). Not for chat — adds latency.

## Auth in workers (launchd / systemd)

**launchd plist snippet** (macOS):

```xml
<key>EnvironmentVariables</key>
<dict>
  <key>CLAUDE_CODE_USE_BEDROCK</key>
  <string>1</string>
  <key>AWS_REGION</key>
  <string>eu-west-1</string>
  <key>AWS_PROFILE</key>
  <string>work-assistant</string>
  <key>HOME</key>
  <string>/Users/axel</string>
  <key>PATH</key>
  <string>/opt/homebrew/bin:/usr/bin:/bin</string>
</dict>
```

**systemd user unit snippet** (Linux):

```ini
[Service]
Environment=CLAUDE_CODE_USE_BEDROCK=1
Environment=AWS_REGION=eu-west-1
Environment=AWS_PROFILE=work-assistant
```

The AWS profile is resolved from `~/.aws/credentials`. For long-running workers, use SSO (`aws sso login`) and refresh tokens via `aws sso login` weekly, or use IAM Identity Center with a longer session.

## `wa doctor` checks (Bedrock-specific)

`wa doctor` runs these on every invocation:

1. `boto3.client('bedrock-runtime')` initializes — fails clearly on missing creds.
2. One-shot Sonnet `Converse` call with a 5-token max output. Fails clearly on 403 (model not enabled) or quota error.
3. One-shot Opus call with same shape. Warns (does not fail) if 403; orchestrator falls back to Sonnet.
4. Embedding model availability check (Phase 4).

## Cost expectations (rough, single-user)

Order-of-magnitude estimate to size the AWS bill:

| Workload | Calls/day | Avg tokens (in/out) | Daily cost |
|---|---|---|---|
| Orchestrator chat (interactive) | 50 | 6k / 1k | ~$0.50 |
| Dispatcher (haiku classifier) | 200 | 1k / 0.1k | ~$0.05 |
| Advisor invocations | 30 | 8k / 1.5k | ~$0.40 |
| Hygiene weekly run (opus) | 1 | 30k / 5k | ~$1.50 / week |
| Embeddings (Phase 4) | 1 batch | 200 events × 1k tokens | ~$0.02 |

**Total**: roughly **$1–2/day** in steady state, plus **~$1.50/week** for the weekly opus run. Budget for $50/month with comfortable headroom. Verify against your own usage in week 1 of Phase 1.

## Switching providers

If at any point you want to switch off Bedrock — to direct Anthropic API, or to a third-party gateway — the only changes needed are:

1. Unset `CLAUDE_CODE_USE_BEDROCK`.
2. Set `ANTHROPIC_API_KEY` (or appropriate alternative).
3. Update `config.toml` model identifiers from Bedrock ARNs to Anthropic slugs.

Application code does not change. This is the architectural payoff of using the SDK rather than calling Bedrock APIs directly.
