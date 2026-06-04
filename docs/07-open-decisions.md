# 07 — Open decisions

Six concrete questions to answer before Phase 1 ships. Each has a recommended default; the user picks or overrides. The plan writer should resolve these — or surface them to the user — before the first line of Phase 0 code is written.

## 1. Embedding storage

**Question**: where do embeddings live?

**Options**:
- **A. `sqlite-vec` extension** — single SQLite file, no extra service, queryable alongside events. *Recommended.*
- **B. External vector DB** (Chroma, LanceDB, Qdrant) — better filtering and scale, but adds a process and a sync layer.

**Recommendation**: **A**. The system is single-user and local-first. Adding a vector service breaks both properties. `sqlite-vec` is mature enough for this scale (max ~100k events at the kind of volumes a single user generates).

**Action**: confirm A or pick B.

---

## 2. Slack webhook ingress

**Question**: how does Slack reach the orchestrator running on a laptop?

**Options**:
- **A. Cloudflare Tunnel** (`cloudflared`) — free, stable, terminates TLS, requires a Cloudflare account.
- **B. Tailscale Funnel** — free for personal use, simpler if already on Tailscale.
- **C. No webhook** — accept up-to-15-minute latency for Slack mentions; rely on cron polling only.

**Recommendation**: **A** if real-time mention response matters, else **C**. Webhooks are a Phase 3 optimization. The system works without them; mentions just lag by up to 15 minutes.

**Trade-off**: real-time response feels good but doubles operational surface (tunnel + signature verification + replay handling). For a personal tool, **C** is often the right answer.

**Action**: pick A, B, or C. If C, drop the webhook deliverable from Phase 3.

---

## 3. Workspace MCP — consolidated vs official

**Question**: one community MCP for Gmail/Calendar/Drive/Docs, or four official ones?

**Options**:
- **A. `taylorwilsdon/google_workspace_mcp`** — single OAuth flow, single subprocess, community-maintained.
- **B. Four separate official MCPs** (Gmail, Calendar, Drive, Docs) — first-party, but four subprocesses and four (or one merged) OAuth flows.

**Recommendation**: **A**, with version pinning. The OAuth-flow consolidation is genuinely valuable, and the community server is actively maintained. Verify in Phase 0 that it exposes Gmail `historyId`-based incremental sync; if not, fall back to direct Gmail API for that source only.

**Action**: confirm A and the Phase 0 capability check, or pick B.

---

## 4. Daily auto-commit budget

**Question**: how many tasks can advisors auto-commit per day before forcing human review?

**Options**:
- **A. 5/day** — strict; you'll see almost everything in the review queue initially.
- **B. 20/day** — middle ground. *Recommended.*
- **C. Unlimited** — full automation; only confidence threshold gates commits.

**Recommendation**: **B**. Strict enough to catch advisor drift early; loose enough that the system feels useful from day one.

**Action**: pick a number. This is configurable, so the cost of getting it wrong is low.

---

## 5. Backfill horizon

**Question**: how far back should Phase 1 first-run ingest from each source?

**Options**:
- **A. 30 days everywhere** — fast, low quota burn, less context.
- **B. Source-specific** (recommended): Slack 30d (history caps make more impractical), Gmail 90d, Calendar 60d, Todoist full. *Recommended.*
- **C. 1 year everywhere** — risks Slack rate limits and Gmail quota burn on first run.

**Recommendation**: **B**. The numbers in `04-ingestion-pipelines.md` reflect this default.

**Action**: confirm B's per-source numbers, or override.

---

## 6. Initial confidence threshold

**Question**: minimum advisor confidence for auto-commit (vs review queue)?

**Options**:
- **A. 0.8** — strict. Most proposals queue for review initially. *Recommended.*
- **B. 0.6** — permissive. More auto-commits, more false positives.
- **C. Per-advisor** — task-hygiene at 0.6, manager-coaching at 0.85, stakeholder-comms at 0.85.

**Recommendation**: **A**, then loosen after observing 1–2 weeks of false-positive rate. Trust is earned. Easier to relax than to apologize for bad auto-commits.

**Action**: pick A or set per-advisor values.

---

## Decisions table (for plan writer to fill in)

| # | Question | Decision | Notes |
|---|---|---|---|
| 1 | Embedding storage | _____ | |
| 2 | Slack webhook ingress | _____ | |
| 3 | Workspace MCP | _____ | |
| 4 | Daily auto-commit budget | _____ | |
| 5 | Backfill horizon | _____ | |
| 6 | Confidence threshold | _____ | |

Once filled, copy this table into `config.toml` defaults and reference it in the Phase 0 PR description.
