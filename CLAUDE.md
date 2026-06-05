# CLAUDE.md

Project-specific instructions for Claude Code in this repo.

## Stack

- Python 3.13+ (`requires-python = ">=3.13"` in `pyproject.toml`).
- `uv` for env + deps. Tests via `uv run pytest`.
- Single-user, local-first agentic productivity system. See `docs/00-overview.md` for full context.

## Mandatory skills

Invoke these BEFORE writing or designing Python in this repo. Not optional.

- **`dignified-python:dignified-python`** — load on every Python design or implementation turn. Covers: ABC vs Protocol, LBYL stance, pathlib, imports, anti-patterns. Must be active during brainstorming, planning, and coding of any Python module here.
- **`superpowers:brainstorming`** — before any creative/design work (features, components, refactors). Terminal state is invoking `superpowers:writing-plans`, never an implementation skill directly.
- **`superpowers:writing-plans`** — after a design is approved, before touching code.
- **`superpowers:test-driven-development`** — when implementing per a plan. Tests first.

If a Python design or implementation turn starts without `dignified-python` active, stop and invoke it before producing any code or design recommendation.

## Conventions specific to this repo

- **All interfaces are ABC. Never `Protocol`.** Applies to internal interfaces (sources, advisors, stores) AND external library facades (HTTP clients, MCP transport, clocks). See `dignified-python` skill — `references/advanced/interfaces.md` — for ABC patterns. Read it whenever creating an interface.
- **No `Any` in our own signatures, models, or return types.** Every value we define has a concrete type. For unknown / heterogeneous payloads (e.g. JSON blobs we parse), define a struct-like wrapper: `pydantic.BaseModel`, frozen `@dataclass`, or `TypedDict` with explicit fields. If shape varies by tool, use a tagged union (`Literal` discriminator + `pydantic` discriminated union). Raw `dict` / `list` only at the parse boundary, immediately validated into a struct.
- **Carve-out for third-party types.** Types we cannot control may leak `Any` (e.g. `structlog.BoundLogger`, MCP SDK `CallToolResult`). Don't fight them. Wrap them at the seam: define our own ABC adapter (e.g. `MCPClient`) that returns one of *our* typed structs, and parse the third-party reply into that struct inside the adapter.
- Module-level imports only. Absolute imports. No re-exports.
- Pathlib only. Always `encoding="utf-8"` on `read_text` / `write_text`.
- Tests under `tests/`, fixture data under `tests/fixtures/<source>/`.
- Migrations under `src/work_assistant/db/migrations/` as numbered SQL files.
- Specs land in `docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md`.
- Plans land in `docs/superpowers/plans/`.

## Ingest / source design

When designing or extending ingestion sources, treat `Source` as an ABC with `name`, `mcp_server`, abstract `async fetch(cursor) -> AsyncIterator[Batch]`, and concrete helpers for body normalization and content hashing. Each `Batch` is `(events, next_cursor, status)`; the worker — not the source — wraps batch persistence in a SQLite transaction.

## Worker model

- Short-lived processes triggered by launchd/systemd, never long-running daemons (orchestrator excepted).
- One bridge per MCP server, sources sharing a server share its bridge.
- Per-source isolation: one source's failure must not block siblings; cursor advances only on successful batch commit.

## Don'ts

- No `Protocol` anywhere. ABC always. (`dignified-python` `references/advanced/interfaces.md`.)
- No `Any`. Define a struct (pydantic / frozen dataclass / TypedDict) at the parse boundary instead.
- No relative imports.
- No `os.path` — pathlib only.
- No long-running ingest daemons.
- No skipping `dignified-python` skill on Python turns.
