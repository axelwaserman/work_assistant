# work-assistant

Personal agentic productivity assistant. See `docs/` for architecture documents
and `docs/superpowers/plans/` for implementation plans.

Phase 0 only ships the foundations (config, SQLite spine, MCP bridge, provider
wiring, `wa doctor`). Phases 1–4 layer functionality on top.

## Quickstart (Phase 0)

```bash
uv sync --all-extras
mkdir -p ~/.work_assistant
cp config.example.toml ~/.work_assistant/config.toml
uv run wa doctor
```
