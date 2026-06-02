"""Tests for work_assistant.config."""

from __future__ import annotations

from pathlib import Path

import pytest

from work_assistant import config

CONFIG_FIXTURE = """
[bedrock]
region = "eu-west-1"
aws_profile = "work-assistant"

[bedrock.models]
sonnet = "arn:aws:bedrock:eu-west-1:123456789012:inference-profile/anthropic.claude-sonnet-4-6-v1:0"
opus   = "arn:aws:bedrock:eu-west-1:123456789012:inference-profile/anthropic.claude-opus-4-7-v1:0"
haiku  = "eu.anthropic.claude-haiku-4-5-v1:0"

[mcp]
todoist_command = ["npx", "-y", "@doist/todoist-mcp"]
slack_command   = ["npx", "-y", "@modelcontextprotocol/server-slack"]
workspace_command = ["uvx", "google-workspace-mcp"]

[ingest]
backfill_days_slack = 30
backfill_days_gmail = 90
backfill_days_calendar = 60
"""


def test_load_returns_validated_config(isolated_home: Path) -> None:
    (isolated_home / ".work_assistant" / "config.toml").write_text(CONFIG_FIXTURE)
    cfg = config.load()
    assert cfg.bedrock.region == "eu-west-1"
    assert cfg.bedrock.models.sonnet.startswith("arn:aws:bedrock")
    assert cfg.mcp.todoist_command == ["npx", "-y", "@doist/todoist-mcp"]
    assert cfg.ingest.backfill_days_gmail == 90


def test_load_missing_file_raises_clear_error(isolated_home: Path) -> None:
    with pytest.raises(config.ConfigError, match="config.toml not found"):
        config.load()


def test_load_invalid_toml_raises_clear_error(isolated_home: Path) -> None:
    (isolated_home / ".work_assistant" / "config.toml").write_text("not = valid = toml")
    with pytest.raises(config.ConfigError, match="invalid TOML"):
        config.load()


def test_load_missing_required_field_raises_clear_error(isolated_home: Path) -> None:
    (isolated_home / ".work_assistant" / "config.toml").write_text("[bedrock]\nregion = 'x'\n")
    with pytest.raises(config.ConfigError, match="validation failed"):
        config.load()


def test_load_rejects_unknown_field(isolated_home: Path) -> None:
    bad_config = CONFIG_FIXTURE + "\n[unknown_section]\nfoo = \"bar\"\n"
    (isolated_home / ".work_assistant" / "config.toml").write_text(bad_config)
    with pytest.raises(config.ConfigError, match="validation failed"):
        config.load()


def test_secret_get_uses_keyring(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    def fake_get_password(service: str, username: str) -> str | None:
        captured["service"] = service
        captured["username"] = username
        return "secret-value"

    monkeypatch.setattr(config.keyring, "get_password", fake_get_password)
    assert config.get_secret("slack-bot-token") == "secret-value"
    assert captured == {"service": "work-assistant", "username": "slack-bot-token"}


def test_secret_get_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config.keyring, "get_password", lambda *a, **kw: None)
    with pytest.raises(config.ConfigError, match="missing secret"):
        config.get_secret("nonexistent-key")
