"""Tests for the `wa ingest` CLI subcommand."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from work_assistant import logging_setup
from work_assistant.ingest import cli as ingest_cli

_MINIMAL_CONFIG = """
[bedrock]
region = "eu-west-1"
aws_profile = "wa"

[bedrock.models]
sonnet = "x"
opus   = "y"
haiku  = "z"

[mcp]
todoist_command   = ["true"]
slack_command     = ["true"]
workspace_command = ["true"]

[ingest]
backfill_days_slack    = 1
backfill_days_gmail    = 1
backfill_days_calendar = 1
"""


@pytest.fixture(autouse=True)
def _config_file(isolated_home: Path) -> None:
    """Provide a minimal valid config.toml so the CLI does not exit 4."""
    (isolated_home / ".work_assistant" / "config.toml").write_text(
        _MINIMAL_CONFIG, encoding="utf-8"
    )


@pytest.fixture(autouse=True)
def _reset_logging_state() -> None:
    """Reset module-level logging state so CLI tests do not pollute siblings."""
    logging_setup._CONFIGURED["done"] = False
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    yield
    logging_setup._CONFIGURED["done"] = False
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()


def test_ingest_cli_no_sources_returns_zero(initialized_db: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(ingest_cli.ingest, [])
    assert result.exit_code == 0, result.output


def test_ingest_cli_unknown_source_exits_two(
    initialized_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = CliRunner()
    result = runner.invoke(ingest_cli.ingest, ["--source", "nope"])
    assert result.exit_code == 2, result.output


def test_ingest_cli_passes_since_as_unix(
    initialized_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_worker(opts):  # type: ignore[no-untyped-def]
        captured["opts"] = opts
        return 0

    monkeypatch.setattr(ingest_cli, "run_worker", fake_run_worker)
    runner = CliRunner()
    result = runner.invoke(ingest_cli.ingest, ["--since", "2026-06-01T00:00:00+00:00"])
    assert result.exit_code == 0
    expected_unix = int(datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC).timestamp())
    assert captured["opts"].since_unix == expected_unix


def test_ingest_cli_parses_comma_separated_source_list(
    initialized_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_worker(opts):  # type: ignore[no-untyped-def]
        captured["opts"] = opts
        return 0

    monkeypatch.setattr(ingest_cli, "run_worker", fake_run_worker)
    runner = CliRunner()
    # Empty registry - but we mock `run_worker` so the registry check is skipped.
    result = runner.invoke(ingest_cli.ingest, ["--source", "slack,gmail"])
    assert result.exit_code == 0
    assert captured["opts"].requested_sources == ["slack", "gmail"]


def test_ingest_cli_dry_run_flag_propagates(
    initialized_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_worker(opts):  # type: ignore[no-untyped-def]
        captured["opts"] = opts
        return 0

    monkeypatch.setattr(ingest_cli, "run_worker", fake_run_worker)
    runner = CliRunner()
    result = runner.invoke(ingest_cli.ingest, ["--dry-run"])
    assert result.exit_code == 0
    assert captured["opts"].dry_run is True


def test_ingest_cli_malformed_since_exits_two(initialized_db: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(ingest_cli.ingest, ["--since", "not-a-date"])
    assert result.exit_code == 2, result.output
    assert "ISO-8601" in result.output


def test_ingest_cli_verbose_sets_debug_level(
    initialized_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, int] = {}

    real_setup = logging_setup.setup

    def spy_setup(proc: str, level: int = logging.INFO) -> None:
        captured["level"] = level
        real_setup(proc, level=level)

    monkeypatch.setattr(logging_setup, "setup", spy_setup)

    async def fake_run_worker(opts):  # type: ignore[no-untyped-def]
        return 0

    monkeypatch.setattr(ingest_cli, "run_worker", fake_run_worker)
    runner = CliRunner()
    result = runner.invoke(ingest_cli.ingest, ["--verbose"])
    assert result.exit_code == 0
    assert captured["level"] == logging.DEBUG
