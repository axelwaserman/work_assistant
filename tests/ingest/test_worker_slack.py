"""End-to-end: real SlackSource registered, FakeMCPClient injected, run_worker drives it."""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.ingest.fakes import FakeClock, FakeMCPClient, ScriptedReply
from work_assistant.ingest.sources.slack import (
    AuthTestResponse,
    ConversationsHistoryResponse,
    ConversationsListResponse,
    SlackChannel,
    SlackMessage,
    SlackSource,
)
from work_assistant.ingest.worker import (
    EXIT_OK,
    WorkerOptions,
    run_worker,
)

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
backfill_days_slack    = 30
backfill_days_gmail    = 90
backfill_days_calendar = 60
"""


@pytest.fixture(autouse=True)
def _config_file(isolated_home: Path) -> None:
    (isolated_home / ".work_assistant" / "config.toml").write_text(
        _MINIMAL_CONFIG, encoding="utf-8"
    )


@pytest.fixture()
def slack_mcp_factory(monkeypatch: pytest.MonkeyPatch):
    """Patch the worker's bridge construction so SlackSource sees a FakeMCPClient."""
    from work_assistant.ingest import worker as worker_mod

    def fake_build_mcp_client(mcp_server: str, settings):  # type: ignore[no-untyped-def]
        if mcp_server == "slack":
            return FakeMCPClient(
                script={
                    "AuthTestRequest": [
                        ScriptedReply(response=AuthTestResponse(user_id="U_OWN", team_id="T1")),
                    ],
                    "ConversationsListRequest": [
                        ScriptedReply(
                            response=ConversationsListResponse(
                                channels=[
                                    SlackChannel(
                                        id="C1",
                                        name="general",
                                        is_member=True,
                                        is_archived=False,
                                        is_im=False,
                                        is_mpim=False,
                                    ),
                                ]
                            )
                        ),
                    ],
                    "ConversationsHistoryRequest": [
                        ScriptedReply(
                            response=ConversationsHistoryResponse(
                                messages=[
                                    SlackMessage(ts="100.000", user="U_AUTHOR", text="hi"),
                                ],
                                has_more=False,
                            )
                        ),
                    ],
                }
            )
        raise NotImplementedError(f"no fake for {mcp_server!r}")

    monkeypatch.setattr(worker_mod, "_build_mcp_client", fake_build_mcp_client)


@pytest.mark.asyncio
async def test_worker_runs_slack_end_to_end(initialized_db: Path, slack_mcp_factory) -> None:
    opts = WorkerOptions(
        registry={"slack": SlackSource},
        sources_enabled=["slack"],
        clock=FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)),
        pid=os.getpid(),
        run_id="test-run",
    )
    code = await run_worker(opts)
    assert code == EXIT_OK
    with sqlite3.connect(initialized_db) as conn:
        n = conn.execute("SELECT count(*) FROM events WHERE source='slack'").fetchone()[0]
    assert n == 1
