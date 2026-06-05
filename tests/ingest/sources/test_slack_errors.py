"""Tests for Slack error classification."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import structlog

from tests.ingest.fakes import FakeClock, FakeMCPClient, ScriptedReply
from work_assistant.ingest.context import IngestContext, SqliteDbFactory
from work_assistant.ingest.errors import PermanentIngestError, TransientIngestError
from work_assistant.ingest.sources.slack import (
    AuthTestResponse,
    ConversationsHistoryResponse,
    ConversationsListResponse,
    SlackChannel,
    SlackCursor,
    SlackError,
    SlackMessage,
    SlackSource,
    map_slack_error,
)


def _ctx(initialized_db: Path, mcp: FakeMCPClient) -> IngestContext:
    return IngestContext(
        db=SqliteDbFactory(db_path=initialized_db),
        mcp=mcp,
        logger=structlog.get_logger("test"),
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)),
    )


def test_map_rate_limited_to_transient() -> None:
    err = map_slack_error("rate_limited")
    assert isinstance(err, TransientIngestError)


def test_map_invalid_auth_to_permanent() -> None:
    for code in ("invalid_auth", "account_inactive", "not_authed", "token_revoked"):
        err = map_slack_error(code)
        assert isinstance(err, PermanentIngestError), f"{code} should be permanent"


def test_map_unknown_error_defaults_to_transient() -> None:
    err = map_slack_error("some_unknown_error")
    assert isinstance(err, TransientIngestError)


@pytest.mark.asyncio
async def test_fetch_skips_channel_on_channel_not_found(initialized_db: Path) -> None:
    """One channel raises channel_not_found; sibling continues; run completes ok."""
    channels = [
        SlackChannel(id="C_OK", name="ok", is_member=True),
        SlackChannel(id="C_GONE", name="gone", is_member=True),
    ]
    mcp = FakeMCPClient(
        script={
            "AuthTestRequest": [
                ScriptedReply(response=AuthTestResponse(user_id="U_OWN", team_id="T1"))
            ],
            "ConversationsListRequest": [
                ScriptedReply(response=ConversationsListResponse(channels=channels)),
            ],
            "ConversationsHistoryRequest": [
                ScriptedReply(
                    response=ConversationsHistoryResponse(
                        messages=[SlackMessage(ts="100.000", user="U1", text="hi")],
                        has_more=False,
                    )
                ),
                ScriptedReply(raises=SlackError("channel_not_found")),
            ],
        }
    )

    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    # C_OK produced one batch with one event; C_GONE was skipped (no batch).
    assert len(batches) == 1
    assert batches[0].events[0].metadata.channel_id == "C_OK"


@pytest.mark.asyncio
async def test_fetch_raises_transient_on_rate_limited(initialized_db: Path) -> None:
    """rate_limited from a non-skippable call propagates as TransientIngestError."""
    mcp = FakeMCPClient(
        script={
            "AuthTestRequest": [
                ScriptedReply(response=AuthTestResponse(user_id="U_OWN", team_id="T1"))
            ],
            "ConversationsListRequest": [ScriptedReply(raises=SlackError("rate_limited"))],
        }
    )
    src = SlackSource(_ctx(initialized_db, mcp))
    with pytest.raises(TransientIngestError):
        [b async for b in src.fetch(SlackCursor())]


@pytest.mark.asyncio
async def test_fetch_raises_permanent_on_invalid_auth(initialized_db: Path) -> None:
    mcp = FakeMCPClient(
        script={
            "AuthTestRequest": [ScriptedReply(raises=SlackError("invalid_auth"))],
        }
    )
    src = SlackSource(_ctx(initialized_db, mcp))
    with pytest.raises(PermanentIngestError):
        [b async for b in src.fetch(SlackCursor())]
