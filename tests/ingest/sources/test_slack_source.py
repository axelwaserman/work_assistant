"""Tests for SlackSource.fetch and cursor_from_timestamp."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import structlog

from tests.ingest.fakes import FakeClock, FakeMCPClient
from tests.ingest.sources._helpers import slack_script
from work_assistant.ingest.context import IngestContext, SqliteDbFactory
from work_assistant.ingest.sources.slack import (
    ChannelCursor,
    ConversationsHistoryResponse,
    ConversationsListResponse,
    ConversationsRepliesResponse,
    SlackChannel,
    SlackCursor,
    SlackMessage,
    SlackSource,
)


def _ctx(initialized_db: Path, mcp: FakeMCPClient) -> IngestContext:
    return IngestContext(
        db=SqliteDbFactory(db_path=initialized_db),
        mcp=mcp,
        logger=structlog.get_logger("test"),
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)),
    )


def _channel(
    channel_id: str,
    name: str,
    *,
    is_member: bool = True,
    is_archived: bool = False,
    is_im: bool = False,
    is_mpim: bool = False,
) -> SlackChannel:
    return SlackChannel(
        id=channel_id,
        name=name,
        is_member=is_member,
        is_archived=is_archived,
        is_im=is_im,
        is_mpim=is_mpim,
    )


def _msg(
    ts: str,
    user: str = "U_OTHER",
    text: str = "hi",
    thread_ts: str | None = None,
) -> SlackMessage:
    return SlackMessage(ts=ts, user=user, text=text, thread_ts=thread_ts)


# --- happy path ---


@pytest.mark.asyncio
async def test_fetch_two_channels_each_three_messages(initialized_db: Path) -> None:
    mcp = FakeMCPClient(
        script=slack_script(
            list_channels=[
                ConversationsListResponse(
                    channels=[
                        _channel("C1", "general"),
                        _channel("C2", "random"),
                    ]
                )
            ],
            histories={
                "C1": [
                    ConversationsHistoryResponse(
                        messages=[_msg("100.000"), _msg("101.000"), _msg("102.000")],
                        has_more=False,
                    )
                ],
                "C2": [
                    ConversationsHistoryResponse(
                        messages=[_msg("200.000"), _msg("201.000"), _msg("202.000")],
                        has_more=False,
                    )
                ],
            },
        )
    )
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    assert len(batches) == 2
    assert all(len(b.events) == 3 for b in batches)
    final = batches[-1].next_cursor
    assert isinstance(final, SlackCursor)
    assert final.lookup("C1").last_seen_ts == "102.000"
    assert final.lookup("C2").last_seen_ts == "202.000"


@pytest.mark.asyncio
async def test_fetch_skips_archived_and_non_member_channels(initialized_db: Path) -> None:
    channels = [
        _channel("C_OK", "general"),
        _channel("C_ARCHIVED", "old", is_archived=True),
        _channel("C_NOT_MEMBER", "other", is_member=False),
    ]
    mcp = FakeMCPClient(
        script=slack_script(
            list_channels=[ConversationsListResponse(channels=channels)],
            histories={
                "C_OK": [ConversationsHistoryResponse(messages=[_msg("100.000")], has_more=False)],
            },
        )
    )
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    assert len(batches) == 1
    assert batches[0].events[0].metadata.channel_id == "C_OK"


@pytest.mark.asyncio
async def test_fetch_empty_channel_yields_zero_event_batch(initialized_db: Path) -> None:
    mcp = FakeMCPClient(
        script=slack_script(
            list_channels=[ConversationsListResponse(channels=[_channel("C1", "general")])],
            histories={"C1": [ConversationsHistoryResponse(messages=[], has_more=False)]},
        )
    )
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    assert len(batches) == 1
    assert batches[0].events == []


# --- threading ---


@pytest.mark.asyncio
async def test_fetch_eligible_thread_pulls_replies(initialized_db: Path) -> None:
    top = _msg("100.000", user="U_OWN", text="anyone?", thread_ts="100.000")
    reply = _msg("101.000", user="U_OTHER", text="me", thread_ts="100.000")
    mcp = FakeMCPClient(
        script=slack_script(
            list_channels=[ConversationsListResponse(channels=[_channel("C1", "general")])],
            histories={"C1": [ConversationsHistoryResponse(messages=[top], has_more=False)]},
            replies={"100.000": [ConversationsRepliesResponse(messages=[top, reply])]},
        )
    )
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    # Top + 1 reply (parent dedup'd from replies list).
    assert len(batches[0].events) == 2


@pytest.mark.asyncio
async def test_fetch_ineligible_thread_does_not_call_replies(initialized_db: Path) -> None:
    top = _msg("100.000", user="U_OTHER", text="random", thread_ts="100.000")
    mcp = FakeMCPClient(
        script=slack_script(
            list_channels=[ConversationsListResponse(channels=[_channel("C1", "general")])],
            histories={"C1": [ConversationsHistoryResponse(messages=[top], has_more=False)]},
        )
    )
    src = SlackSource(_ctx(initialized_db, mcp))
    batches = [b async for b in src.fetch(SlackCursor())]
    assert len(batches[0].events) == 1
    replies_calls = [
        c for c in mcp.calls if type(c.request).__name__ == "ConversationsRepliesRequest"
    ]
    assert replies_calls == []


# --- cursor seeding ---


@pytest.mark.asyncio
async def test_fetch_new_channel_seeds_from_backfill_window(initialized_db: Path) -> None:
    """New channel not in cursor: seeded at clock.now_unix() - 30 * 86400."""
    mcp = FakeMCPClient(
        script=slack_script(
            list_channels=[ConversationsListResponse(channels=[_channel("C_NEW", "fresh")])],
            histories={
                "C_NEW": [ConversationsHistoryResponse(messages=[_msg("100.000")], has_more=False)],
            },
        )
    )
    src = SlackSource(_ctx(initialized_db, mcp))
    [b async for b in src.fetch(SlackCursor())]
    history_call = next(
        c for c in mcp.calls if type(c.request).__name__ == "ConversationsHistoryRequest"
    )
    expected_oldest = src.ctx.clock.now_unix() - 30 * 86400
    assert history_call.request.oldest == str(expected_oldest)


@pytest.mark.asyncio
async def test_cursor_from_timestamp_returns_empty_cursor(initialized_db: Path) -> None:
    """Per spec §4.4: synthesized cursor is empty; fetch() seeds at runtime."""
    mcp = FakeMCPClient(script={})
    src = SlackSource(_ctx(initialized_db, mcp))
    cursor = src.cursor_from_timestamp(1_700_000_000)
    assert isinstance(cursor, SlackCursor)
    assert cursor.channels == []


@pytest.mark.asyncio
async def test_fetch_resumes_from_existing_channel_cursor(initialized_db: Path) -> None:
    """If cursor has C1 at ts=500, fetch should pass oldest=500."""
    mcp = FakeMCPClient(
        script=slack_script(
            list_channels=[ConversationsListResponse(channels=[_channel("C1", "general")])],
            histories={
                "C1": [ConversationsHistoryResponse(messages=[_msg("600.000")], has_more=False)],
            },
        )
    )
    src = SlackSource(_ctx(initialized_db, mcp))
    cursor = SlackCursor(
        channels=[
            ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="500.000"),
        ]
    )
    [b async for b in src.fetch(cursor)]
    history_call = next(
        c for c in mcp.calls if type(c.request).__name__ == "ConversationsHistoryRequest"
    )
    assert history_call.request.oldest == "500.000"


@pytest.mark.asyncio
async def test_fetch_uses_since_unix_when_set(initialized_db: Path) -> None:
    """When ctx.since_unix is set, new channels seed at that ts instead of backfill window."""
    mcp = FakeMCPClient(
        script=slack_script(
            list_channels=[ConversationsListResponse(channels=[_channel("C_NEW", "fresh")])],
            histories={"C_NEW": [ConversationsHistoryResponse(messages=[], has_more=False)]},
        )
    )
    ctx = IngestContext(
        db=SqliteDbFactory(db_path=initialized_db),
        mcp=mcp,
        logger=structlog.get_logger("test"),
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)),
        since_unix=1_700_000_000,
    )
    src = SlackSource(ctx)
    [b async for b in src.fetch(SlackCursor())]
    history_call = next(
        c for c in mcp.calls if type(c.request).__name__ == "ConversationsHistoryRequest"
    )
    assert history_call.request.oldest == "1700000000"


@pytest.mark.asyncio
async def test_fetch_with_since_unix_discards_persisted_cursor(initialized_db: Path) -> None:
    """When ctx.since_unix is set, _load_or_init_cursor returns empty SlackCursor."""
    # Pre-populate ingest_cursors with a different cursor.
    import sqlite3

    with sqlite3.connect(initialized_db) as conn:
        existing = SlackCursor(
            channels=[
                ChannelCursor(channel_id="C_NEW", channel_name="fresh", last_seen_ts="999.000"),
            ]
        ).model_dump_json()
        conn.execute(
            "INSERT INTO ingest_cursors(source, cursor, updated_at, last_status) "
            "VALUES (?, ?, ?, ?)",
            ("slack", existing, 1700000000, "ok"),
        )

    mcp = FakeMCPClient(
        script=slack_script(
            list_channels=[ConversationsListResponse(channels=[_channel("C_NEW", "fresh")])],
            histories={"C_NEW": [ConversationsHistoryResponse(messages=[], has_more=False)]},
        )
    )
    ctx = IngestContext(
        db=SqliteDbFactory(db_path=initialized_db),
        mcp=mcp,
        logger=structlog.get_logger("test"),
        settings=None,  # type: ignore[arg-type]
        clock=FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)),
        since_unix=1_700_000_000,
    )
    src = SlackSource(ctx)
    [b async for b in src.fetch(None)]
    history_call = next(
        c for c in mcp.calls if type(c.request).__name__ == "ConversationsHistoryRequest"
    )
    # Persisted ts=999.000 ignored; since_unix used instead.
    assert history_call.request.oldest == "1700000000"
