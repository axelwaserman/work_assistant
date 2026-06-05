"""Tests for SlackCursor and ChannelCursor."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from work_assistant.ingest.sources.slack import (
    AuthTestRequest,
    ChannelCursor,
    ConversationsHistoryRequest,
    ConversationsHistoryResponse,
    ConversationsListRequest,
    ConversationsRepliesRequest,
    GetPermalinkRequest,
    SlackChannel,
    SlackCursor,
    SlackMessage,
    SlackUser,
    UsersInfoRequest,
    UsersInfoResponse,
)


def test_slack_cursor_default_is_empty() -> None:
    cur = SlackCursor()
    assert cur.channels == []


def test_slack_cursor_serializes_to_json() -> None:
    cur = SlackCursor(
        channels=[
            ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="100.000"),
        ]
    )
    payload = cur.model_dump_json()
    assert "C1" in payload
    assert "general" in payload
    assert "100.000" in payload


def test_slack_cursor_round_trip() -> None:
    original = SlackCursor(
        channels=[
            ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="100.000"),
            ChannelCursor(channel_id="C2", channel_name="random", last_seen_ts="200.000"),
        ]
    )
    payload = original.model_dump_json()
    restored = SlackCursor.model_validate_json(payload)
    assert restored == original


def test_slack_cursor_lookup_returns_match_or_none() -> None:
    cur = SlackCursor(
        channels=[
            ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="100.000"),
        ]
    )
    found = cur.lookup("C1")
    assert found is not None
    assert found.channel_name == "general"
    assert cur.lookup("C_MISSING") is None


def test_slack_cursor_with_updated_replaces_existing() -> None:
    cur = SlackCursor(
        channels=[
            ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="100.000"),
            ChannelCursor(channel_id="C2", channel_name="random", last_seen_ts="200.000"),
        ]
    )
    new = cur.with_updated(
        ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="500.000")
    )
    assert new.lookup("C1").last_seen_ts == "500.000"
    assert new.lookup("C2").last_seen_ts == "200.000"
    # original unchanged (frozen)
    assert cur.lookup("C1").last_seen_ts == "100.000"


def test_slack_cursor_with_updated_appends_new() -> None:
    cur = SlackCursor()
    new = cur.with_updated(
        ChannelCursor(channel_id="C9", channel_name="new", last_seen_ts="42.000")
    )
    assert len(new.channels) == 1
    assert new.lookup("C9").last_seen_ts == "42.000"


def test_channel_cursor_is_frozen() -> None:
    ch = ChannelCursor(channel_id="C1", channel_name="general", last_seen_ts="1.0")
    with pytest.raises(ValidationError):
        ch.last_seen_ts = "2.0"  # type: ignore[misc]


def test_slack_channel_parses() -> None:
    ch = SlackChannel(
        id="C1",
        name="general",
        is_member=True,
        is_archived=False,
        is_im=False,
        is_mpim=False,
    )
    assert ch.id == "C1"
    assert ch.is_member is True


def test_slack_message_parses_with_optional_fields() -> None:
    msg = SlackMessage(
        ts="100.000",
        user="U1",
        text="hi",
        thread_ts=None,
        subtype=None,
    )
    assert msg.ts == "100.000"
    assert msg.thread_ts is None


def test_slack_user_parses() -> None:
    user = SlackUser(id="U1", name="alice", real_name="Alice", email="alice@example.com")
    assert user.email == "alice@example.com"


def test_conversations_list_request_tool_name() -> None:
    assert ConversationsListRequest.tool_name == "conversations_list"


def test_conversations_history_request_serializes_args() -> None:
    req = ConversationsHistoryRequest(channel="C1", oldest="100.000", limit=200)
    assert req.tool_name == "conversations_history"
    args = req.model_dump()
    assert args == {"channel": "C1", "oldest": "100.000", "limit": 200}


def test_conversations_history_response_round_trip() -> None:
    payload = """{
        "messages": [{"ts": "100.000", "user": "U1", "text": "hi"}],
        "has_more": false,
        "response_metadata": null
    }"""
    resp = ConversationsHistoryResponse.model_validate_json(payload)
    assert len(resp.messages) == 1
    assert resp.messages[0].ts == "100.000"
    assert resp.has_more is False


def test_conversations_replies_request_tool_name() -> None:
    assert ConversationsRepliesRequest.tool_name == "conversations_replies"


def test_users_info_request_tool_name() -> None:
    assert UsersInfoRequest.tool_name == "users_info"


def test_get_permalink_request_tool_name() -> None:
    assert GetPermalinkRequest.tool_name == "chat_get_permalink"


def test_auth_test_request_tool_name() -> None:
    assert AuthTestRequest.tool_name == "auth_test"


def test_users_info_response_round_trip() -> None:
    payload = '{"user": {"id": "U1", "name": "alice", "real_name": "Alice", "email": "a@b.com"}}'
    resp = UsersInfoResponse.model_validate_json(payload)
    assert resp.user.email == "a@b.com"
