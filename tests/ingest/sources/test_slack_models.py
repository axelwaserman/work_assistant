"""Tests for SlackCursor and ChannelCursor."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from work_assistant.ingest.sources.slack import ChannelCursor, SlackCursor


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
