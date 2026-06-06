"""Tests for _thread_eligible and _normalize_message helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from tests.ingest.fakes import FakeClock
from work_assistant.ingest.context import SqliteDbFactory
from work_assistant.ingest.sources.slack import (
    SlackChannel,
    SlackMessage,
    SlackUser,
    _normalize_message,
    _SlackUserCache,
    _thread_eligible,
)


def _cache(initialized_db: Path) -> _SlackUserCache:
    clock = FakeClock(datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC))
    return _SlackUserCache(db=SqliteDbFactory(db_path=initialized_db), clock=clock)


# --- _thread_eligible ---


def test_thread_eligible_top_level_authored_by_user() -> None:
    top = SlackMessage(ts="100.000", user="U_OWN", text="hello")
    assert _thread_eligible(top, replies=None, own_user_id="U_OWN") is True


def test_thread_eligible_user_mentioned_in_top_level() -> None:
    top = SlackMessage(ts="100.000", user="U_OTHER", text="hey <@U_OWN> thoughts?")
    assert _thread_eligible(top, replies=None, own_user_id="U_OWN") is True


def test_thread_not_eligible_when_user_absent() -> None:
    top = SlackMessage(ts="100.000", user="U_OTHER", text="lunch?")
    assert _thread_eligible(top, replies=None, own_user_id="U_OWN") is False


def test_thread_eligible_user_authored_a_reply() -> None:
    top = SlackMessage(ts="100.000", user="U_OTHER", text="anyone?")
    replies = [
        SlackMessage(ts="100.000", user="U_OTHER", text="anyone?"),
        SlackMessage(ts="101.000", user="U_OWN", text="here"),
    ]
    assert _thread_eligible(top, replies=replies, own_user_id="U_OWN") is True


def test_thread_eligible_user_mentioned_in_a_reply() -> None:
    top = SlackMessage(ts="100.000", user="U_OTHER", text="anyone?")
    replies = [
        SlackMessage(ts="100.000", user="U_OTHER", text="anyone?"),
        SlackMessage(ts="101.000", user="U_3RD", text="<@U_OWN> what about you"),
    ]
    assert _thread_eligible(top, replies=replies, own_user_id="U_OWN") is True


# --- _normalize_message ---


def _channel(channel_id: str = "C1", name: str = "general", **kw: bool) -> SlackChannel:
    return SlackChannel(
        id=channel_id,
        name=name,
        is_member=kw.get("is_member", True),
        is_archived=kw.get("is_archived", False),
        is_im=kw.get("is_im", False),
        is_mpim=kw.get("is_mpim", False),
    )


def test_normalize_message_builds_event(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    channel = _channel()
    msg = SlackMessage(ts="100.123", user="U_AUTHOR", text="hello world")
    event = _normalize_message(
        msg=msg,
        channel=channel,
        cache=cache,
        own_user_id="U_OWN",
        source_name="slack",
    )
    assert event.source == "slack"
    assert event.source_id == "C1:100.123"
    assert event.kind == "message"
    assert event.body == "hello world"
    assert event.body_truncated is False
    assert event.thread_key == "100.123"  # not threaded -> ts
    assert event.actor == "U_AUTHOR"  # cache miss -> raw user_id
    assert event.metadata.kind == "slack"
    assert event.metadata.channel_id == "C1"
    assert event.metadata.channel_name == "general"
    assert event.metadata.is_mention is False
    assert event.occurred_at == 100  # int(float(ts))


def test_normalize_message_threaded_uses_thread_ts(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    msg = SlackMessage(ts="200.000", user="U1", text="reply text", thread_ts="100.000")
    event = _normalize_message(
        msg=msg,
        channel=_channel(),
        cache=cache,
        own_user_id="U_OWN",
        source_name="slack",
    )
    assert event.thread_key == "100.000"


def test_normalize_message_truncates_long_body(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    long_text = "a" * 200_000
    msg = SlackMessage(ts="100.000", user="U1", text=long_text)
    event = _normalize_message(
        msg=msg,
        channel=_channel(),
        cache=cache,
        own_user_id="U_OWN",
        source_name="slack",
    )
    assert len(event.body.encode("utf-8")) <= 100_000
    assert event.body_truncated is True


def test_normalize_message_rewrites_mention_using_cache(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    cache.upsert(
        SlackUser(id="U_BOB", name="bob", real_name="Bob Roberts", email="bob@example.com"),
        fetched_at=cache._clock.now_unix(),
    )
    msg = SlackMessage(ts="100.000", user="U1", text="hey <@U_BOB> ping")
    event = _normalize_message(
        msg=msg,
        channel=_channel(),
        cache=cache,
        own_user_id="U_OWN",
        source_name="slack",
    )
    assert "@Bob Roberts" in event.body or "@bob" in event.body
    assert "<@U_BOB>" not in event.body


def test_normalize_message_keeps_unknown_mention_literal(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    msg = SlackMessage(ts="100.000", user="U1", text="hey <@U_UNKNOWN>")
    event = _normalize_message(
        msg=msg,
        channel=_channel(),
        cache=cache,
        own_user_id="U_OWN",
        source_name="slack",
    )
    assert "<@U_UNKNOWN>" in event.body


def test_normalize_message_marks_mention_metadata(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    msg = SlackMessage(ts="100.000", user="U1", text="hey <@U_OWN> question")
    event = _normalize_message(
        msg=msg,
        channel=_channel(),
        cache=cache,
        own_user_id="U_OWN",
        source_name="slack",
    )
    assert event.metadata.is_mention is True
