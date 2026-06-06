"""Tests for the in-DB Slack user cache."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from tests.ingest.fakes import FakeClock
from work_assistant.ingest.context import SqliteDbFactory
from work_assistant.ingest.sources.slack import SlackUser, _SlackUserCache


def _cache(initialized_db: Path, *, now: datetime | None = None) -> _SlackUserCache:
    clock = FakeClock(now or datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC))
    db = SqliteDbFactory(db_path=initialized_db)
    return _SlackUserCache(db=db, clock=clock)


def test_get_returns_none_for_missing_user(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    assert cache.get("U_MISSING") is None


def test_upsert_then_get_returns_user(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    user = SlackUser(id="U1", name="alice", real_name="Alice", email="alice@example.com")
    cache.upsert(user, fetched_at=cache._clock.now_unix())
    found = cache.get("U1")
    assert found is not None
    assert found.email == "alice@example.com"
    assert found.name == "Alice"  # display_name preferred when stored


def test_get_returns_none_for_stale_entry(initialized_db: Path) -> None:
    """Stored entry older than 7 days should be treated as missing."""
    cache = _cache(initialized_db)
    user = SlackUser(id="U1", name="alice", real_name=None, email=None)
    eight_days_ago = cache._clock.now_unix() - 8 * 86400
    cache.upsert(user, fetched_at=eight_days_ago)
    assert cache.get("U1") is None


def test_upsert_replaces_existing_row(initialized_db: Path) -> None:
    cache = _cache(initialized_db)
    cache.upsert(
        SlackUser(id="U1", name="alice", real_name="Alice", email=None),
        fetched_at=cache._clock.now_unix(),
    )
    cache.upsert(
        SlackUser(id="U1", name="alice", real_name="Alice Smith", email="alice@example.com"),
        fetched_at=cache._clock.now_unix(),
    )
    found = cache.get("U1")
    assert found is not None
    assert found.name == "Alice Smith"
    assert found.email == "alice@example.com"
