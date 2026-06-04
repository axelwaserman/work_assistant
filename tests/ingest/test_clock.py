"""Tests for work_assistant.ingest.clock."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from tests.ingest.fakes import FakeClock
from work_assistant.ingest.clock import SystemClock


def test_system_clock_returns_utc_now() -> None:
    clock = SystemClock()
    before = datetime.now(UTC)
    got = clock.now()
    after = datetime.now(UTC)
    assert before <= got <= after
    assert got.tzinfo is UTC


def test_system_clock_now_unix() -> None:
    clock = SystemClock()
    delta = abs(clock.now_unix() - int(datetime.now(UTC).timestamp()))
    assert delta <= 2


def test_fake_clock_does_not_advance_on_its_own() -> None:
    start = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)
    clock = FakeClock(start)
    assert clock.now() == start
    assert clock.now() == start
    assert clock.now_unix() == int(start.timestamp())


def test_fake_clock_advance() -> None:
    start = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)
    clock = FakeClock(start)
    clock.advance(seconds=90)
    assert clock.now() == start + timedelta(seconds=90)
