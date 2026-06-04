"""Test fakes used by ingest tests. Lives under `tests/` because nothing in
`src/` should depend on a fake."""

from __future__ import annotations

from datetime import datetime, timedelta

from work_assistant.ingest.clock import Clock


class FakeClock(Clock):
    """A `Clock` that only moves when `advance()` is called."""

    def __init__(self, initial: datetime) -> None:
        self._now = initial

    def now(self) -> datetime:
        return self._now

    def advance(self, *, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)
