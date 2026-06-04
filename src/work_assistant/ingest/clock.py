"""Clock abstraction so tests can drive lock-TTL and heartbeat code paths
deterministically. Repo convention: ABC, not Protocol."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime


class Clock(ABC):
    @abstractmethod
    def now(self) -> datetime:
        """Return the current time as a UTC-aware `datetime`."""

    def now_unix(self) -> int:
        """Convenience: unix seconds. Default impl uses `now()`."""
        return int(self.now().timestamp())


class SystemClock(Clock):
    def now(self) -> datetime:
        return datetime.now(UTC)
