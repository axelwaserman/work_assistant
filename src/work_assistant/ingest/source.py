"""The `Source` ABC every per-source implementation must satisfy.

Per repo convention (`CLAUDE.md`) this is an ABC, not a Protocol. ABC gives
us runtime validation at instantiation, reliable `isinstance()` checks, and
no structural-typing surprises when sources are registered dynamically.
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, ClassVar

from work_assistant.ingest.models import Batch, Cursor

if TYPE_CHECKING:
    from work_assistant.ingest.context import IngestContext


class Source(ABC):
    """Per-source ingest contract.

    `name` and `mcp_server` are class attributes set by each concrete source.
    The worker reads them to build the registry and group sources by bridge.
    """

    name: ClassVar[str]
    mcp_server: ClassVar[str]

    def __init__(self, ctx: IngestContext) -> None:
        self.ctx = ctx

    @abstractmethod
    def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
        """Async generator. Each yielded `Batch` is wrapped by the worker in
        a single SQLite transaction; the cursor advances on commit only.

        Implementations: `async def fetch(...) -> AsyncIterator[Batch]: yield ...`.
        """

    @abstractmethod
    def normalize_body(self, raw: str) -> tuple[str, bool]:
        """Strip / decode HTML / truncate to ~100 KB. Returns `(body, truncated)`."""

    @abstractmethod
    async def resolve_actor(self, raw_actor: str) -> str | None:
        """Map provider id to email when possible; else best-effort id; else None."""

    @abstractmethod
    def cursor_from_timestamp(self, ts: int) -> Cursor:
        """Synthesize a one-shot cursor for `wa ingest --since`. Per-source spec
        defines the mapping to its native cursor shape."""

    def compute_content_hash(self, source_id: str, body: str | None) -> str:
        """Concrete. `sha256(name + ':' + source_id + ':' + (body or ''))`."""
        payload = f"{self.name}:{source_id}:{body or ''}".encode()
        return hashlib.sha256(payload).hexdigest()
