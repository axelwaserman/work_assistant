"""Test fakes used by ingest tests. Lives under `tests/` because nothing in
`src/` should depend on a fake."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TypeVar

from work_assistant.ingest.clock import Clock
from work_assistant.ingest.models import Batch, Cursor, NormalizedEvent, SlackMetadata
from work_assistant.ingest.source import Source
from work_assistant.mcp.client import MCPClient, MCPRequest, MCPResponse


class FakeClock(Clock):
    """A `Clock` that only moves when `advance()` is called."""

    def __init__(self, initial: datetime) -> None:
        self._now = initial

    def now(self) -> datetime:
        return self._now

    def advance(self, *, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)


_RespT = TypeVar("_RespT", bound=MCPResponse)


@dataclass(frozen=True)
class ScriptedReply:
    response: MCPResponse | None = None
    raises: BaseException | None = None


@dataclass(frozen=True)
class RecordedCall:
    request: MCPRequest


class FakeMCPClientError(RuntimeError):
    """Raised by FakeMCPClient on misuse (under-scripted call, type mismatch)."""


@dataclass
class FakeMCPClient(MCPClient):
    """In-memory MCPClient driven by a script keyed by request class name.

    Use the `MCPRequest` subclass `__name__` as the key so error messages name
    the offending call site clearly.
    """

    script: dict[str, list[ScriptedReply]] = field(default_factory=dict)
    calls: list[RecordedCall] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.script = {k: list(v) for k, v in self.script.items()}

    async def call(
        self,
        request: MCPRequest,
        response_model: type[_RespT],
    ) -> _RespT:
        self.calls.append(RecordedCall(request=request))
        key = type(request).__name__
        if key not in self.script or not self.script[key]:
            raise FakeMCPClientError(
                f"FakeMCPClient: unexpected call to {key} with "
                f"args={request.model_dump()!r}. "
                f"Scripted keys: {list(self.script)!r}"
            )
        scripted = self.script[key].pop(0)
        if scripted.raises is not None:
            raise scripted.raises
        if not isinstance(scripted.response, response_model):
            actual = type(scripted.response).__name__ if scripted.response is not None else "None"
            raise FakeMCPClientError(
                f"FakeMCPClient: scripted response for {key} is "
                f"{actual}, expected {response_model.__name__}"
            )
        return scripted.response


class StubSource(Source):
    """A `Source` whose `fetch()` yields a pre-scripted list of batches.

    Use `StubSource.make(batches=...)` to build a class on the fly with the
    required `name`/`mcp_server` set, then construct it with an `IngestContext`.
    Set `raise_after` to inject an exception after N batches have been yielded.
    """

    _scripted_batches: list[Batch] = []
    _raise_after: int | None = None
    _raise_exc: BaseException | None = None

    name = "stub"
    mcp_server = "stub"

    @classmethod
    def make(
        cls,
        *,
        name: str = "stub",
        mcp_server: str = "stub",
        batches: list[Batch] | None = None,
        raise_after: int | None = None,
        raise_exc: BaseException | None = None,
    ) -> type[Source]:
        attrs: dict[str, object] = {
            "name": name,
            "mcp_server": mcp_server,
            "_scripted_batches": list(batches or []),
            "_raise_after": raise_after,
            "_raise_exc": raise_exc,
        }
        return type(f"StubSource_{name}", (cls,), attrs)

    async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
        for index, batch in enumerate(self._scripted_batches):
            yield batch
            emitted = index + 1
            if (
                self._raise_after is not None
                and emitted >= self._raise_after
                and self._raise_exc is not None
            ):
                raise self._raise_exc

    def normalize_body(self, raw: str) -> tuple[str, bool]:
        return raw, False

    async def resolve_actor(self, raw_actor: str) -> str | None:
        return raw_actor

    def cursor_from_timestamp(self, ts: int) -> Cursor:
        return Cursor()


def make_event(
    *,
    source: str = "slack",
    source_id: str = "m1",
    body: str = "hello",
    occurred_at: int = 1_700_000_000,
) -> NormalizedEvent:
    """Helper: builds a NormalizedEvent with a Slack metadata variant."""
    md = SlackMetadata(
        channel_id="C1",
        channel_name="general",
        is_im=False,
        is_mpim=False,
        is_dm=False,
        is_mention=False,
        reactions_json="[]",
        files_json="[]",
    )
    return NormalizedEvent(
        source=source,  # type: ignore[arg-type]
        source_id=source_id,
        source_link=None,
        content_hash="0" * 64,
        occurred_at=occurred_at,
        actor=None,
        thread_key=None,
        kind="message",
        title=None,
        body=body,
        body_truncated=False,
        metadata=md,
    )
