"""Test fakes used by ingest tests. Lives under `tests/` because nothing in
`src/` should depend on a fake."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TypeVar

from work_assistant.ingest.clock import Clock
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
