"""Source-facing MCP adapter.

`MCPBridge` (`src/work_assistant/mcp/bridge.py`) returns the MCP SDK's
`CallToolResult` with `Any` payloads. `MCPClient` is the ABC every source
calls; `BridgeMCPClient` is the production impl that wraps `MCPBridge`,
parses the response, and contains all `Any` at the seam.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any, ClassVar, TypeVar

from pydantic import BaseModel, ConfigDict

from work_assistant.ingest.errors import MCPTimeoutError
from work_assistant.mcp.bridge import MCPBridge

MCP_CALL_TIMEOUT_S_DEFAULT = 60.0


class MCPRequest(BaseModel):
    """Per-tool typed request. Subclass per (server, tool).

    Subclasses MUST set `tool_name: ClassVar[str]` to the MCP tool's name.
    Argument fields go on the subclass; `model_dump()` becomes the arguments
    dict passed to `MCPBridge.call_tool`.
    """

    model_config = ConfigDict(frozen=True)

    tool_name: ClassVar[str] = ""


class MCPResponse(BaseModel):
    """Per-tool typed response. Subclass per (server, tool)."""

    model_config = ConfigDict(frozen=True)


_RespT = TypeVar("_RespT", bound=MCPResponse)


class MCPClient(ABC):
    @abstractmethod
    async def call(
        self,
        request: MCPRequest,
        response_model: type[_RespT],
    ) -> _RespT:
        """Dispatch a tool call and parse the result into `response_model`."""


class BridgeMCPClient(MCPClient):
    """Production adapter: wraps `MCPBridge`. The only place `Any` lives."""

    def __init__(
        self,
        bridge: MCPBridge,
        timeout_s: float = MCP_CALL_TIMEOUT_S_DEFAULT,
    ) -> None:
        self._bridge = bridge
        self._timeout_s = timeout_s

    async def call(
        self,
        request: MCPRequest,
        response_model: type[_RespT],
    ) -> _RespT:
        tool_name = type(request).tool_name
        if not tool_name:
            raise ValueError(f"{type(request).__name__} missing `tool_name: ClassVar[str]`")
        arguments: dict[str, Any] = request.model_dump()
        try:
            result = await asyncio.wait_for(
                self._bridge.call_tool(tool_name, arguments),
                timeout=self._timeout_s,
            )
        except TimeoutError as exc:
            raise MCPTimeoutError(
                f"MCP call {tool_name!r} timed out after {self._timeout_s}s"
            ) from exc
        return self._parse(result, response_model)

    @staticmethod
    def _parse(result: Any, response_model: type[_RespT]) -> _RespT:
        """Pull the first text block out of `CallToolResult` and validate it.

        Most MCP tools return a single text content item with a JSON payload.
        For tools returning a bare string (e.g. `ping -> "pong"`), we wrap it
        as `{"text": "<value>"}` so a one-field `MCPResponse` parses cleanly.
        """
        content = result.content
        if not content:
            raise ValueError(
                f"MCP response had no content; cannot parse into {response_model.__name__}"
            )
        first = content[0]
        text: str | None = getattr(first, "text", None)
        if text is None:
            raise ValueError(
                f"MCP response first content item has no `.text`; got {type(first).__name__}"
            )
        stripped = text.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            return response_model.model_validate_json(text)
        return response_model.model_validate({"text": text})
