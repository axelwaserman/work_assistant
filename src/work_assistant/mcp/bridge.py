"""Subprocess bridge for MCP servers.

Wraps the Python MCP SDK's stdio client so the rest of the codebase has one
typed entry point per server: spawn the server, call tools, list tools, run a
health check, and clean up on context exit.
"""

from __future__ import annotations

import logging
from contextlib import AsyncExitStack
from typing import Any, Self

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CallToolResult, Tool

from work_assistant import paths

_log = logging.getLogger(__name__)


class MCPBridge:
    """Manages a single stdio MCP server subprocess."""

    def __init__(self, name: str, command: list[str], env: dict[str, str] | None = None) -> None:
        if not command:
            raise ValueError("command must be a non-empty list")
        self._name = name
        self._command = command
        self._env = env
        self._stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None
        self._init_error: BaseException | None = None

    async def __aenter__(self) -> Self:
        self._stack = AsyncExitStack()
        await self._stack.__aenter__()

        stderr_path = paths.logs_dir() / f"mcp-{self._name}.stderr.log"
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_file = self._stack.enter_context(
            open(stderr_path, "a", encoding="utf-8", buffering=1)  # noqa: SIM115
        )

        params = StdioServerParameters(
            command=self._command[0],
            args=self._command[1:],
            env=self._env,
        )
        try:
            read, write = await self._stack.enter_async_context(
                stdio_client(params, errlog=stderr_file)
            )
            session = await self._stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            self._session = session
        except Exception as exc:
            # Defer init failures so the context manager body can still run
            # cleanup (e.g., flush stderr capture). Any subsequent tool call
            # surfaces the failure via _require_session.
            self._init_error = exc
            _log.warning(
                "mcp bridge init failed",
                extra={"server": self._name, "error": f"{type(exc).__name__}: {exc}"},
            )
            return self
        _log.info("mcp bridge started", extra={"server": self._name})
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        if self._stack is not None:
            await self._stack.aclose()
            self._stack = None
            self._session = None
        _log.info("mcp bridge stopped", extra={"server": self._name})

    @property
    def name(self) -> str:
        return self._name

    def _require_session(self) -> ClientSession:
        if self._init_error is not None:
            raise RuntimeError(
                f"MCPBridge[{self._name}] failed to initialize: "
                f"{type(self._init_error).__name__}: {self._init_error}"
            ) from self._init_error
        if self._session is None:
            raise RuntimeError(f"MCPBridge[{self._name}] not started; use 'async with'")
        return self._session

    async def list_tools(self) -> list[Tool]:
        session = self._require_session()
        result = await session.list_tools()
        return list(result.tools)

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> CallToolResult:
        session = self._require_session()
        return await session.call_tool(name, arguments)

    async def health_check(self) -> tuple[bool, str]:
        """Return (ok, detail). `detail` includes a comma-separated tool list on success."""
        try:
            tools = await self.list_tools()
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"
        names = ", ".join(sorted(t.name for t in tools))
        return True, f"tools=[{names}]"
