"""Tests for work_assistant.mcp.bridge.

Uses a mock stdio MCP server spawned from a tiny inline Python script so we
don't depend on Node/npm in the test environment.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from work_assistant import paths
from work_assistant.mcp import bridge

# Minimal MCP-over-stdio server that responds to `tools/list` with one tool.
# Using the python `mcp` SDK's FastMCP server so this is real, not mocked.
SERVER_SCRIPT = """\
import sys
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("mock")

@mcp.tool()
def ping() -> str:
    return "pong"

if __name__ == "__main__":
    mcp.run(transport="stdio")
"""


@pytest.fixture()
def mock_server_path(tmp_path: Path) -> Path:
    p = tmp_path / "mock_mcp_server.py"
    p.write_text(SERVER_SCRIPT)
    return p


@pytest.mark.asyncio
async def test_start_and_list_tools(isolated_home: Path, mock_server_path: Path) -> None:
    paths.ensure_dirs()
    async with bridge.MCPBridge(
        name="mock",
        command=[sys.executable, str(mock_server_path)],
    ) as br:
        tools = await br.list_tools()
        names = [t.name for t in tools]
        assert "ping" in names


@pytest.mark.asyncio
async def test_call_tool(isolated_home: Path, mock_server_path: Path) -> None:
    paths.ensure_dirs()
    async with bridge.MCPBridge(
        name="mock",
        command=[sys.executable, str(mock_server_path)],
    ) as br:
        result = await br.call_tool("ping", {})
        # The python SDK returns a CallToolResult; first content item is text.
        text = result.content[0].text  # type: ignore[union-attr]
        assert text == "pong"


@pytest.mark.asyncio
async def test_stderr_is_captured_to_log(isolated_home: Path, mock_server_path: Path) -> None:
    paths.ensure_dirs()
    async with bridge.MCPBridge(
        name="mock-err",
        command=[
            sys.executable,
            "-c",
            "import sys; sys.stderr.write('boom\\n'); import time; time.sleep(0.5)",
        ],
    ) as br:
        # We don't actually call list_tools (server isn't a real MCP); just
        # verify the stderr log file fills up on shutdown.
        _ = br
    err_log = paths.logs_dir() / "mcp-mock-err.stderr.log"
    assert err_log.exists()
    assert "boom" in err_log.read_text()


@pytest.mark.asyncio
async def test_health_check_returns_status(isolated_home: Path, mock_server_path: Path) -> None:
    paths.ensure_dirs()
    async with bridge.MCPBridge(
        name="mock",
        command=[sys.executable, str(mock_server_path)],
    ) as br:
        ok, detail = await br.health_check()
        assert ok is True
        assert "ping" in detail


@pytest.mark.asyncio
async def test_failed_init_propagates_on_tool_call(isolated_home: Path) -> None:
    paths.ensure_dirs()
    # Subprocess that exits immediately — not a real MCP server, so init fails.
    async with bridge.MCPBridge(
        name="mock-bad",
        command=[sys.executable, "-c", "import sys; sys.exit(1)"],
    ) as br:
        with pytest.raises(RuntimeError, match="mock-bad") as excinfo:
            await br.list_tools()
        assert excinfo.value.__cause__ is not None
