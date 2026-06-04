"""Tests for the MCPClient ABC, BridgeMCPClient, and FakeMCPClient."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import ClassVar

import pytest

from tests.ingest.fakes import FakeMCPClient, FakeMCPClientError, ScriptedReply
from work_assistant import paths
from work_assistant.ingest.errors import MCPTimeoutError
from work_assistant.mcp.bridge import MCPBridge
from work_assistant.mcp.client import (
    BridgeMCPClient,
    MCPRequest,
    MCPResponse,
)


class _PingRequest(MCPRequest):
    tool_name: ClassVar[str] = "ping"


class _PingResponse(MCPResponse):
    text: str


class _OtherResponse(MCPResponse):
    other: str


SERVER_SCRIPT = """\
from mcp.server.fastmcp import FastMCP
mcp = FastMCP("mock")

@mcp.tool()
def ping() -> str:
    return "pong"

@mcp.tool()
def echo(text: str) -> str:
    return text

if __name__ == "__main__":
    mcp.run(transport="stdio")
"""


@pytest.fixture()
def mock_server_path(tmp_path: Path) -> Path:
    p = tmp_path / "mock_mcp_server.py"
    p.write_text(SERVER_SCRIPT, encoding="utf-8")
    return p


@pytest.mark.asyncio
async def test_bridge_client_parses_text_response(
    isolated_home: Path, mock_server_path: Path
) -> None:
    paths.ensure_dirs()
    async with MCPBridge(name="mock", command=[sys.executable, str(mock_server_path)]) as br:
        client = BridgeMCPClient(br)
        resp = await client.call(_PingRequest(), _PingResponse)
        assert resp.text == "pong"


@pytest.mark.asyncio
async def test_bridge_client_raises_mcp_timeout(
    isolated_home: Path, mock_server_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the bridge call exceeds the timeout, BridgeMCPClient raises MCPTimeoutError."""
    paths.ensure_dirs()
    async with MCPBridge(name="mock", command=[sys.executable, str(mock_server_path)]) as br:
        client = BridgeMCPClient(br, timeout_s=0.01)

        async def slow_call(*a: object, **kw: object) -> object:
            await asyncio.sleep(1.0)
            raise AssertionError("should have timed out")

        monkeypatch.setattr(br, "call_tool", slow_call)
        with pytest.raises(MCPTimeoutError):
            await client.call(_PingRequest(), _PingResponse)


@pytest.mark.asyncio
async def test_fake_returns_scripted_response() -> None:
    fake = FakeMCPClient(
        script={"_PingRequest": [ScriptedReply(response=_PingResponse(text="pong"))]}
    )
    resp = await fake.call(_PingRequest(), _PingResponse)
    assert resp.text == "pong"
    assert len(fake.calls) == 1


@pytest.mark.asyncio
async def test_fake_raises_on_unscripted_call() -> None:
    fake = FakeMCPClient(script={})
    with pytest.raises(FakeMCPClientError, match="unexpected call"):
        await fake.call(_PingRequest(), _PingResponse)


@pytest.mark.asyncio
async def test_fake_raises_on_scripted_failure() -> None:
    fake = FakeMCPClient(script={"_PingRequest": [ScriptedReply(raises=RuntimeError("boom"))]})
    with pytest.raises(RuntimeError, match="boom"):
        await fake.call(_PingRequest(), _PingResponse)


@pytest.mark.asyncio
async def test_fake_rejects_response_type_mismatch() -> None:
    fake = FakeMCPClient(
        script={"_PingRequest": [ScriptedReply(response=_OtherResponse(other="x"))]}
    )
    with pytest.raises(FakeMCPClientError, match="expected _PingResponse"):
        await fake.call(_PingRequest(), _PingResponse)
