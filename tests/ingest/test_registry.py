"""Tests for work_assistant.ingest.registry."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from work_assistant.ingest.models import Batch, Cursor
from work_assistant.ingest.registry import (
    SOURCES,
    UnknownSourceError,
    select_sources,
)
from work_assistant.ingest.source import Source


class _StubSlack(Source):
    name = "slack"
    mcp_server = "slack"

    async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
        if False:
            yield  # pragma: no cover

    def normalize_body(self, raw: str) -> tuple[str, bool]:
        return raw, False

    async def resolve_actor(self, raw_actor: str) -> str | None:
        return None

    def cursor_from_timestamp(self, ts: int) -> Cursor:
        return Cursor()


def test_default_registry_contains_slack() -> None:
    """Importing work_assistant.ingest.sources registers SlackSource via side-effect."""
    import work_assistant.ingest.sources  # noqa: F401  -- side-effect import

    assert "slack" in SOURCES
    assert SOURCES["slack"].name == "slack"


def test_select_sources_filters_to_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    registry: dict[str, type[Source]] = {"slack": _StubSlack}
    selected = select_sources(registry=registry, requested=["slack"])
    assert selected == {"slack": _StubSlack}


def test_select_sources_raises_on_unknown_name() -> None:
    registry: dict[str, type[Source]] = {"slack": _StubSlack}
    with pytest.raises(UnknownSourceError, match="gmail"):
        select_sources(registry=registry, requested=["slack", "gmail"])


def test_select_sources_returns_all_when_requested_is_none() -> None:
    registry: dict[str, type[Source]] = {"slack": _StubSlack}
    selected = select_sources(registry=registry, requested=None)
    assert selected == {"slack": _StubSlack}


def test_select_sources_returns_empty_when_requested_is_empty() -> None:
    registry: dict[str, type[Source]] = {"slack": _StubSlack}
    selected = select_sources(registry=registry, requested=[])
    assert selected == {}
