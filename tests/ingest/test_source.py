"""Tests for the Source ABC and its concrete content-hash helper."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator

import pytest

from work_assistant.ingest.models import Batch, Cursor
from work_assistant.ingest.source import Source


def test_cannot_instantiate_abstract_source() -> None:
    with pytest.raises(TypeError):
        Source(ctx=None)  # type: ignore[abstract]


def test_subclass_missing_abstract_methods_cannot_instantiate() -> None:
    class Half(Source):
        name = "slack"
        mcp_server = "slack"

        async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
            if False:
                yield  # pragma: no cover

    with pytest.raises(TypeError):
        Half(ctx=None)  # type: ignore[abstract]


def test_complete_subclass_can_instantiate_and_hash() -> None:
    class Done(Source):
        name = "slack"
        mcp_server = "slack"

        async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
            if False:
                yield  # pragma: no cover

        def normalize_body(self, raw: str) -> tuple[str, bool]:
            return raw, False

        async def resolve_actor(self, raw_actor: str) -> str | None:
            return raw_actor

        def cursor_from_timestamp(self, ts: int) -> Cursor:
            return Cursor()

    src = Done(ctx=None)  # type: ignore[arg-type]
    got = src.compute_content_hash("m1", "hello")
    expected = hashlib.sha256(b"slack:m1:hello").hexdigest()
    assert got == expected


def test_content_hash_handles_none_body() -> None:
    class Done(Source):
        name = "gmail"
        mcp_server = "workspace"

        async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
            if False:
                yield  # pragma: no cover

        def normalize_body(self, raw: str) -> tuple[str, bool]:
            return raw, False

        async def resolve_actor(self, raw_actor: str) -> str | None:
            return None

        def cursor_from_timestamp(self, ts: int) -> Cursor:
            return Cursor()

    src = Done(ctx=None)  # type: ignore[arg-type]
    got = src.compute_content_hash("e1", None)
    expected = hashlib.sha256(b"gmail:e1:").hexdigest()
    assert got == expected
