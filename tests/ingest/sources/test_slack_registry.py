"""Importing the sources package must register SlackSource."""

from __future__ import annotations


def test_importing_sources_registers_slack() -> None:
    from work_assistant.ingest import sources  # noqa: F401
    from work_assistant.ingest.registry import SOURCES
    from work_assistant.ingest.sources.slack import SlackSource
    assert SOURCES.get("slack") is SlackSource
