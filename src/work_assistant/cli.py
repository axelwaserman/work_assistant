"""Top-level `wa` CLI."""

from __future__ import annotations

import click

from work_assistant.ingest.cli import ingest as _ingest_cmd


@click.group()
def cli() -> None:
    """work-assistant CLI."""


cli.add_command(_ingest_cmd)
