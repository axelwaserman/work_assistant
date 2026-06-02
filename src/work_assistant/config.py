"""Configuration loader and secret accessor for work-assistant.

Configuration shape lives in `config.toml` and is validated with Pydantic.
Secrets live in the OS keyring under the service name `work-assistant`.
"""

from __future__ import annotations

import tomllib
from typing import Any

import keyring
from pydantic import BaseModel, ConfigDict, ValidationError

from work_assistant import paths

KEYRING_SERVICE = "work-assistant"


class ConfigError(Exception):
    """Raised when configuration cannot be loaded or validated."""


class _StrictBase(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BedrockModels(_StrictBase):
    sonnet: str
    opus: str
    haiku: str


class BedrockConfig(_StrictBase):
    region: str
    aws_profile: str
    models: BedrockModels


class McpConfig(_StrictBase):
    todoist_command: list[str]
    slack_command: list[str]
    workspace_command: list[str]


class IngestConfig(_StrictBase):
    backfill_days_slack: int
    backfill_days_gmail: int
    backfill_days_calendar: int


class Config(_StrictBase):
    bedrock: BedrockConfig
    mcp: McpConfig
    ingest: IngestConfig


def load() -> Config:
    """Load and validate `~/.work_assistant/config.toml`.

    Raises `ConfigError` with a clear message on any failure.
    """
    cfg_path = paths.config_path()
    if not cfg_path.exists():
        raise ConfigError(f"config.toml not found at {cfg_path}")
    try:
        raw: dict[str, Any] = tomllib.loads(cfg_path.read_text())
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {cfg_path}: {exc}") from exc
    try:
        return Config.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"config validation failed:\n{exc}") from exc


def get_secret(name: str) -> str:
    """Return a secret from the OS keyring, or raise `ConfigError` if missing."""
    value = keyring.get_password(KEYRING_SERVICE, name)
    if value is None:
        raise ConfigError(
            f"missing secret '{name}' in keyring service '{KEYRING_SERVICE}'. "
            f"Set it with: wa secrets set {name}"
        )
    return value
