"""Source-facing data shapes shared by the worker and every `Source` impl.

Every model is frozen. `NormalizedEvent.metadata` is a discriminated union
across the four supported sources; per-source code constructs the variant that
matches its `Source.name`.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


class Cursor(BaseModel):
    """Source-specific cursor state. Each source subclasses this with its own
    fields (e.g. `SlackCursor.channels: dict[str, str]`). The base is empty so
    a default cursor (e.g. for `--since`) can still be serialized."""

    model_config = ConfigDict(frozen=True)


class SlackMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["slack"] = "slack"
    channel_id: str
    channel_name: str
    is_im: bool
    is_mpim: bool
    is_dm: bool
    is_mention: bool
    reactions_json: str
    files_json: str


class GmailMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["gmail"] = "gmail"
    to: list[str]
    cc: list[str]
    labels: list[str]
    has_attachments: bool
    internal_date: int


class CalendarMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["calendar"] = "calendar"
    start: str
    end: str
    attendees_json: str
    hangout_link: str | None
    attachments_json: str
    is_organizer: bool


class TodoistMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["todoist"] = "todoist"
    project_id: str
    project_path: str
    labels: list[str]
    due_json: str | None
    priority: int
    completed: bool
    parent_id: str | None
    ws_footer_json: str | None


EventMetadata = Annotated[
    SlackMetadata | GmailMetadata | CalendarMetadata | TodoistMetadata,
    Field(discriminator="kind"),
]


_metadata_adapter: TypeAdapter[
    SlackMetadata | GmailMetadata | CalendarMetadata | TodoistMetadata
] = TypeAdapter(EventMetadata)


def parse_event_metadata(
    payload: dict[str, object],
) -> SlackMetadata | GmailMetadata | CalendarMetadata | TodoistMetadata:
    """Parse a metadata dict into the right variant via the `kind` discriminator."""
    return _metadata_adapter.validate_python(payload)


SourceName = Literal["slack", "gmail", "calendar", "todoist"]
EventKind = Literal["message", "email", "meeting", "doc", "task_state"]


class NormalizedEvent(BaseModel):
    """Aligned with the `events` table in `docs/02-data-model.md`.

    Fields map 1:1 to columns except:
    - `body_truncated` is NOT a column. The worker logs it; sources should
      truncate before constructing the event.
    - `metadata` becomes `metadata_json` at write time via `model_dump_json()`.
    """

    model_config = ConfigDict(frozen=True)

    source: SourceName
    source_id: str
    source_link: str | None
    content_hash: str
    occurred_at: int
    actor: str | None
    thread_key: str | None
    kind: EventKind
    title: str | None
    body: str | None
    body_truncated: bool
    metadata: EventMetadata


class Batch(BaseModel):
    """A unit of fetch progress. The worker wraps each `Batch` in a single
    SQLite transaction; the cursor advances on commit."""

    model_config = ConfigDict(frozen=True)

    events: list[NormalizedEvent]
    next_cursor: Cursor
    status: Literal["ok", "partial"]
