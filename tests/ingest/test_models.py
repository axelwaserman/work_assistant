"""Tests for work_assistant.ingest.models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from work_assistant.ingest import models


def test_cursor_is_frozen() -> None:
    cur = models.Cursor()
    with pytest.raises(ValidationError):
        cur.x = 1  # type: ignore[attr-defined]  # frozen models reject any attribute set
    models.Cursor.model_validate(cur.model_dump())  # safe parse still works


def test_slack_metadata_round_trips_through_union() -> None:
    md = models.SlackMetadata(
        channel_id="C1",
        channel_name="general",
        is_im=False,
        is_mpim=False,
        is_dm=False,
        is_mention=True,
        reactions_json="[]",
        files_json="[]",
    )
    payload = md.model_dump_json()
    parsed = models.SlackMetadata.model_validate_json(payload)
    assert parsed == md


def test_gmail_metadata_required_fields() -> None:
    with pytest.raises(ValidationError):
        models.GmailMetadata(  # type: ignore[call-arg]
            to=["a@b"],
            cc=[],
            labels=[],
            has_attachments=False,
            # internal_date missing
        )


def test_normalized_event_minimum_construction() -> None:
    md = models.SlackMetadata(
        channel_id="C1",
        channel_name="g",
        is_im=False,
        is_mpim=False,
        is_dm=False,
        is_mention=False,
        reactions_json="[]",
        files_json="[]",
    )
    ev = models.NormalizedEvent(
        source="slack",
        source_id="m1",
        source_link=None,
        content_hash="0" * 64,
        occurred_at=1_700_000_000,
        actor=None,
        thread_key=None,
        kind="message",
        title=None,
        body="hello",
        body_truncated=False,
        metadata=md,
    )
    assert ev.source == "slack"
    assert ev.metadata.kind == "slack"


def test_batch_holds_events_and_status() -> None:
    cursor = models.Cursor()
    batch = models.Batch(events=[], next_cursor=cursor, status="ok")
    assert batch.status == "ok"
    assert batch.events == []
    assert batch.next_cursor is cursor


def test_metadata_union_resolves_by_discriminator() -> None:
    raw = {
        "kind": "calendar",
        "start": "2026-06-04T10:00:00Z",
        "end": "2026-06-04T11:00:00Z",
        "attendees_json": "[]",
        "hangout_link": None,
        "attachments_json": "[]",
        "is_organizer": True,
    }
    parsed = models.parse_event_metadata(raw)
    assert isinstance(parsed, models.CalendarMetadata)
    assert parsed.is_organizer is True
