"""Slack source implementation.

Cron-driven incremental pull. Per-channel cursor stored in `ingest_cursors`
under `source='slack'`. Per-source plans were promised cursor-shape parsing
in the Phase 1 scaffold; this module reads its own cursor row via
`ctx.db` rather than relying on the runner's deferred parsing.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from work_assistant.ingest.models import Cursor


class ChannelCursor(BaseModel):
    """Per-channel high-water-mark for incremental pulls.

    `last_seen_ts` is a Slack ts string (e.g. `"1717420800.123456"`); pass it
    as the `oldest` argument to `conversations.history` to fetch newer messages.
    """

    model_config = ConfigDict(frozen=True)

    channel_id: str
    channel_name: str
    last_seen_ts: str


class SlackCursor(Cursor):
    """Frozen pydantic cursor for the Slack source.

    `channels` is a list (not a dict) so JSON dumps stay self-describing.
    Lookups are O(N) but N == joined-channel-count which is small.
    """

    channels: list[ChannelCursor] = Field(default_factory=list)

    def lookup(self, channel_id: str) -> ChannelCursor | None:
        for ch in self.channels:
            if ch.channel_id == channel_id:
                return ch
        return None

    def with_updated(self, ch: ChannelCursor) -> SlackCursor:
        """Return a new SlackCursor with `ch` replacing or appended."""
        replaced = False
        new_channels: list[ChannelCursor] = []
        for existing in self.channels:
            if existing.channel_id == ch.channel_id:
                new_channels.append(ch)
                replaced = True
            else:
                new_channels.append(existing)
        if not replaced:
            new_channels.append(ch)
        return SlackCursor(channels=new_channels)
