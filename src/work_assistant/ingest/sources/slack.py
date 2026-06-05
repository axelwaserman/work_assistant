"""Slack source implementation.

Cron-driven incremental pull. Per-channel cursor stored in `ingest_cursors`
under `source='slack'`. Per-source plans were promised cursor-shape parsing
in the Phase 1 scaffold; this module reads its own cursor row via
`ctx.db` rather than relying on the runner's deferred parsing.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field

from work_assistant.ingest.models import Cursor
from work_assistant.mcp.client import MCPRequest, MCPResponse


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


class SlackChannel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    id: str
    name: str
    is_member: bool = False
    is_archived: bool = False
    is_im: bool = False
    is_mpim: bool = False


class SlackMessage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    ts: str
    user: str | None = None
    text: str = ""
    thread_ts: str | None = None
    subtype: str | None = None


class SlackUser(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    id: str
    name: str
    real_name: str | None = None
    email: str | None = None


class _ResponseMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    next_cursor: str | None = None


# --- conversations.list ---


class ConversationsListRequest(MCPRequest):
    tool_name: ClassVar[str] = "conversations_list"

    types: str = "public_channel,private_channel,im,mpim"
    limit: int = 1000
    exclude_archived: bool = True


class ConversationsListResponse(MCPResponse):
    channels: list[SlackChannel]
    response_metadata: _ResponseMetadata | None = None


# --- conversations.history ---


class ConversationsHistoryRequest(MCPRequest):
    tool_name: ClassVar[str] = "conversations_history"

    channel: str
    oldest: str
    limit: int = 200


class ConversationsHistoryResponse(MCPResponse):
    messages: list[SlackMessage]
    has_more: bool = False
    response_metadata: _ResponseMetadata | None = None


# --- conversations.replies ---


class ConversationsRepliesRequest(MCPRequest):
    tool_name: ClassVar[str] = "conversations_replies"

    channel: str
    ts: str
    limit: int = 200


class ConversationsRepliesResponse(MCPResponse):
    messages: list[SlackMessage]
    has_more: bool = False


# --- users.info ---


class UsersInfoRequest(MCPRequest):
    tool_name: ClassVar[str] = "users_info"

    user: str


class UsersInfoResponse(MCPResponse):
    user: SlackUser


# --- chat.getPermalink ---


class GetPermalinkRequest(MCPRequest):
    tool_name: ClassVar[str] = "chat_get_permalink"

    channel: str
    message_ts: str


class GetPermalinkResponse(MCPResponse):
    permalink: str


# --- auth.test ---


class AuthTestRequest(MCPRequest):
    tool_name: ClassVar[str] = "auth_test"


class AuthTestResponse(MCPResponse):
    user_id: str
    team_id: str
