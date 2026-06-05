"""Slack source implementation.

Cron-driven incremental pull. Per-channel cursor stored in `ingest_cursors`
under `source='slack'`. Per-source plans were promised cursor-shape parsing
in the Phase 1 scaffold; this module reads its own cursor row via
`ctx.db` rather than relying on the runner's deferred parsing.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import AsyncIterator
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field

from work_assistant.ingest.clock import Clock
from work_assistant.ingest.context import DbFactory
from work_assistant.ingest.errors import IngestError, PermanentIngestError, TransientIngestError
from work_assistant.ingest.models import Batch, Cursor, NormalizedEvent, SlackMetadata, SourceName
from work_assistant.ingest.source import Source
from work_assistant.mcp.client import MCPRequest, MCPResponse

USER_CACHE_TTL_SECONDS = 7 * 86400


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


class _SlackUserCache:
    """SQLite-backed cache of `users.info` results.

    Wraps the `slack_users` table from migration 0002. Returns `None` for
    rows older than `USER_CACHE_TTL_SECONDS` so callers refetch.
    """

    def __init__(self, db: DbFactory, clock: Clock) -> None:
        self._db = db
        self._clock = clock

    def get(self, user_id: str) -> SlackUser | None:
        with self._db.open() as conn:
            row = conn.execute(
                "SELECT user_id, email, display_name, fetched_at "
                "FROM slack_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        if row is None:
            return None
        if self.is_stale(row["fetched_at"]):
            return None
        return SlackUser(
            id=row["user_id"],
            name=row["display_name"],
            real_name=None,
            email=row["email"],
        )

    def upsert(self, user: SlackUser, fetched_at: int) -> None:
        with self._db.open() as conn:
            conn.execute(
                "INSERT INTO slack_users(user_id, email, display_name, fetched_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET "
                " email = excluded.email,"
                " display_name = excluded.display_name,"
                " fetched_at = excluded.fetched_at",
                (user.id, user.email, user.real_name or user.name, fetched_at),
            )

    def is_stale(self, fetched_at: int) -> bool:
        return (self._clock.now_unix() - fetched_at) > USER_CACHE_TTL_SECONDS


_MENTION_RE = re.compile(r"<@([A-Z0-9_]+)>")
BODY_MAX_BYTES = 100_000


def _thread_eligible(
    top: SlackMessage,
    *,
    replies: list[SlackMessage] | None,
    own_user_id: str,
) -> bool:
    """Return True iff the user authored or was @-mentioned at any depth."""
    own_marker = f"<@{own_user_id}>"
    if top.user == own_user_id:
        return True
    if own_marker in top.text:
        return True
    if replies is None:
        return False
    for reply in replies:
        if reply.user == own_user_id:
            return True
        if own_marker in reply.text:
            return True
    return False


def _rewrite_mentions(text: str, cache: _SlackUserCache) -> str:
    def _replace(match: re.Match[str]) -> str:
        user_id = match.group(1)
        cached = cache.get(user_id)
        if cached is None:
            return match.group(0)  # leave literal
        return f"@{cached.name}"

    return _MENTION_RE.sub(_replace, text)


def _truncate_body(body: str) -> tuple[str, bool]:
    """Return (body, truncated). UTF-8 byte-bounded; drop incomplete trailing codepoint."""
    encoded = body.encode("utf-8")
    if len(encoded) <= BODY_MAX_BYTES:
        return body, False
    truncated_bytes = encoded[:BODY_MAX_BYTES]
    return truncated_bytes.decode("utf-8", errors="ignore"), True


def _normalize_message(
    *,
    msg: SlackMessage,
    channel: SlackChannel,
    cache: _SlackUserCache,
    own_user_id: str,
    source_name: SourceName,
) -> NormalizedEvent:
    """Build a NormalizedEvent from a Slack message + channel + actor cache."""
    rewritten = _rewrite_mentions(msg.text, cache)
    body, truncated = _truncate_body(rewritten)
    own_marker = f"<@{own_user_id}>"
    is_mention = own_marker in msg.text
    thread_key = msg.thread_ts or msg.ts
    source_id = f"{channel.id}:{msg.ts}"
    occurred_at = int(float(msg.ts))
    payload = f"{source_name}:{source_id}:{body}".encode()
    content_hash = hashlib.sha256(payload).hexdigest()

    metadata = SlackMetadata(
        channel_id=channel.id,
        channel_name=channel.name,
        is_im=channel.is_im,
        is_mpim=channel.is_mpim,
        is_dm=channel.is_im,
        is_mention=is_mention,
        reactions_json="[]",
        files_json="[]",
    )

    return NormalizedEvent(
        source=source_name,
        source_id=source_id,
        source_link=None,
        content_hash=content_hash,
        occurred_at=occurred_at,
        actor=msg.user,
        thread_key=thread_key,
        kind="message",
        title=body[:80] if body else None,
        body=body,
        body_truncated=truncated,
        metadata=metadata,
    )


_PERMANENT_SLACK_ERRORS = frozenset(
    {
        "invalid_auth",
        "account_inactive",
        "not_authed",
        "token_revoked",
    }
)

_SKIPPABLE_PER_CHANNEL_ERRORS = frozenset(
    {
        "channel_not_found",
        "not_in_channel",
    }
)


class SlackError(Exception):
    """Raised by the MCP layer (or test fakes) carrying a Slack-style error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def map_slack_error(code: str) -> IngestError:
    """Map Slack error codes to our ingest exception hierarchy."""
    if code in _PERMANENT_SLACK_ERRORS:
        return PermanentIngestError(f"slack auth: {code}")
    if code == "rate_limited":
        return TransientIngestError(f"slack rate limited; persist cursor and exit: {code}")
    return TransientIngestError(f"slack: {code}")


BACKFILL_DAYS_DEFAULT = 30
PER_CHANNEL_LIMIT = 200


class SlackSource(Source):
    """Cron-driven Slack ingest. See docs/superpowers/specs/2026-06-05-slack-source-design.md."""

    name: ClassVar[str] = "slack"
    mcp_server: ClassVar[str] = "slack"

    def cursor_from_timestamp(self, ts: int) -> SlackCursor:
        """Per spec §4.4: returns empty SlackCursor; fetch() seeds each channel.

        We can't enumerate channels here (that requires an async MCP call and
        this method is sync per the Source ABC). The fetch loop treats
        cursor.lookup(channel_id) is None as "seed at backfill window OR
        caller-provided since_unix"; the worker plumbs since_unix through
        IngestContext (Task 10).
        """
        return SlackCursor()

    def normalize_body(self, raw: str) -> tuple[str, bool]:
        return _truncate_body(raw)

    async def resolve_actor(self, raw_actor: str) -> str | None:
        cache = _SlackUserCache(db=self.ctx.db, clock=self.ctx.clock)
        cached = cache.get(raw_actor)
        if cached is not None:
            return cached.email or cached.name
        try:
            resp = await self.ctx.mcp.call(
                UsersInfoRequest(user=raw_actor),
                UsersInfoResponse,
            )
        except Exception:
            return None
        cache.upsert(resp.user, fetched_at=self.ctx.clock.now_unix())
        return resp.user.email or resp.user.name

    async def fetch(self, cursor: Cursor | None) -> AsyncIterator[Batch]:
        slack_cursor = self._load_or_init_cursor(cursor)
        cache = _SlackUserCache(db=self.ctx.db, clock=self.ctx.clock)
        own_user_id = await self._resolve_own_user_id()

        try:
            list_resp = await self.ctx.mcp.call(
                ConversationsListRequest(),
                ConversationsListResponse,
            )
        except SlackError as exc:
            raise map_slack_error(exc.code) from exc
        for channel in list_resp.channels:
            if channel.is_archived or not channel.is_member:
                continue
            ch_cursor = slack_cursor.lookup(channel.id)
            if ch_cursor is None:
                seed_ts = (
                    self.ctx.since_unix
                    if self.ctx.since_unix is not None
                    else self.ctx.clock.now_unix() - BACKFILL_DAYS_DEFAULT * 86400
                )
                ch_cursor = ChannelCursor(
                    channel_id=channel.id,
                    channel_name=channel.name,
                    last_seen_ts=str(seed_ts),
                )

            try:
                history = await self.ctx.mcp.call(
                    ConversationsHistoryRequest(
                        channel=channel.id,
                        oldest=ch_cursor.last_seen_ts,
                        limit=PER_CHANNEL_LIMIT,
                    ),
                    ConversationsHistoryResponse,
                )
            except SlackError as exc:
                if exc.code in _SKIPPABLE_PER_CHANNEL_ERRORS:
                    self.ctx.logger.warning(
                        "slack_channel_skipped",
                        channel_id=channel.id,
                        channel_name=channel.name,
                        code=exc.code,
                    )
                    continue
                raise map_slack_error(exc.code) from exc

            events: list[NormalizedEvent] = []
            for msg in history.messages:
                events.append(
                    _normalize_message(
                        msg=msg,
                        channel=channel,
                        cache=cache,
                        own_user_id=own_user_id,
                        source_name="slack",
                    )
                )
                if msg.thread_ts and _thread_eligible(
                    msg,
                    replies=None,
                    own_user_id=own_user_id,
                ):
                    try:
                        replies = await self.ctx.mcp.call(
                            ConversationsRepliesRequest(channel=channel.id, ts=msg.thread_ts),
                            ConversationsRepliesResponse,
                        )
                    except SlackError as exc:
                        if exc.code in _SKIPPABLE_PER_CHANNEL_ERRORS:
                            self.ctx.logger.warning(
                                "slack_replies_skipped",
                                channel_id=channel.id,
                                ts=msg.thread_ts,
                                code=exc.code,
                            )
                            continue  # skip this thread, keep top-level event
                        raise map_slack_error(exc.code) from exc
                    if not _thread_eligible(
                        msg,
                        replies=replies.messages,
                        own_user_id=own_user_id,
                    ):
                        continue
                    for reply in replies.messages:
                        if reply.ts == msg.thread_ts:
                            continue
                        events.append(
                            _normalize_message(
                                msg=reply,
                                channel=channel,
                                cache=cache,
                                own_user_id=own_user_id,
                                source_name="slack",
                            )
                        )

            new_last_seen = (
                max(m.ts for m in history.messages) if history.messages else ch_cursor.last_seen_ts
            )
            updated_ch = ChannelCursor(
                channel_id=channel.id,
                channel_name=channel.name,
                last_seen_ts=new_last_seen,
            )
            slack_cursor = slack_cursor.with_updated(updated_ch)

            yield Batch(events=events, next_cursor=slack_cursor, status="ok")

    def _load_or_init_cursor(self, cursor: Cursor | None) -> SlackCursor:
        """Phase 1 scaffold passes None; we read our own row.

        When ctx.since_unix is set, discard persisted state — `--since`
        replaces it for this run. See spec §4.4.
        """
        if self.ctx.since_unix is not None:
            return SlackCursor()
        if isinstance(cursor, SlackCursor):
            return cursor
        with self.ctx.db.open() as conn:
            row = conn.execute(
                "SELECT cursor FROM ingest_cursors WHERE source = 'slack'"
            ).fetchone()
        if row is None or not row["cursor"]:
            return SlackCursor()
        return SlackCursor.model_validate_json(row["cursor"])

    async def _resolve_own_user_id(self) -> str:
        try:
            resp = await self.ctx.mcp.call(AuthTestRequest(), AuthTestResponse)
        except SlackError as exc:
            raise map_slack_error(exc.code) from exc
        return resp.user_id
