"""Test helpers for Slack source tests. Not for use outside tests/ingest/sources/."""

from __future__ import annotations

from tests.ingest.fakes import ScriptedReply
from work_assistant.ingest.sources.slack import (
    AuthTestResponse,
    ConversationsHistoryResponse,
    ConversationsListResponse,
    ConversationsRepliesResponse,
    UsersInfoResponse,
)


def slack_script(
    *,
    own_user_id: str = "U_OWN",
    team_id: str = "T_TEAM",
    list_channels: list[ConversationsListResponse] | None = None,
    histories: dict[str, list[ConversationsHistoryResponse]] | None = None,
    replies: dict[str, list[ConversationsRepliesResponse]] | None = None,
    users: dict[str, list[UsersInfoResponse]] | None = None,
) -> dict[str, list[ScriptedReply]]:
    """Build a FakeMCPClient script for typical Slack flows.

    Keys map to MCPRequest subclass __name__. Values are scripted replies
    consumed in order. Use FakeMCPClient(script=slack_script(...)) directly.
    """
    script: dict[str, list[ScriptedReply]] = {}
    script["AuthTestRequest"] = [
        ScriptedReply(response=AuthTestResponse(user_id=own_user_id, team_id=team_id)),
    ]
    script["ConversationsListRequest"] = [ScriptedReply(response=r) for r in (list_channels or [])]
    if histories:
        script["ConversationsHistoryRequest"] = [
            ScriptedReply(response=r)
            for _chan_id, replies_list in histories.items()
            for r in replies_list
        ]
    if replies:
        script["ConversationsRepliesRequest"] = [
            ScriptedReply(response=r)
            for _thread_ts, replies_list in replies.items()
            for r in replies_list
        ]
    if users:
        script["UsersInfoRequest"] = [
            ScriptedReply(response=r) for _u_id, replies_list in users.items() for r in replies_list
        ]
    return script
