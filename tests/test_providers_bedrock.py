"""Tests for work_assistant.providers.bedrock."""

from __future__ import annotations

from typing import Any

import pytest

from work_assistant.providers import bedrock


class _FakeClient:
    def __init__(self, response: dict[str, Any] | None = None,
                 raise_exc: Exception | None = None) -> None:
        self._response = response
        self._raise = raise_exc
        self.last_kwargs: dict[str, Any] | None = None

    def converse(self, **kwargs: Any) -> dict[str, Any]:
        self.last_kwargs = kwargs
        if self._raise is not None:
            raise self._raise
        assert self._response is not None
        return self._response


def test_smoke_test_returns_ok_on_successful_call(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeClient(
        response={"output": {"message": {"content": [{"text": "ok"}]}},
                  "stopReason": "end_turn"}
    )
    monkeypatch.setattr(bedrock, "_make_client", lambda region, profile: fake)

    ok, detail = bedrock.smoke_test(
        region="eu-west-1",
        profile="work-assistant",
        model_id="arn:aws:bedrock:eu-west-1:111:inference-profile/x",
    )

    assert ok is True
    assert "ok" in detail.lower()
    assert fake.last_kwargs is not None
    assert fake.last_kwargs["modelId"] == "arn:aws:bedrock:eu-west-1:111:inference-profile/x"
    assert fake.last_kwargs["inferenceConfig"]["maxTokens"] == 5


def test_smoke_test_returns_failure_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeClient(raise_exc=RuntimeError("AccessDeniedException: model not enabled"))
    monkeypatch.setattr(bedrock, "_make_client", lambda region, profile: fake)
    ok, detail = bedrock.smoke_test(
        region="eu-west-1",
        profile="work-assistant",
        model_id="arn:aws:bedrock:eu-west-1:111:inference-profile/x",
    )
    assert ok is False
    assert "AccessDenied" in detail or "model not enabled" in detail
