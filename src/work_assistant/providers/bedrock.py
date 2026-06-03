"""Bedrock connectivity helpers used by `wa doctor`.

We deliberately avoid importing boto3 at module load — `_make_client` is the
single seam tests can monkeypatch.
"""

from __future__ import annotations

from typing import Any


def _make_client(region: str, profile: str) -> Any:
    import boto3

    session = boto3.Session(profile_name=profile, region_name=region)
    return session.client("bedrock-runtime")


def smoke_test(region: str, profile: str, model_id: str) -> tuple[bool, str]:
    """Issue a 5-token Converse call to verify Bedrock auth + model access.

    Returns `(ok, detail)`. On success, `detail` is the model's text output.
    On failure, `detail` is the exception class + message (one line).
    """
    try:
        client = _make_client(region, profile)
        resp = client.converse(
            modelId=model_id,
            messages=[{"role": "user", "content": [{"text": "ping"}]}],
            inferenceConfig={"maxTokens": 5, "temperature": 0.0},
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"

    try:
        text = resp["output"]["message"]["content"][0]["text"]
    except (KeyError, IndexError, TypeError):
        return False, f"unexpected response shape: {resp!r}"
    return True, f"response={text!r}"
