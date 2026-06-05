"""Tests for work_assistant.ingest.errors."""

from __future__ import annotations

from work_assistant.ingest import errors


def test_stall_is_transient() -> None:
    err = errors.SourceStallError("two zero-insert batches")
    assert isinstance(err, errors.TransientIngestError)
    assert isinstance(err, errors.IngestError)


def test_permanent_distinct_from_transient() -> None:
    perm = errors.PermanentIngestError("auth revoked")
    assert isinstance(perm, errors.IngestError)
    assert not isinstance(perm, errors.TransientIngestError)


def test_mcp_timeout_is_transient() -> None:
    err = errors.MCPTimeoutError("call timed out after 60s")
    assert isinstance(err, errors.TransientIngestError)


def test_classify_unknown_exception_is_transient() -> None:
    bucket = errors.classify(RuntimeError("oops"))
    assert bucket == "transient"


def test_classify_permanent() -> None:
    bucket = errors.classify(errors.PermanentIngestError("nope"))
    assert bucket == "permanent"


def test_classify_stall_is_transient() -> None:
    bucket = errors.classify(errors.SourceStallError("stall"))
    assert bucket == "transient"
