"""Exception hierarchy for the ingest worker.

The worker catches anything that bubbles out of a `Source` and classifies it
into a transient or permanent bucket via `classify()`. The bucket drives the
worker's exit code per spec §6.2.
"""

from __future__ import annotations

from typing import Literal

ErrorBucket = Literal["transient", "permanent"]


class IngestError(Exception):
    """Base class for ingest worker failures we classify."""


class TransientIngestError(IngestError):
    """Retryable next tick: network 5xx, rate-limit, busy db, MCP timeout, stall."""


class PermanentIngestError(IngestError):
    """Not retryable without operator action: auth revoked, account disabled,
    schema-incompatible payload."""


class SourceStallError(TransientIngestError):
    """Two consecutive zero-insert batches with non-empty input. Almost always
    a pagination or dedup-key bug. Surfaces as exit 1."""


class MCPTimeoutError(TransientIngestError):
    """An MCP tool call exceeded `MCP_CALL_TIMEOUT_S` seconds."""


def classify(exc: BaseException) -> ErrorBucket:
    """Map an exception to its exit-code bucket. Unknown errors are transient."""
    if isinstance(exc, PermanentIngestError):
        return "permanent"
    return "transient"
