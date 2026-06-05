"""Source registry. Per-source plans add entries to `SOURCES` in their own
modules; this module owns the dict and the `select_sources()` helper."""

from __future__ import annotations

from work_assistant.ingest.source import Source


class UnknownSourceError(ValueError):
    """Raised when `--source` names a source not present in the registry."""


SOURCES: dict[str, type[Source]] = {}


def select_sources(
    *,
    registry: dict[str, type[Source]],
    requested: list[str] | None,
) -> dict[str, type[Source]]:
    """Pick the subset of `registry` matching `requested`.

    - `requested=None` -> return the full registry.
    - `requested=[]` -> return an empty dict (caller decides whether that's an error).
    - Any name not in `registry` -> raise `UnknownSourceError`.
    """
    if requested is None:
        return dict(registry)
    unknown = [n for n in requested if n not in registry]
    if unknown:
        raise UnknownSourceError(
            f"unknown source(s): {', '.join(unknown)}; available: {sorted(registry)}"
        )
    return {n: registry[n] for n in requested}
