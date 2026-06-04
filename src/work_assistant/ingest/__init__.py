"""Ingest worker scaffold.

This package owns the worker process (`wa ingest`), the `Source` ABC, the
`MCPClient` adapter, `IngestContext`, lock model, and exit-code logic. Per-
source implementations live under their own modules and are wired into
`registry.SOURCES`.
"""
