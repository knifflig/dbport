"""Input declaration and ingest tracking value objects."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class InputDeclaration(BaseModel):
    """Declarative specification of a warehouse table to load as input."""

    model_config = ConfigDict(frozen=True)

    table_address: str
    """Fully-qualified `<agency>.<dataset_id>` warehouse address."""

    filters: dict[str, str] | None = None
    """Optional equality filters applied at scan time (pushed to Iceberg)."""

    version: str | None = None
    """Dataset version to load (e.g. '2026-03-09').

    None (default) → load the latest published version, resolved from the
    table's ``dbport.metadata_json`` property.  Falls back to the current
    Iceberg snapshot for tables without DBPort metadata.

    Set explicitly to load a specific historical version::

        client.load("wifor.emp__regional_trends", version="2025-01-01")
    """


class IngestRecord(BaseModel):
    """Persisted record of a completed input ingest (written to dbport.lock)."""

    model_config = ConfigDict(frozen=True)

    table_address: str
    last_snapshot_id: int | None = None
    last_snapshot_timestamp_ms: int | None = None
    rows_loaded: int | None = None
    filters: dict[str, str] | None = None
    version: str | None = None
    """Dataset version that was loaded (resolved from DBPort metadata or explicit)."""
