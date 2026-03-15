"""ILockStore port — abstract interface for dbport.lock persistence."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ..entities.codelist import CodelistEntry
    from ..entities.input import IngestRecord
    from ..entities.schema import DatasetSchema
    from ..entities.version import VersionRecord


@runtime_checkable
class ILockStore(Protocol):
    """Outbound port for reading and writing dbport.lock.

    The lock file is TOML, contains no secrets, and is safe to commit.
    Secondary adapter: adapters/secondary/lock/toml.py
    """

    def read_schema(self) -> DatasetSchema | None:
        """Return the persisted output schema, or None if not yet defined."""
        ...

    def write_schema(self, schema: DatasetSchema) -> None:
        """Persist the output schema (DDL + parsed columns)."""
        ...

    def read_codelist_entries(self) -> dict[str, CodelistEntry]:
        """Return all persisted codelist entries keyed by column_name."""
        ...

    def write_codelist_entry(self, entry: CodelistEntry) -> None:
        """Upsert a single codelist entry (matched by column_name)."""
        ...

    def read_ingest_records(self) -> list[IngestRecord]:
        """Return all persisted input ingest records."""
        ...

    def write_ingest_record(self, record: IngestRecord) -> None:
        """Upsert an ingest record (matched by table_address)."""
        ...

    def read_versions(self) -> list[VersionRecord]:
        """Return all persisted version records, oldest first."""
        ...

    def append_version(self, record: VersionRecord) -> None:
        """Append a new version record (or update existing by version string)."""
        ...

    def read_run_hook(self) -> str | None:
        """Return the configured run hook path, or None if not set."""
        ...

    def write_run_hook(self, hook: str) -> None:
        """Persist the run hook path for this model."""
        ...
