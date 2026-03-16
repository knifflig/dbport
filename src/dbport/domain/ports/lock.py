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

    def read_version(self) -> str | None:
        """Return the configured default publish version, or None."""
        ...

    def write_version(self, version: str) -> None:
        """Set the default publish version for this model."""
        ...

    # ------------------------------------------------------------------
    # Project-level operations (not model-scoped)
    # ------------------------------------------------------------------

    def read_default_model_key(self) -> str | None:
        """Return the default_model key from the lock file, or None."""
        ...

    def write_default_model_key(self, model_key: str) -> None:
        """Set the default_model key in the lock file."""
        ...

    def read_models_folder(self) -> str:
        """Return the models_folder from the lock file, defaulting to 'models'."""
        ...

    def write_models_folder(self, folder: str) -> None:
        """Set the models_folder key in the lock file."""
        ...

    def list_model_keys(self) -> list[str]:
        """Return all model keys present in the lock file."""
        ...

    def read_model_data(self, model_key: str) -> dict | None:
        """Return raw model data dict for *model_key*, or None if absent."""
        ...

    def register_model(self) -> None:
        """Ensure this model's header (agency, dataset_id, paths) exists in the lock file."""
        ...
