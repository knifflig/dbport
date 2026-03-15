"""ICatalog port — abstract interface for Iceberg warehouse operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ..entities.input import InputDeclaration
    from ..entities.version import DatasetVersion, VersionRecord


@runtime_checkable
class ICatalog(Protocol):
    """Outbound port for all Iceberg catalog interactions.

    Secondary adapter: adapters/secondary/catalog/iceberg.py
    """

    def table_exists(self, table_address: str) -> bool:
        """Return True if the table exists in the warehouse."""
        ...

    def load_arrow_schema(self, table_address: str) -> Any:
        """Return the PyArrow schema of an existing warehouse table."""
        ...

    def resolve_input_snapshot(
        self,
        table_address: str,
        version: str | None,
    ) -> tuple[str | None, int | None]:
        """Resolve a version label to a (resolved_version, snapshot_id) pair.

        Reads ``dbport.metadata_json`` from Iceberg table properties (no data
        scan).  Returns the resolved version string and its corresponding
        Iceberg snapshot_id.

        - *version* is None  → return the latest version from metadata
          (``last_updated_data_at``).
        - *version* is set   → find the matching entry in the ``versions``
          list and return its snapshot_id.
        - Table has no DBPort metadata → return (None, None) so the caller falls
          back to ``current_snapshot()``.
        """
        ...

    def ingest_into_compute(
        self,
        declaration: InputDeclaration,
        compute: Any,
        snapshot_id: int | None = None,
    ) -> int:
        """Load a warehouse table into DuckDB.

        *snapshot_id* pins the read to a specific Iceberg snapshot; used by
        the Arrow fallback path.  The DuckDB extension path reads the current
        snapshot but still records *snapshot_id* as the intended version.

        Returns the number of rows loaded.
        """
        ...

    def current_snapshot(self, table_address: str) -> tuple[int | None, int | None]:
        """Return (snapshot_id, snapshot_timestamp_ms) for a table's current snapshot."""
        ...

    def write_versioned(
        self,
        table_address: str,
        version: DatasetVersion,
        compute: Any,
        overwrite: bool = False,
    ) -> VersionRecord:
        """Write DuckDB output table to the warehouse.

        Primary path: DuckDB iceberg extension (data stays in DuckDB).
        Fallback: streaming Arrow batches through pyiceberg when the catalog
        does not support the multi-table commit endpoint.

        When overwrite=True the completed checkpoint is ignored and rows are
        re-written unconditionally (used for publish mode="refresh").
        """
        ...

    def get_table_property(
        self,
        table_address: str,
        key: str,
    ) -> str | None:
        """Return a single table property value, or None if not set."""
        ...

    def update_table_properties(
        self,
        table_address: str,
        properties: dict[str, str],
    ) -> None:
        """Merge key/value properties into the Iceberg table metadata."""
        ...
