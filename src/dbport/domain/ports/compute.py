"""ICompute port — abstract interface for local SQL compute (DuckDB)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa


@runtime_checkable
class ICompute(Protocol):
    """Outbound port for local DuckDB operations.

    Secondary adapter: adapters/secondary/compute/duckdb.py
    """

    def execute(self, sql: str, parameters: list[object] | None = None) -> duckdb.DuckDBPyRelation:
        """Execute a SQL statement, returning a relation/result."""
        ...

    def execute_file(self, path: str) -> None:
        """Read and execute all statements from a .sql file."""
        ...

    def relation_exists(self, schema: str, table: str) -> bool:
        """Return True if schema.table exists in DuckDB."""
        ...

    def to_arrow_batches(self, sql: str, batch_size: int = 10_000) -> pa.RecordBatchReader:
        """Stream the result of a SELECT as PyArrow RecordBatch chunks."""
        ...

    def register_arrow(self, view_name: str, arrow_object: pa.Table | pa.RecordBatchReader) -> None:
        """Register an Arrow object (Table, RecordBatchReader, …) as a DuckDB view."""
        ...

    def unregister_arrow(self, view_name: str) -> None:
        """Unregister a previously registered Arrow view."""
        ...

    def ensure_extensions(self) -> None:
        """Install and load required DuckDB extensions for warehouse operations."""
        ...

    def close(self) -> None:
        """Release the DuckDB connection."""
        ...
