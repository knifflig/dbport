"""DuckDBComputeAdapter — implements ICompute using duckdb."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Standard schemas created on every new DuckDB database.
_INIT_SCHEMAS = ("inputs", "staging", "outputs")


class DuckDBComputeAdapter:
    """File-backed DuckDB connection with schema initialisation.

    Implements: ICompute
    Dependencies: duckdb
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._con: Any = None  # duckdb.DuckDBPyConnection, lazy-loaded

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_con(self) -> Any:
        if self._con is None:
            try:
                import duckdb
            except ImportError as exc:
                raise RuntimeError(
                    "duckdb is required. Install it: pip install duckdb"
                ) from exc
            self._con = duckdb.connect(str(self._path))
            for schema in _INIT_SCHEMAS:
                self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            logger.debug("DuckDB connected: %s", self._path)
        return self._con

    # ------------------------------------------------------------------
    # ICompute
    # ------------------------------------------------------------------

    def execute(self, sql: str, parameters: list[Any] | None = None) -> Any:
        """Execute a SQL statement. Returns the DuckDB relation/cursor."""
        con = self._get_con()
        if parameters:
            return con.execute(sql, parameters)
        return con.execute(sql)

    def execute_file(self, path: str) -> None:
        """Read and execute all statements from a .sql file."""
        sql = Path(path).read_text(encoding="utf-8")
        self._get_con().execute(sql)

    def relation_exists(self, schema: str, table: str) -> bool:
        """Return True if schema.table exists in DuckDB."""
        result = self._get_con().execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
        return bool(result and result[0] > 0)

    def to_arrow_batches(self, sql: str, batch_size: int = 10_000) -> Any:
        """Stream the result of a SELECT as a PyArrow RecordBatchReader."""
        return self._get_con().execute(sql).to_arrow_reader(batch_size)

    def register_arrow(self, view_name: str, arrow_object: Any) -> None:
        """Register an Arrow object (Table, RecordBatchReader, …) as a DuckDB view."""
        self._get_con().register(view_name, arrow_object)

    def unregister_arrow(self, view_name: str) -> None:
        """Unregister a previously registered Arrow view."""
        self._get_con().unregister(view_name)

    def close(self) -> None:
        if self._con is not None:
            try:
                self._con.close()
            except Exception:
                pass
            self._con = None
