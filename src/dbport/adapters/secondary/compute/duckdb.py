"""DuckDBComputeAdapter — implements ICompute using duckdb."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Standard schemas created on every new DuckDB database.
_INIT_SCHEMAS = ("inputs", "staging", "outputs")

# Extensions installed on every new DuckDB connection.
# Order matters: avro is a transitive dependency of iceberg.
_REQUIRED_EXTENSIONS = ("httpfs", "avro", "iceberg")

_EXTENSIONS_REPO = "https://extensions.duckdb.org"


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

    def ensure_extensions(self) -> None:
        """Install and load required DuckDB extensions.

        Called explicitly when warehouse operations are needed (publish).
        Strategy: LOAD (fast) → DuckDB INSTALL via HTTPS → Python download fallback.
        """
        con = self._get_con()
        for ext in _REQUIRED_EXTENSIONS:
            try:
                con.execute(f"LOAD {ext}")
                continue
            except Exception:
                pass
            # Try DuckDB INSTALL via HTTPS
            try:
                con.execute(
                    f"SET custom_extension_repository = '{_EXTENSIONS_REPO}'"
                )
                con.execute(f"INSTALL {ext}")
                con.execute(f"LOAD {ext}")
                continue
            except Exception:
                pass
            # Fallback: download via Python urllib and place in extension dir
            try:
                self._download_extension(ext)
                con.execute(f"LOAD {ext}")
            except Exception as exc:
                raise RuntimeError(
                    f"Required DuckDB extension '{ext}' could not be loaded: {exc}"
                ) from exc

    @staticmethod
    def _download_extension(ext: str) -> None:
        """Download a DuckDB extension via Python urllib (HTTPS)."""
        import gzip
        import urllib.request

        import duckdb

        version = duckdb.__version__
        platform = "linux_amd64"
        url = f"{_EXTENSIONS_REPO}/v{version}/{platform}/{ext}.duckdb_extension.gz"
        ext_dir = Path.home() / ".duckdb" / "extensions" / f"v{version}" / platform
        ext_dir.mkdir(parents=True, exist_ok=True)
        dest = ext_dir / f"{ext}.duckdb_extension"
        if dest.exists():
            return
        logger.info("Downloading DuckDB extension %s from %s", ext, url)
        resp = urllib.request.urlopen(url, timeout=60)  # noqa: S310
        dest.write_bytes(gzip.decompress(resp.read()))

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
                logger.debug("Error closing DuckDB connection", exc_info=True)
            self._con = None
