"""DBPort — single public entrypoint for all warehouse interactions.

Usage:
    from dbport import DBPort

    with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
        port.schema("sql/create_output.sql")
        port.columns.geo.meta(codelist_id="NUTS2024")
        port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
        port.execute("sql/final_output.sql")
        port.publish(version="2026-03-09")
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ...domain.entities.dataset import Dataset
from ...domain.entities.input import InputDeclaration
from ...domain.entities.version import DatasetVersion
from ...infrastructure.credentials import WarehouseCreds
from ...infrastructure.logging import setup_logging
from .columns import ColumnRegistry

if TYPE_CHECKING:
    from ...domain.ports.catalog import ICatalog
    from ...domain.ports.compute import ICompute
    from ...domain.ports.lock import ILockStore
    from ...domain.ports.metadata import IMetadataStore

logger = logging.getLogger(__name__)


def _find_repo_root(start: Path) -> Path:
    """Walk up from *start* until a directory containing pyproject.toml is found.

    Falls back to *start* if no pyproject.toml is found before the filesystem root.
    """
    current = start.resolve()
    while True:
        if (current / "pyproject.toml").exists():
            return current
        parent = current.parent
        if parent == current:
            return start.resolve()
        current = parent


def _caller_dir(stack_depth: int = 2) -> Path:
    """Return the directory of the file that called DBPort().

    stack_depth=2: frame 0 = _caller_dir, frame 1 = __init__, frame 2 = user code.
    """
    import inspect
    frame = inspect.stack()[stack_depth]
    return Path(frame.filename).resolve().parent


class DBPort:
    """Single entrypoint for DBPort data product models.

    A single dbport.lock file is maintained at the repository root (next to
    pyproject.toml). Multiple models in the same repo share one lock file,
    each stored under its own namespaced section keyed by agency.dataset_id.

    Credentials are resolved from explicit kwargs first, then environment
    variables (ICEBERG_REST_URI, ICEBERG_CATALOG_TOKEN, ICEBERG_WAREHOUSE, etc.).
    """

    def __init__(
        self,
        agency: str,
        dataset_id: str,
        *,
        # Credentials — all optional, resolved from env vars if not supplied
        catalog_uri: str | None = None,
        catalog_token: str | None = None,
        warehouse: str | None = None,
        s3_endpoint: str | None = None,
        s3_access_key: str | None = None,
        s3_secret_key: str | None = None,
        # Local paths — auto-discovered when not provided
        duckdb_path: str | None = None,
        lock_path: str | None = None,
        model_root: str | None = None,
    ) -> None:
        setup_logging()

        # ------------------------------------------------------------------
        # Resolve caller's directory, repo root, and model root
        # ------------------------------------------------------------------
        if model_root is not None:
            caller_dir = Path(model_root).resolve()
        else:
            caller_dir = _caller_dir(stack_depth=2)

        if lock_path is None:
            repo_root = _find_repo_root(caller_dir)
            _lock_path = repo_root / "dbport.lock"
        else:
            _lock_path = Path(lock_path)
            repo_root = _lock_path.parent

        # model_root: relative path from repo root to the model's directory
        try:
            _model_root_rel = str(caller_dir.relative_to(repo_root))
        except ValueError:
            _model_root_rel = str(caller_dir)

        _model_root_abs = str(caller_dir)

        # duckdb lives in the model's own data/ subdirectory by default
        if duckdb_path is None:
            _duckdb_path = caller_dir / "data" / f"{dataset_id}.duckdb"
        else:
            _duckdb_path = Path(duckdb_path)

        _duckdb_path.parent.mkdir(parents=True, exist_ok=True)

        # ------------------------------------------------------------------
        # Credentials
        # ------------------------------------------------------------------
        creds_overrides: dict[str, Any] = {}
        if catalog_uri:
            creds_overrides["catalog_uri"] = catalog_uri
        if catalog_token:
            creds_overrides["catalog_token"] = catalog_token
        if warehouse:
            creds_overrides["warehouse"] = warehouse
        if s3_endpoint:
            creds_overrides["s3_endpoint"] = s3_endpoint
        if s3_access_key:
            creds_overrides["s3_access_key"] = s3_access_key
        if s3_secret_key:
            creds_overrides["s3_secret_key"] = s3_secret_key

        self._creds = WarehouseCreds(**creds_overrides)

        # ------------------------------------------------------------------
        # Dataset identity
        # ------------------------------------------------------------------
        self._dataset = Dataset(
            agency=agency,
            dataset_id=dataset_id,
            duckdb_path=str(_duckdb_path),
            lock_path=str(_lock_path),
            model_root=_model_root_abs,
        )

        # ------------------------------------------------------------------
        # Wire secondary adapters (lazy imports to keep startup fast)
        # ------------------------------------------------------------------
        # duckdb_path in the lock is relative to the repo root (same as model_root)
        try:
            _duckdb_path_rel = str(_duckdb_path.relative_to(repo_root))
        except ValueError:
            _duckdb_path_rel = str(_duckdb_path)

        model_key = self._dataset.table_address  # "agency.dataset_id"
        self._lock: ILockStore = self._make_lock(
            _lock_path,
            model_key=model_key,
            model_root=_model_root_rel,
            duckdb_path=_duckdb_path_rel,
        )
        self._compute: ICompute = self._make_compute(_duckdb_path)
        self._catalog: ICatalog = self._make_catalog(self._creds)
        self._metadata: IMetadataStore = self._make_metadata()

        # Public column registry
        self.columns = ColumnRegistry(self._lock)

        # Auto-detect schema from warehouse if table exists
        self._auto_detect_schema()

        # Sync local DuckDB state with lock + warehouse
        self._sync_local_state()

        # Update last_fetched_at on every run (fire-and-forget)
        self._update_last_fetched()

        logger.debug(
            "Ready: %s (model_root=%s, duckdb=%s)",
            self._dataset.table_address,
            _model_root_rel,
            _duckdb_path,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def schema(self, ddl_or_path: str) -> None:
        """Declare the output table schema from a DDL string or .sql file path."""
        from ...application.services.schema import DefineSchemaService

        svc = DefineSchemaService(self._compute, self._lock)
        svc.execute(ddl_or_path, base_dir=self._dataset.model_root)
        self.columns._refresh()

    def load(
        self,
        table_address: str,
        *,
        filters: dict[str, str] | None = None,
        version: str | None = None,
    ) -> None:
        """Load an Iceberg table into DuckDB.

        For tables published by DBPort, the load is automatically
        pinned to a specific Iceberg snapshot so no TOCTOU race can occur and
        the ingest record in dbport.lock records the exact version loaded.

        *version* (default None) → load the latest published dataset version,
        resolved from the table's ``dbport.metadata_json`` property without a
        data scan.  Pass an explicit version string to pin to a historical
        release::

            port.load("wifor.emp__regional_trends", version="2025-01-01")

        For tables without DBPort metadata (e.g. Eurostat inputs), version is
        ignored and the current Iceberg snapshot is used as before.
        """
        from ...application.services.ingest import IngestService

        svc = IngestService(self._catalog, self._compute, self._lock)
        declaration = InputDeclaration(
            table_address=table_address, filters=filters, version=version
        )
        svc.execute(declaration)

    def execute(self, sql_or_path: str) -> None:
        """Run a SQL string or .sql file in DuckDB."""
        from ...application.services.transform import TransformService

        svc = TransformService(self._compute)
        svc.execute(sql_or_path, base_dir=self._dataset.model_root)

    def publish(
        self,
        *,
        version: str,
        params: dict[str, str] | None = None,
        mode: str | None = None,
    ) -> None:
        """Write the output table to Iceberg with full metadata and codelists.

        mode=None (default): normal idempotent publish.
        mode="dry": validate schemas only; no data is written.
        mode="refresh": overwrite an existing version unconditionally.
        """
        from ...application.services.publish import PublishService

        svc = PublishService(
            self._dataset,
            self._catalog,
            self._compute,
            self._lock,
            self._metadata,
        )
        svc.execute(DatasetVersion(version=version, params=params, mode=mode))

    def close(self) -> None:
        """Release resources (DuckDB connection)."""
        try:
            self._compute.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> DBPort:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Adapter wiring (private)
    # ------------------------------------------------------------------

    def _make_lock(
        self,
        path: Path,
        model_key: str,
        model_root: str,
        duckdb_path: str,
    ) -> ILockStore:
        from ...adapters.secondary.lock.toml import TomlLockAdapter
        return TomlLockAdapter(path, model_key=model_key, model_root=model_root, duckdb_path=duckdb_path)

    def _make_compute(self, path: Path) -> ICompute:
        from ...adapters.secondary.compute.duckdb import DuckDBComputeAdapter
        return DuckDBComputeAdapter(path)

    def _make_catalog(self, creds: WarehouseCreds) -> ICatalog:
        from ...adapters.secondary.catalog.iceberg import IcebergCatalogAdapter
        return IcebergCatalogAdapter(creds)

    def _make_metadata(self) -> IMetadataStore:
        from ...adapters.secondary.metadata.materialize import MetadataAdapter
        return MetadataAdapter()

    def _auto_detect_schema(self) -> None:
        """Auto-detect output schema from warehouse table if it exists."""
        from ...application.services.auto_schema import AutoSchemaService

        try:
            svc = AutoSchemaService(self._catalog, self._compute, self._lock)
            schema = svc.execute(self._dataset.table_address)
            if schema is not None:
                self.columns._refresh()
                logger.debug(
                    "Auto-detected schema from warehouse for %s",
                    self._dataset.table_address,
                )
        except Exception as exc:
            logger.debug("Auto-schema detection skipped: %s", exc)

    def _sync_local_state(self) -> None:
        """Sync DuckDB with lock file: output table + inputs."""
        from ...application.services.sync import SyncService

        try:
            svc = SyncService(self._catalog, self._compute, self._lock)
            svc.execute(self._dataset.table_address)
        except Exception as exc:
            logger.debug("Local sync skipped: %s", exc)

    def _update_last_fetched(self) -> None:
        from ...application.services.fetch import FetchService

        svc = FetchService(self._dataset, self._catalog)
        svc.execute()
