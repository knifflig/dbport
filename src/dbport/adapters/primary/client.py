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
from typing import TYPE_CHECKING

from ...domain.entities.dataset import Dataset
from ...domain.entities.input import IngestRecord, InputDeclaration
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
        load_inputs_on_init: bool = True,
        config_only: bool = False,
    ) -> None:
        """Initialize a DBPort instance.

        In **full mode** (default), initialization performs these phases:

        1. **Path resolution** — discovers ``model_root`` (from the calling
           script's directory or the explicit *model_root* kwarg), repo root
           (walks up to ``pyproject.toml``), lock path, and DuckDB path.
        2. **Credential resolution** — merges explicit kwargs with environment
           variables via ``WarehouseCreds``.
        3. **Adapter wiring** — creates lock, compute (DuckDB), catalog
           (Iceberg REST), and metadata adapters.
        4. **State sync** — auto-detects schema from warehouse, syncs the
           output table in DuckDB, updates ``last_fetched_at``, and (when
           *load_inputs_on_init* is True) reloads previously configured
           inputs.  All sync errors are logged but never fail initialization.

        In **config_only** mode, only the lock adapter and column registry
        are created.  No credentials, DuckDB, or catalog connection are
        required.  Data methods (``schema``, ``load``, ``execute``, ``run``,
        ``configure_input``, ``publish``) raise ``RuntimeError`` if called.
        Column metadata methods (``.meta()``, ``.attach()``) work normally.
        """
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

        if not config_only:
            _duckdb_path.parent.mkdir(parents=True, exist_ok=True)

        # ------------------------------------------------------------------
        # Credentials (skipped in config_only mode)
        # ------------------------------------------------------------------
        if not config_only:
            creds_overrides: dict[str, str] = {}
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
        else:
            self._creds = None  # type: ignore[assignment]

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
        self._config_only = config_only
        self._lock: ILockStore = self._make_lock(
            _lock_path,
            model_key=model_key,
            model_root=_model_root_rel,
            duckdb_path=_duckdb_path_rel,
        )

        # Public column registry (only needs lock adapter)
        self.columns = ColumnRegistry(self._lock)

        if config_only:
            # Lightweight mode: skip DuckDB, catalog, metadata, and all sync
            self._compute = None  # type: ignore[assignment]
            self._catalog = None  # type: ignore[assignment]
            self._metadata = None  # type: ignore[assignment]
            return

        self._compute: ICompute = self._make_compute(_duckdb_path)
        self._catalog: ICatalog = self._make_catalog(self._creds)
        self._metadata: IMetadataStore = self._make_metadata()

        from ...infrastructure.progress import progress_phase

        with progress_phase("sync", title="Sync", icon="🔄"):
            # Auto-detect schema from warehouse if table exists
            self._auto_detect_schema()

            # Sync local DuckDB output state with lock + warehouse
            self._sync_output_state()

            # Update last_fetched_at on every run (fire-and-forget)
            self._update_last_fetched()

        if load_inputs_on_init:
            with progress_phase("load", title="Load", icon="📥"):
                self._load_inputs()

        logger.debug(
            "Ready: %s (model_root=%s, duckdb=%s)",
            self._dataset.table_address,
            _model_root_rel,
            _duckdb_path,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _require_full_mode(self, method: str) -> None:
        """Raise if called in config_only mode."""
        if self._config_only:
            raise RuntimeError(f"DBPort.{method}() requires full mode (config_only=False)")

    def schema(self, ddl_or_path: str) -> None:
        """Declare the output table schema from a DDL string or .sql file path."""
        self._require_full_mode("schema")
        from ...application.services.schema import DefineSchemaService

        svc = DefineSchemaService(self._compute, self._lock).with_catalog(
            self._catalog,
            self._dataset.table_address,
        )
        svc.execute(ddl_or_path, base_dir=self._dataset.model_root)
        self.columns._refresh()

    def load(
        self,
        table_address: str,
        *,
        filters: dict[str, str] | None = None,
        version: str | None = None,
    ) -> IngestRecord:
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
        self._require_full_mode("load")
        from ...application.services.ingest import IngestService

        svc = IngestService(self._catalog, self._compute, self._lock)
        declaration = InputDeclaration(
            table_address=table_address, filters=filters, version=version
        )
        return svc.execute(declaration)

    def configure_input(
        self,
        table_address: str,
        *,
        filters: dict[str, str] | None = None,
        version: str | None = None,
    ) -> IngestRecord:
        """Validate and persist an input declaration without loading it."""
        self._require_full_mode("configure_input")
        from ...application.services.ingest import IngestService

        svc = IngestService(self._catalog, self._compute, self._lock)
        declaration = InputDeclaration(
            table_address=table_address, filters=filters, version=version
        )
        return svc.configure(declaration)

    def execute(self, sql_or_path: str) -> None:
        """Run a SQL string or .sql file in DuckDB."""
        self._require_full_mode("execute")
        from ...application.services.transform import TransformService

        svc = TransformService(self._compute)
        svc.execute(sql_or_path, base_dir=self._dataset.model_root)

    def run(
        self,
        *,
        version: str | None = None,
        mode: str | None = None,
    ) -> None:
        """Execute the configured run hook, optionally publishing afterward.

        Reads the ``run_hook`` path from ``dbport.lock`` and dispatches by
        file extension (``.sql`` or ``.py``).  If *version* is provided,
        ``publish()`` is called after the hook completes.
        """
        self._require_full_mode("run")
        from ...application.services.run import RunService

        svc = RunService(self._compute, self._lock)
        svc.execute(self, version=version, mode=mode)

    @property
    def run_hook(self) -> str | None:
        """Return the configured run hook path, defaulting to main.py."""
        from ...application.services.run import resolve_run_hook

        return resolve_run_hook(self._lock, self._dataset.model_root)

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
        self._require_full_mode("publish")
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
        if self._config_only:
            return
        try:
            self._compute.close()
        except Exception:
            logger.debug("Error closing compute adapter", exc_info=True)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> DBPort:
        """Enter the context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Exit the context manager and release resources."""
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

        return TomlLockAdapter(
            path, model_key=model_key, model_root=model_root, duckdb_path=duckdb_path
        )

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
        """Auto-detect output schema from warehouse table if it exists.

        Skipped when a user-declared schema (``source='local'``) is already
        present in the lock file.  On success, the detected schema is
        persisted to the lock with ``source='warehouse'`` and the column
        registry is refreshed.  Errors are logged at debug level and never
        fail initialization.
        """
        from ...application.services.auto_schema import AutoSchemaService
        from ...infrastructure.progress import progress_callback

        cb = progress_callback.get(None)
        if cb:
            cb.started("Detecting schema from warehouse")

        try:
            svc = AutoSchemaService(self._catalog, self._compute, self._lock)
            schema = svc.execute(self._dataset.table_address)
            if schema is not None:
                self.columns._refresh()
                logger.debug(
                    "Auto-detected schema from warehouse for %s",
                    self._dataset.table_address,
                )
                if cb:
                    cb.finished("Schema detected from warehouse")
            else:
                if cb:
                    cb.finished("No existing warehouse table")
        except Exception as exc:
            if cb:
                cb.finished("Schema detection skipped")
            logger.debug("Auto-schema detection skipped: %s", exc)

    def _sync_output_state(self) -> None:
        """Create the output table in DuckDB from the lock file schema.

        If the output table already exists in DuckDB (e.g. from a prior
        run), DDL execution is skipped to preserve existing data.  Errors
        are logged at warning level and never fail initialization.
        """
        from ...application.services.sync import SyncService

        try:
            svc = SyncService(self._catalog, self._compute, self._lock)
            svc.sync_output_table(self._dataset.table_address)
        except Exception as exc:
            logger.warning("Local sync skipped: %s", exc)

    def _load_inputs(self) -> None:
        """Reload all inputs declared in the lock file into DuckDB.

        Each input is loaded independently; a failure on one input is
        logged at debug level and does not prevent other inputs from
        loading.  Only called when ``load_inputs_on_init=True``.
        """
        from ...application.services.sync import SyncService

        try:
            svc = SyncService(self._catalog, self._compute, self._lock)
            svc.sync_inputs()
        except Exception as exc:
            logger.debug("Input sync skipped: %s", exc)

    def _update_last_fetched(self) -> None:
        """Update ``dbport.last_fetched_at`` in the warehouse table properties.

        Fire-and-forget: errors are logged at debug level inside
        ``FetchService`` and never fail initialization.  No new Iceberg
        snapshot is created.
        """
        from ...application.services.fetch import FetchService
        from ...infrastructure.progress import progress_callback

        cb = progress_callback.get(None)
        if cb:
            cb.started("Updating warehouse timestamp")

        svc = FetchService(self._dataset, self._catalog)
        svc.execute()

        if cb:
            cb.finished()
