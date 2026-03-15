"""SyncService — bring local DuckDB state in sync with lock file and warehouse."""

from __future__ import annotations

import logging

from ...domain.entities.input import InputDeclaration
from ...infrastructure.progress import progress_callback
from ...domain.ports.catalog import ICatalog
from ...domain.ports.compute import ICompute
from ...domain.ports.lock import ILockStore

logger = logging.getLogger(__name__)


class SyncService:
    """Ensure DuckDB matches the lock file (which reflects the warehouse).

    Called during DBPort init to bring the local environment up to date:
    1. Create the output table in DuckDB from the lock schema (if defined).
    2. Reload any inputs that are missing or stale in DuckDB.
    """

    def __init__(
        self,
        catalog: ICatalog,
        compute: ICompute,
        lock: ILockStore,
    ) -> None:
        self._catalog = catalog
        self._compute = compute
        self._lock = lock

    def execute(self, table_address: str) -> None:
        """Sync local DuckDB state with lock file."""
        self._sync_output_table(table_address)
        self._sync_inputs()

    def _sync_output_table(self, table_address: str) -> None:
        """Create or recreate the output table from lock schema.

        Skips DDL execution when the table already exists to preserve data
        from a previous ``dbp run``.
        """
        schema = self._lock.read_schema()
        if schema is None:
            return

        # Split into namespace and table name
        if "." in table_address:
            ns, name = table_address.split(".", 1)
        else:
            ns, name = "main", table_address

        # Skip if table already exists — avoids wiping data from a prior run
        if self._compute.relation_exists(ns, name):
            logger.debug("Output table already exists, skipping DDL: %s", table_address)
            return

        cb = progress_callback.get(None)
        if cb:
            cb.started(f"Creating output table {table_address}")

        if ns != "main":
            self._compute.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
        self._compute.execute(schema.ddl.statement)
        logger.debug("Output table synced from lock schema: %s", table_address)

        if cb:
            cb.finished()

    def _sync_inputs(self) -> None:
        """Reload inputs that are missing or stale in DuckDB."""
        from .ingest import IngestService

        records = self._lock.read_ingest_records()
        if not records:
            return

        ingest_svc = IngestService(self._catalog, self._compute, self._lock)
        for record in records:
            declaration = InputDeclaration(
                table_address=record.table_address,
                filters=record.filters,
                version=record.version,
            )
            try:
                ingest_svc.execute(declaration)
            except Exception as exc:
                cb = progress_callback.get(None)
                if cb:
                    cb.failed(f"Failed to sync {record.table_address}")
                logger.warning(
                    "Failed to sync input %s: %s",
                    record.table_address,
                    exc,
                )
