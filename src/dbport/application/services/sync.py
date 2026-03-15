"""SyncService — bring local DuckDB state in sync with lock file and warehouse."""

from __future__ import annotations

import logging

from ...domain.entities.input import InputDeclaration
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
        """Create or recreate the output table from lock schema."""
        schema = self._lock.read_schema()
        if schema is None:
            return

        ns = table_address.split(".", 1)[0] if "." in table_address else None
        if ns:
            self._compute.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
        self._compute.execute(schema.ddl.statement)
        logger.debug("Output table synced from lock schema: %s", table_address)

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
                logger.warning(
                    "Failed to sync input %s: %s",
                    record.table_address,
                    exc,
                )
