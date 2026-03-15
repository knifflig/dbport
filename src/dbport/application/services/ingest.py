"""IngestService — client.load(...) use case."""

from __future__ import annotations

import logging

from ...domain.entities.input import IngestRecord, InputDeclaration
from ...domain.ports.catalog import ICatalog
from ...domain.ports.compute import ICompute
from ...domain.ports.lock import ILockStore

logger = logging.getLogger(__name__)


class IngestService:
    """Load an Iceberg table into DuckDB; persist ingest state to dbport.lock.

    Responsibilities:
    - Resolve the target Iceberg snapshot via DBPort metadata (Fix A + Fix C):
        * Read ``dbport.metadata_json`` from table properties (no data scan).
        * Default: latest published version (``last_updated_data_at``).
        * Override: explicit ``declaration.version`` resolves to its snapshot.
        * Non-DBPort tables: fall back to the current snapshot.
    - Compare resolved snapshot against the persisted IngestRecord to decide
      skip vs. re-ingest (Fix B: compare against the *actual* pinned snapshot,
      not a pre-load best-guess).
    - Load from the catalog into DuckDB, passing the resolved snapshot_id so
      the Arrow path can pin the scan exactly.
    - Write the updated IngestRecord (with resolved version + snapshot_id) to
      dbport.lock.
    """

    def __init__(
        self, catalog: ICatalog, compute: ICompute, lock: ILockStore
    ) -> None:
        self._catalog = catalog
        self._compute = compute
        self._lock = lock

    def execute(self, declaration: InputDeclaration) -> IngestRecord:
        """Load (or skip) the input. Returns the resulting IngestRecord."""
        from ...adapters.secondary.compute.ingest_cache import should_skip_ingest

        table_address = declaration.table_address
        parts = table_address.split(".", 1)
        schema_name, table_name = (parts[0], parts[1]) if len(parts) == 2 else ("main", parts[0])

        # ------------------------------------------------------------------
        # Resolve snapshot (Fix A + Fix C)
        # Try DBPort metadata first (O(1), no data scan).
        # Fall back to current_snapshot() for non-DBPort tables.
        # ------------------------------------------------------------------
        resolved_version: str | None = None
        resolved_snap_id: int | None = None
        snap_ts_ms: int | None = None

        try:
            resolved_version, resolved_snap_id = self._catalog.resolve_input_snapshot(
                table_address, declaration.version
            )
        except ValueError:
            # Explicit version not found in metadata — propagate to caller
            raise
        except Exception as exc:
            logger.debug(
                "resolve_input_snapshot failed for %s (%s); "
                "falling back to current_snapshot()",
                table_address, exc,
            )

        if resolved_snap_id is None:
            # Either no DBPort metadata, or the table was published before Fix C
            # and the versions list has no iceberg_snapshot_id yet.
            # Fall back to current_snapshot() so the cache still works.
            resolved_snap_id, snap_ts_ms = self._catalog.current_snapshot(table_address)

        logger.debug(
            "Resolved %s → version=%s snapshot_id=%s",
            table_address, resolved_version, resolved_snap_id,
        )

        # ------------------------------------------------------------------
        # Cache check: skip if snapshot unchanged and table already loaded
        # (Fix B: uses the *resolved* snapshot, not a pre-load best-guess)
        # ------------------------------------------------------------------
        existing_records = self._lock.read_ingest_records()
        current_record = next(
            (r for r in existing_records if r.table_address == table_address),
            None,
        )

        relation_exists = self._compute.relation_exists(schema_name, table_name)
        if relation_exists and should_skip_ingest(
            current_record, resolved_snap_id, table_address, declaration.filters
        ):
            from ...infrastructure.progress import progress_callback

            cb = progress_callback.get(None)
            if cb:
                cb.log(f"Skipping {table_address} (snapshot unchanged)")
            logger.debug(
                "Skipping %s (snapshot unchanged: %s version=%s)",
                table_address, resolved_snap_id, resolved_version,
            )
            return current_record  # type: ignore[return-value]

        # ------------------------------------------------------------------
        # Load — pass resolved snapshot_id so Arrow path can pin the scan
        # ------------------------------------------------------------------
        logger.debug(
            "Loading %s (version=%s snapshot=%s)",
            table_address, resolved_version, resolved_snap_id,
        )
        rows_loaded = self._catalog.ingest_into_compute(
            declaration, self._compute, snapshot_id=resolved_snap_id
        )
        logger.debug(
            "Loaded %d rows into %s.%s",
            rows_loaded, schema_name, table_name,
        )

        # ------------------------------------------------------------------
        # Persist ingest record (Fix B: record the resolved snapshot, not
        # the pre-load current_snapshot() value)
        # ------------------------------------------------------------------
        record = IngestRecord(
            table_address=table_address,
            last_snapshot_id=resolved_snap_id,
            last_snapshot_timestamp_ms=snap_ts_ms,
            rows_loaded=rows_loaded,
            filters=declaration.filters,
            version=resolved_version,
        )
        self._lock.write_ingest_record(record)
        return record
