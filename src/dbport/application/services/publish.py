"""PublishService — client.publish(...) use case."""

from __future__ import annotations

import logging
from datetime import UTC

from ...domain.entities.codelist import ColumnCodelist
from ...domain.entities.dataset import Dataset
from ...domain.entities.version import DatasetVersion, VersionRecord
from ...domain.ports.catalog import ICatalog
from ...domain.ports.compute import ICompute
from ...domain.ports.lock import ILockStore
from ...domain.ports.metadata import IMetadataStore

logger = logging.getLogger(__name__)


class PublishService:
    """Write DuckDB output to Iceberg; manage metadata and codelists.

    Responsibilities:
    1. Fail-fast: lock schema == DuckDB schema
    2. Fail-fast: local schema compatible with existing warehouse schema (if any)
    3. Version conflict: skip if already completed; resume if interrupted
    4. Write output table to warehouse via DuckDB iceberg extension
    5. Generate codelist CSVs for categorical columns
    6. Materialize and attach metadata.json to Iceberg table properties
    7. Append VersionRecord to dbport.lock
    """

    def __init__(
        self,
        dataset: Dataset,
        catalog: ICatalog,
        compute: ICompute,
        lock: ILockStore,
        metadata: IMetadataStore,
    ) -> None:
        self._dataset = dataset
        self._catalog = catalog
        self._compute = compute
        self._lock = lock
        self._metadata = metadata

    def execute(self, version: DatasetVersion) -> VersionRecord:
        """Publish the dataset. Returns the resulting VersionRecord.

        Behaviour depends on version.mode:
        - None (default): idempotent — skip if version already completed.
        - "dry": validate schemas only; no data is written to the warehouse.
        - "refresh": overwrite an existing version, ignoring the completed checkpoint.
        """
        from datetime import datetime

        from ...adapters.secondary.catalog.drift import SchemaDriftError, check_schema_drift

        dataset = self._dataset
        table_address = dataset.table_address
        mode = version.mode

        # ------------------------------------------------------------------
        # 1. Load schema from lock
        # ------------------------------------------------------------------
        schema = self._lock.read_schema()
        if schema is None:
            raise RuntimeError(
                "No schema defined. Call client.schema(...) before publishing."
            )

        # ------------------------------------------------------------------
        # 2. Fail-fast: version already completed → skip idempotently
        #    (skipped when mode="refresh")
        # ------------------------------------------------------------------
        if mode != "refresh":
            existing_versions = self._lock.read_versions()
            for vr in existing_versions:
                if vr.version == version.version and vr.completed:
                    from ...infrastructure.progress import progress_callback

                    cb = progress_callback.get(None)
                    if cb:
                        cb.log(f"Version {version.version} already completed; skipping")
                    logger.info(
                        "Version %s already completed; skipping", version.version
                    )
                    return vr

        # ------------------------------------------------------------------
        # 3. Fail-fast: local schema vs warehouse schema (if table exists)
        # ------------------------------------------------------------------
        if self._catalog.table_exists(table_address):
            try:
                warehouse_arrow = self._catalog.load_arrow_schema(table_address)
                reader = self._compute.to_arrow_batches(
                    f"SELECT * FROM {table_address} LIMIT 0"
                )
                local_arrow = getattr(reader, "schema_arrow", None) or reader.schema
                check_schema_drift(local_arrow, warehouse_arrow)
            except SchemaDriftError:
                raise
            except Exception as exc:
                logger.warning("Schema drift check skipped: %s", exc)

        # ------------------------------------------------------------------
        # Dry run — schema checks passed; return without writing any data
        # ------------------------------------------------------------------
        if mode == "dry":
            logger.debug(
                "Dry run for %s version=%s — no data written",
                table_address,
                version.version,
            )
            from ...domain.entities.version import VersionRecord
            return VersionRecord(
                version=version.version,
                published_at=datetime.now(UTC).replace(microsecond=0),
                params=version.params,
                rows=0,
                completed=False,
            )

        # ------------------------------------------------------------------
        # 4. Write to Iceberg via DuckDB
        # ------------------------------------------------------------------
        logger.debug("Writing %s to Iceberg", table_address)
        version_record = self._catalog.write_versioned(
            table_address,
            version,
            self._compute,
            overwrite=(mode == "refresh"),
        )

        # ------------------------------------------------------------------
        # 6. Gather inputs and codelist entries
        # ------------------------------------------------------------------
        inputs = self._lock.read_ingest_records()
        codelist_entries = self._lock.read_codelist_entries()
        codelists = ColumnCodelist(entries=codelist_entries)

        # ------------------------------------------------------------------
        # 7. Generate codelist bytes in-memory
        # ------------------------------------------------------------------
        codelist_bytes = self._metadata.generate_codelist_bytes(
            codelists,
            self._compute,
            table_address,
        )

        # ------------------------------------------------------------------
        # 8. Build metadata.json in-memory
        # ------------------------------------------------------------------
        prev_metadata = self._catalog.get_table_property(
            table_address, "dbport.metadata_json"
        )
        metadata_bytes = self._metadata.build_metadata_json(
            dataset, version, inputs, codelists, prev_metadata,
            snapshot_id=version_record.iceberg_snapshot_id,
        )

        # ------------------------------------------------------------------
        # 9. Attach metadata and codelists to Iceberg table
        # ------------------------------------------------------------------
        self._metadata.attach_to_table(
            table_address, metadata_bytes, codelist_bytes, codelist_entries, self._catalog
        )

        # ------------------------------------------------------------------
        # 10. Persist VersionRecord to lock
        # ------------------------------------------------------------------
        self._lock.append_version(version_record)
        logger.debug(
            "Published %s version=%s rows=%s",
            table_address,
            version.version,
            version_record.rows,
        )
        return version_record
