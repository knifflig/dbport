"""IcebergCatalogAdapter — DuckDB-first Iceberg warehouse adapter.

Data operations (ingest, publish) prefer the DuckDB iceberg extension.
When the catalog does not support the multi-table commit endpoint
(e.g. Supabase), falls back to streaming Arrow batches through pyiceberg.
Metadata operations (snapshots, table properties, column docs) use pyiceberg.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ....infrastructure.credentials import WarehouseCreds

if TYPE_CHECKING:
    from ....domain.entities.input import InputDeclaration
    from ....domain.entities.version import DatasetVersion, VersionRecord

logger = logging.getLogger(__name__)


def _write_table_properties(table: Any, properties: dict[str, str]) -> None:
    """Write properties to an Iceberg table using transaction or update_properties."""
    tx = getattr(table, "transaction", None)
    if callable(tx):
        t = tx()
        set_props = getattr(t, "set_properties", None)
        commit = getattr(t, "commit_transaction", None)
        if callable(set_props) and callable(commit):
            set_props(properties)
            commit()
            return

    updater = getattr(table, "update_properties", None)
    if callable(updater):
        with updater() as u:
            for k, v in properties.items():
                u.set(k, v)
        return

    raise RuntimeError("Iceberg table does not support writing table properties")


def _write_column_docs(table: Any, column_docs: dict[str, str]) -> None:
    """Write column doc strings to an Iceberg table schema."""
    update_schema = getattr(table, "update_schema", None)
    if not callable(update_schema):
        raise RuntimeError("Iceberg table does not support schema updates for column docs")
    with update_schema() as update:
        for col, doc in column_docs.items():
            update.update_column(col, doc=doc)


class IcebergCatalogAdapter:
    """DuckDB-first Iceberg warehouse adapter.

    Data operations (ingest, publish) go through the DuckDB iceberg extension —
    data never leaves DuckDB for these paths.  Metadata-only operations
    (snapshots, table properties, column docs) use pyiceberg.

    Implements: ICatalog
    Dependencies: duckdb (iceberg extension), pyiceberg (metadata only)
    """

    _FALLBACK_BATCH_SIZE = 50_000
    _MAX_COMMIT_CONFLICT_RETRIES = 5

    def __init__(self, creds: WarehouseCreds) -> None:
        self._creds = creds
        self._catalog: Any = None  # pyiceberg.catalog.Catalog, lazy-loaded
        self._warehouse_attached = False
        self._duckdb_writes_supported: bool | None = None  # None = untested

    # ------------------------------------------------------------------
    # pyiceberg catalog (metadata operations only)
    # ------------------------------------------------------------------

    def _get_catalog(self) -> Any:
        """Lazy-load pyiceberg catalog for metadata operations."""
        if self._catalog is None:
            try:
                from pyiceberg.catalog import load_catalog
            except ImportError as exc:
                raise RuntimeError(
                    "pyiceberg is required. Install it: pip install pyiceberg[s3fs]"
                ) from exc
            creds = self._creds
            props: dict[str, str] = {
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            }
            if creds.s3_endpoint:
                props["s3.endpoint"] = creds.s3_endpoint
            if creds.s3_access_key:
                props["s3.access-key-id"] = creds.s3_access_key
            if creds.s3_secret_key:
                props["s3.secret-access-key"] = creds.s3_secret_key
            if creds.s3_region:
                props["s3.region"] = creds.s3_region

            self._catalog = load_catalog(
                "dbport",
                type="rest",
                warehouse=creds.warehouse,
                uri=creds.catalog_uri,
                token=creds.catalog_token,
                **props,
            )
        return self._catalog

    def _parse_address(self, table_address: str) -> tuple[str, str]:
        parts = table_address.split(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid table address (expected 'schema.table'): {table_address!r}")
        return parts[0], parts[1]

    # ------------------------------------------------------------------
    # DuckDB warehouse connection
    # ------------------------------------------------------------------

    def _ensure_warehouse_attached(self, compute: Any) -> None:
        """Load extensions, configure S3, and ATTACH the warehouse (idempotent).

        Extensions are installed via the compute adapter's ensure_extensions().
        """
        if self._warehouse_attached:
            return

        # Load extensions (iceberg, httpfs) — delegated to compute adapter
        compute.ensure_extensions()

        creds = self._creds

        # S3 path-style configuration for Supabase
        if creds.s3_endpoint:
            s3_endpoint = creds.s3_endpoint.replace("https://", "").replace("http://", "")
            compute.execute("SET s3_url_style='path'")
            compute.execute(f"SET s3_endpoint='{s3_endpoint}'")
            if creds.s3_access_key:
                compute.execute(f"SET s3_access_key_id='{creds.s3_access_key}'")
            if creds.s3_secret_key:
                compute.execute(f"SET s3_secret_access_key='{creds.s3_secret_key}'")
            if creds.s3_region:
                compute.execute(f"SET s3_region='{creds.s3_region}'")

        # REST catalog secret
        compute.execute(
            "CREATE OR REPLACE SECRET dbport_iceberg_catalog ("
            " TYPE ICEBERG,"
            f" ENDPOINT '{creds.catalog_uri}',"
            f" TOKEN '{creds.catalog_token}'"
            ")"
        )

        # Attach warehouse (idempotent)
        already = compute.execute(
            "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'dbport_warehouse'"
        ).fetchone()[0]
        if not already:
            compute.execute(
                f"ATTACH '{creds.warehouse}' AS dbport_warehouse ("
                "  TYPE ICEBERG,"
                "  SECRET dbport_iceberg_catalog,"
                "  ACCESS_DELEGATION_MODE 'none',"
                "  SUPPORT_STAGE_CREATE false,"
                "  PURGE_REQUESTED true"
                ")"
            )

        self._warehouse_attached = True

    # ------------------------------------------------------------------
    # ICatalog
    # ------------------------------------------------------------------

    def table_exists(self, table_address: str) -> bool:
        """Return True if the table exists in the warehouse."""
        catalog = self._get_catalog()
        ns, name = self._parse_address(table_address)
        try:
            return catalog.table_exists((ns, name))
        except Exception:
            return False

    def load_arrow_schema(self, table_address: str) -> Any:
        """Return the PyArrow schema of an existing warehouse table."""
        catalog = self._get_catalog()
        ns, name = self._parse_address(table_address)
        table = catalog.load_table((ns, name))
        return table.schema().as_arrow()

    # ------------------------------------------------------------------
    # Version / snapshot resolution (Fix A + Fix C)
    # ------------------------------------------------------------------

    def resolve_input_snapshot(
        self,
        table_address: str,
        version: str | None,
    ) -> tuple[str | None, int | None]:
        """Resolve a version label → (resolved_version, snapshot_id) via table properties.

        Reads only ``dbport.metadata_json`` from Iceberg table properties —
        no data scan, O(1) cost.

        Returns (None, None) when the table carries no DBPort metadata so the
        caller can fall back to ``current_snapshot()``.
        """
        import json

        raw = self.get_table_property(table_address, "dbport.metadata_json")
        if not raw:
            return None, None

        try:
            meta = json.loads(raw)
        except Exception:
            return None, None

        versions_list: list[dict] = meta.get("versions") or []

        if version is None:
            # Default: use the latest published version recorded in metadata
            latest_version_str: str | None = meta.get("last_updated_data_at")
            if not latest_version_str:
                return None, None
            # Find matching snapshot_id in versions list
            for entry in reversed(versions_list):
                if entry.get("version") == latest_version_str:
                    snap = entry.get("iceberg_snapshot_id")
                    return latest_version_str, (int(snap) if snap is not None else None)
            # Version string found but no snapshot entry yet (first publish
            # before this fix was deployed) — return version with no snapshot
            return latest_version_str, None
        else:
            # Specific version requested
            for entry in versions_list:
                if entry.get("version") == version:
                    snap = entry.get("iceberg_snapshot_id")
                    return version, (int(snap) if snap is not None else None)
            # Version not found in metadata
            raise ValueError(
                f"Version {version!r} not found in dbport.metadata_json for "
                f"{table_address!r}. Available: "
                f"{[e.get('version') for e in versions_list]}"
            )

    _MAX_INGEST_RETRIES = 3
    _INGEST_RETRY_BACKOFF = (2, 4, 8)  # seconds

    @staticmethod
    def _is_transient_s3_error(exc: Exception) -> bool:
        """Return True if the exception looks like a transient S3/network error."""
        msg = str(exc).lower()
        # S3 key/auth errors that may be transient (token refresh, eventual consistency)
        for pattern in (
            "invalidkey", "invalid key",
            "nosuchkey", "no such key",
            "invalidaccesskeyid",
            "requesttimeout", "request timeout",
            "slowdown",
            "serviceunavailable", "service unavailable",
            "internalerror", "internal error",
            "connection reset", "connection aborted",
            "broken pipe",
            "ioerror", "oserror",
        ):
            if pattern in msg:
                return True
        # HTTP 5xx or 429
        for code in ("500", "502", "503", "504", "429"):
            if code in msg:
                return True
        return False

    def _ingest_via_arrow(
        self,
        declaration: InputDeclaration,
        compute: Any,
        snapshot_id: int | None = None,
    ) -> int:
        """Arrow ingest: pyiceberg scan → RecordBatchReader → DuckDB.

        Uses Arrow C++ multi-threaded parquet reader.  The RecordBatchReader is
        registered directly with DuckDB so data is streamed in one pass without
        loading the entire table into Python memory.

        Retries up to ``_MAX_INGEST_RETRIES`` times on transient S3/network
        errors with exponential backoff.

        *snapshot_id* pins the scan to a specific Iceberg snapshot (Fix A).
        """
        import time

        try:
            from pyiceberg.expressions import And, EqualTo
        except ImportError as exc:
            raise RuntimeError(
                "pyiceberg is required for Arrow ingest fallback. "
                "Install it: pip install 'pyiceberg[s3fs]'"
            ) from exc

        from ....infrastructure.progress import progress_callback

        catalog = self._get_catalog()
        ns, name = self._parse_address(declaration.table_address)

        # Build pyiceberg row filter from filters dict
        row_filter: Any = None
        if declaration.filters:
            expr: Any = None
            for col, val in declaration.filters.items():
                term = EqualTo(str(col), value=val)
                expr = term if expr is None else And(expr, term)
            row_filter = expr

        # Build scan kwargs — pin to snapshot when known (Fix A)
        scan_kwargs: dict[str, Any] = {}
        if snapshot_id is not None:
            scan_kwargs["snapshot_id"] = snapshot_id
        if row_filter is not None:
            scan_kwargs["row_filter"] = row_filter

        last_exc: Exception | None = None

        for attempt in range(1, self._MAX_INGEST_RETRIES + 1):
            cb = progress_callback.get(None)

            try:
                iceberg_table = catalog.load_table((ns, name))
                scan = iceberg_table.scan(**scan_kwargs)

                if cb:
                    cb.started(f"Loading {declaration.table_address}", total=None)

                reader = scan.to_arrow_batch_reader()
                compute.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
                compute.register_arrow("_dbport_ingest_tmp", reader)
                try:
                    compute.execute(
                        f"CREATE OR REPLACE TABLE {ns}.{name} AS "
                        f"SELECT * FROM _dbport_ingest_tmp"
                    )
                finally:
                    compute.unregister_arrow("_dbport_ingest_tmp")

                row_count = compute.execute(
                    f"SELECT COUNT(*) FROM {ns}.{name}"
                ).fetchone()[0]

                if cb:
                    cb.finished(f"Loaded {declaration.table_address} ({row_count:,} rows)")

                logger.debug(
                    "Arrow ingest: %d rows into %s.%s (snapshot=%s)",
                    row_count, ns, name, snapshot_id,
                )
                return int(row_count)

            except Exception as exc:
                last_exc = exc

                if attempt >= self._MAX_INGEST_RETRIES or not self._is_transient_s3_error(exc):
                    if cb:
                        cb.finished()
                    raise

                backoff = self._INGEST_RETRY_BACKOFF[attempt - 1]
                failed = getattr(cb, "failed", None)
                if cb and callable(failed):
                    failed(
                        f"Loading {declaration.table_address} failed, "
                        f"retrying ({attempt}/{self._MAX_INGEST_RETRIES})"
                    )
                elif cb:
                    cb.finished()
                logger.warning(
                    "Transient error loading %s (attempt %d/%d): %s; "
                    "retrying in %ds",
                    declaration.table_address, attempt,
                    self._MAX_INGEST_RETRIES, exc, backoff,
                )
                time.sleep(backoff)

        raise last_exc  # type: ignore[misc]  # pragma: no cover — unreachable, satisfies type checker

    def ingest_into_compute(
        self,
        declaration: InputDeclaration,
        compute: Any,
        snapshot_id: int | None = None,
    ) -> int:
        """Load a warehouse table into DuckDB via pyiceberg Arrow scan.

        Uses pyiceberg's Arrow C++ multi-threaded parquet reader, streamed
        directly into DuckDB via RecordBatchReader without loading the full
        table into Python memory.  Pins to *snapshot_id* when provided.
        """
        return self._ingest_via_arrow(declaration, compute, snapshot_id=snapshot_id)

    def current_snapshot(self, table_address: str) -> tuple[int | None, int | None]:
        """Return (snapshot_id, snapshot_timestamp_ms) for a table's current snapshot."""
        catalog = self._get_catalog()
        ns, name = self._parse_address(table_address)
        try:
            table = catalog.load_table((ns, name))
        except Exception:
            return None, None

        try:
            snap = table.current_snapshot()
            if snap is not None:
                sid = getattr(snap, "snapshot_id", None)
                ts = getattr(snap, "timestamp_ms", None)
                return (
                    int(sid) if sid is not None else None,
                    int(ts) if ts is not None else None,
                )
        except Exception:
            pass

        try:
            metadata = getattr(table, "metadata", None)
            sid = getattr(metadata, "current_snapshot_id", None)
            if sid is not None:
                return int(sid), None
        except Exception:
            pass

        return None, None

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_duckdb_write_unsupported(exc: Exception) -> bool:
        """Check if the error indicates DuckDB iceberg writes are permanently unsupported.

        Detects known incompatibilities:
        1. Multi-table commit endpoint not implemented (404 on /transactions/commit)
        2. S3 auth failure during commit (vended credentials + custom S3 endpoint)
        3. S3 403 Forbidden / InvalidAccessKeyId during commit
        """
        msg = str(exc)
        msg_lower = msg.lower()
        # Multi-table commit endpoint not supported by catalog
        if "transactions/commit" in msg and ("404" in msg or "Not Found" in msg):
            return True
        # S3 authentication failure during commit (vended creds + custom endpoint)
        if "failed to commit" in msg_lower and "authorization mechanism" in msg_lower:
            return True
        # S3 403 Forbidden or InvalidAccessKeyId during commit
        if "failed to commit" in msg_lower and ("403" in msg or "forbidden" in msg_lower):
            return True
        return False

    def _write_via_duckdb(
        self, table_address: str, compute: Any, overwrite: bool,
    ) -> None:
        """Primary write path: DuckDB iceberg extension. Data never leaves DuckDB."""
        self._ensure_warehouse_attached(compute)
        if overwrite:
            try:
                compute.execute(f"DROP TABLE dbport_warehouse.{table_address}")
            except Exception:
                pass  # Table may not exist yet
            compute.execute(
                f"CREATE TABLE dbport_warehouse.{table_address} AS "
                f"SELECT * FROM {table_address}"
            )
        else:
            try:
                compute.execute(
                    f"INSERT INTO dbport_warehouse.{table_address} "
                    f"SELECT * FROM {table_address}"
                )
            except Exception as exc:
                # Only fall through to CREATE if this looks like a missing-table error.
                # Iceberg/S3 commit failures must propagate for fallback detection.
                if self._is_duckdb_write_unsupported(exc):
                    raise
                compute.execute(
                    f"CREATE TABLE dbport_warehouse.{table_address} AS "
                    f"SELECT * FROM {table_address}"
                )

    def _write_via_streaming_arrow(
        self,
        table_address: str,
        version: DatasetVersion,
        compute: Any,
        overwrite: bool,
        total_rows: int,
    ) -> None:
        """Fallback: stream Arrow batches through pyiceberg with per-batch checkpointing.

        Pattern adapted from the legacy iceberg_versioned_writer.py.
        Each batch (50K rows) is committed in a single pyiceberg transaction
        that atomically appends data + updates checkpoint properties.
        On commit conflict, the outer loop reloads table metadata and
        resumes from the remote checkpoint.
        """
        import pyarrow as pa

        try:
            from pyiceberg.exceptions import CommitFailedException
        except ImportError:
            CommitFailedException = Exception  # type: ignore[misc,assignment]

        catalog = self._get_catalog()
        ns, name = self._parse_address(table_address)
        version_key = re.sub(r"[^A-Za-z0-9_.-]", "_", version.version)
        checkpoint_prefix = f"dbport.upload.v2.{version_key}."
        batches_prop = checkpoint_prefix + "batches_appended"
        rows_prop = checkpoint_prefix + "rows_appended"
        completed_prop = checkpoint_prefix + "completed"

        from ....infrastructure.progress import progress_callback

        cb = progress_callback.get(None)
        if cb:
            cb.started(f"Publishing {table_address}", total=total_rows)

        # Get Arrow schema from DuckDB (zero rows)
        schema_reader = compute.to_arrow_batches(
            f"SELECT * FROM {table_address} LIMIT 0", batch_size=1,
        )
        arrow_schema = schema_reader.schema

        attempt = 0
        while True:
            attempt += 1

            # Ensure table exists
            if overwrite and attempt == 1:
                try:
                    catalog.drop_table((ns, name), purge_requested=True)
                except Exception:
                    pass

            try:
                iceberg_table = catalog.load_table((ns, name))
            except Exception:
                iceberg_table = catalog.create_table(
                    f"{ns}.{name}", schema=arrow_schema,
                )

            # Read checkpoint from remote
            props = getattr(iceberg_table, "properties", None) or {}
            committed_batches = int(props.get(batches_prop, 0))
            committed_rows = int(props.get(rows_prop, 0))

            if overwrite and attempt == 1:
                committed_batches = 0
                committed_rows = 0

            # Stream from DuckDB
            data_reader = compute.to_arrow_batches(
                f"SELECT * FROM {table_address}",
                batch_size=self._FALLBACK_BATCH_SIZE,
            )

            # Skip already-committed batches (batch-enumeration resume)
            skipped = 0
            batch_iter = iter(data_reader)
            while skipped < committed_batches:
                try:
                    batch = next(batch_iter)
                except StopIteration:
                    break
                if batch.num_rows > 0:
                    skipped += 1

            batches_total = committed_batches
            rows_total = committed_rows
            had_conflict = False

            for batch in batch_iter:
                if batch.num_rows == 0:
                    continue

                arrow_chunk = pa.Table.from_batches([batch], schema=arrow_schema)

                try:
                    # Atomic: data append + checkpoint in one transaction
                    tx = iceberg_table.transaction()
                    tx.append(arrow_chunk)
                    tx.set_properties({
                        batches_prop: str(batches_total + 1),
                        rows_prop: str(rows_total + batch.num_rows),
                    })
                    tx.commit_transaction()

                    batches_total += 1
                    rows_total += batch.num_rows

                    if cb:
                        cb.update(batch.num_rows)

                    if batches_total % 20 == 0:
                        logger.debug(
                            "Streaming write progress: %d/%d rows (%.0f%%)",
                            rows_total, total_rows,
                            100 * rows_total / total_rows if total_rows else 0,
                        )

                except CommitFailedException as exc:
                    msg = str(exc)
                    if "branch main has changed" in msg or "expected id" in msg:
                        had_conflict = True
                        logger.warning(
                            "Commit conflict (attempt %d/%d); retrying from checkpoint",
                            attempt, self._MAX_COMMIT_CONFLICT_RETRIES,
                        )
                        break
                    raise

            if had_conflict:
                if attempt >= self._MAX_COMMIT_CONFLICT_RETRIES:
                    raise RuntimeError(
                        f"Commit conflicts not resolved after "
                        f"{self._MAX_COMMIT_CONFLICT_RETRIES} attempts "
                        f"(table={table_address})"
                    )
                continue  # Retry: reload table, resume from checkpoint

            # Mark completed
            try:
                iceberg_table = catalog.load_table((ns, name))
                _write_table_properties(iceberg_table, {
                    completed_prop: "1",
                    rows_prop: str(rows_total),
                })
            except Exception as exc:
                logger.warning("Could not write completion checkpoint: %s", exc)

            if cb:
                cb.finished(
                    f"Published {table_address} ({rows_total:,} rows)"
                )

            logger.debug(
                "Streaming write complete: %d rows in %d batches (%s)",
                rows_total, batches_total, table_address,
            )
            break  # Done

    # ------------------------------------------------------------------
    # write_versioned — public API
    # ------------------------------------------------------------------

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
        """
        from ....domain.entities.version import VersionRecord

        version_key = re.sub(r"[^A-Za-z0-9_.-]", "_", version.version)
        checkpoint_prefix = f"dbport.upload.v2.{version_key}."
        completed_prop = checkpoint_prefix + "completed"
        rows_prop = checkpoint_prefix + "rows_appended"

        from ....infrastructure.progress import progress_callback

        # Check if version already completed (via pyiceberg table properties)
        if not overwrite:
            ns, name = self._parse_address(table_address)
            try:
                catalog = self._get_catalog()
                iceberg_table = catalog.load_table((ns, name))
                props = getattr(iceberg_table, "properties", None) or {}
                if str(props.get(completed_prop, "")).strip().lower() in ("1", "true", "yes"):
                    committed_rows = int(props.get(rows_prop, 0))
                    cb = progress_callback.get(None)
                    if cb:
                        cb.log(f"Version {version_key} already completed; skipping")
                    logger.info(
                        "Version %s already completed; skipping (table=%s rows=%d)",
                        version_key, table_address, committed_rows,
                    )
                    snap_id, snap_ts_ms = self.current_snapshot(table_address)
                    snap_ts = (
                        datetime.fromtimestamp(snap_ts_ms / 1000, tz=timezone.utc)
                        if snap_ts_ms is not None else None
                    )
                    return VersionRecord(
                        version=version.version,
                        published_at=datetime.now(timezone.utc).replace(microsecond=0),
                        iceberg_snapshot_id=snap_id,
                        iceberg_snapshot_timestamp=snap_ts,
                        params=version.params,
                        rows=committed_rows,
                        completed=True,
                    )
            except Exception:
                pass  # Table doesn't exist yet — proceed with write

        row_count = compute.execute(
            f"SELECT COUNT(*) FROM {table_address}"
        ).fetchone()[0]

        cb = progress_callback.get(None)

        # --- Primary: DuckDB iceberg extension ---
        if self._duckdb_writes_supported is not False:
            if cb:
                cb.started(
                    f"Publishing {table_address} ({row_count:,} rows)", total=None
                )
            try:
                self._write_via_duckdb(table_address, compute, overwrite)
                self._duckdb_writes_supported = True
                if cb:
                    cb.finished(
                        f"Published {table_address} ({row_count:,} rows)"
                    )
            except Exception as exc:
                if self._is_duckdb_write_unsupported(exc):
                    if cb:
                        cb.log("Switching to streaming Arrow fallback")
                    logger.info(
                        "DuckDB iceberg commit not supported by catalog; "
                        "switching to streaming Arrow fallback"
                    )
                    self._duckdb_writes_supported = False
                    failed = getattr(cb, "failed", None)
                    if cb and callable(failed):
                        failed("DuckDB write unsupported, using Arrow fallback")
                    elif cb:
                        cb.finished("Switching to streaming Arrow fallback")
                else:
                    if cb:
                        cb.finished()
                    raise

        # --- Fallback: streaming Arrow via pyiceberg ---
        if self._duckdb_writes_supported is False:
            self._write_via_streaming_arrow(
                table_address, version, compute, overwrite, row_count,
            )

        logger.debug(
            "write_versioned: done (table=%s version=%s rows=%d)",
            table_address, version_key, row_count,
        )

        # Mark completed (DuckDB path only; fallback marks its own)
        if self._duckdb_writes_supported is True:
            ns, name = self._parse_address(table_address)
            try:
                catalog = self._get_catalog()
                iceberg_table = catalog.load_table((ns, name))
                _write_table_properties(iceberg_table, {
                    completed_prop: "1",
                    rows_prop: str(row_count),
                })
            except Exception as exc:
                logger.warning("Could not write completion checkpoint: %s", exc)

        snap_id, snap_ts_ms = self.current_snapshot(table_address)
        snap_ts: datetime | None = None
        if snap_ts_ms is not None:
            snap_ts = datetime.fromtimestamp(snap_ts_ms / 1000, tz=timezone.utc)

        return VersionRecord(
            version=version.version,
            published_at=datetime.now(timezone.utc).replace(microsecond=0),
            iceberg_snapshot_id=snap_id,
            iceberg_snapshot_timestamp=snap_ts,
            params=version.params,
            rows=int(row_count),
            completed=True,
        )

    def get_table_property(
        self, table_address: str, key: str
    ) -> str | None:
        """Return a single table property value, or None if not set."""
        catalog = self._get_catalog()
        ns, name = self._parse_address(table_address)
        try:
            table = catalog.load_table((ns, name))
            return (getattr(table, "properties", None) or {}).get(key)
        except Exception:
            return None

    def update_table_properties(
        self, table_address: str, properties: dict[str, str]
    ) -> None:
        """Merge key/value properties into the Iceberg table metadata."""
        catalog = self._get_catalog()
        ns, name = self._parse_address(table_address)
        table = catalog.load_table((ns, name))
        _write_table_properties(table, properties)

    def update_column_docs(
        self, table_address: str, column_docs: dict[str, str]
    ) -> None:
        """Write column doc strings to the Iceberg table schema."""
        catalog = self._get_catalog()
        ns, name = self._parse_address(table_address)
        table = catalog.load_table((ns, name))
        _write_column_docs(table, column_docs)
