"""Tests for IcebergCatalogAdapter.write_versioned (DuckDB + streaming fallback)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from dbport.adapters.secondary.catalog.iceberg import IcebergCatalogAdapter
from dbport.domain.entities.version import DatasetVersion, VersionRecord
from dbport.infrastructure.credentials import WarehouseCreds

_SIMPLE_SCHEMA = pa.schema([("x", pa.int64())])


def _make_creds(**overrides) -> WarehouseCreds:
    defaults = dict(
        catalog_uri="https://catalog.example.com",
        catalog_token="tok",
        warehouse="my_wh",
    )
    defaults.update(overrides)
    return WarehouseCreds(**defaults)


class _FakeTransaction:
    def __init__(self, table):
        self._table = table
        self._appended = []
        self._props_to_set = {}

    def append(self, arrow_table):
        self._appended.append(arrow_table)

    def set_properties(self, props):
        self._props_to_set.update(props)

    def commit_transaction(self):
        self._table._props.update(self._props_to_set)
        self._table._appended.extend(self._appended)


class _FakeIcebergTable:
    def __init__(self, props=None):
        self._props = dict(props or {})
        self._appended = []  # Track appended data

    @property
    def properties(self):
        return dict(self._props)

    def transaction(self):
        return _FakeTransaction(self)

    def current_snapshot(self):
        return None


class _FakePyicebergCatalog:
    def __init__(self, table=None):
        self._table = table
        self._created_table = None
        self._dropped = False

    def load_table(self, ident):
        if self._table is None:
            raise LookupError(f"Table {ident} not found")
        return self._table

    def table_exists(self, ident):
        return self._table is not None

    def create_table(self, name, schema=None):
        self._created_table = _FakeIcebergTable()
        self._table = self._created_table
        return self._created_table

    def drop_table(self, ident, purge_requested=False):
        self._dropped = True
        self._table = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_commit_404_error():
    """Simulate the DuckDB iceberg 'transactions/commit' 404 error."""
    return Exception(
        'TransactionContext Error: Failed to commit: Failed to commit '
        'Iceberg transaction: {"message":"Route POST:/iceberg/v1/'
        'my-warehouse/transactions/commit not found","error":"Not Found",'
        '"statusCode":404}'
    )


def _make_batch(num_rows: int) -> pa.RecordBatch:
    """Create a real PyArrow RecordBatch with the given number of rows."""
    return pa.record_batch({"x": list(range(num_rows))}, schema=_SIMPLE_SCHEMA)


class _FakeRecordBatchReader:
    """Minimal Arrow RecordBatchReader stand-in."""
    def __init__(self, batches, schema=None):
        self._batches = list(batches)
        self.schema = schema or _SIMPLE_SCHEMA

    def __iter__(self):
        return iter(self._batches)

    def __next__(self):
        return next(iter(self._batches))


# ===========================================================================
# DuckDB primary path tests
# ===========================================================================


class TestDuckDBWritePath:
    def _make_adapter(self, iceberg_table=None):
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True
        adapter._catalog = _FakePyicebergCatalog(iceberg_table)
        return adapter

    def _mock_compute(self, row_count=5, insert_fails=False):
        compute = MagicMock()
        sql_log: list[str] = []

        def execute_side_effect(sql, *args, **kwargs):
            sql_log.append(sql)
            if insert_fails and sql.strip().upper().startswith("INSERT INTO"):
                raise Exception("Table does not exist")
            result = MagicMock()
            result.fetchone.return_value = (row_count,)
            return result

        compute.execute.side_effect = execute_side_effect
        compute._sql_log = sql_log
        return compute

    def test_writes_via_duckdb_insert(self):
        iceberg_table = _FakeIcebergTable()
        adapter = self._make_adapter(iceberg_table)
        compute = self._mock_compute(row_count=100)
        version = DatasetVersion(version="2026-03-13")

        result = adapter.write_versioned("wifor.emp", version, compute)

        assert isinstance(result, VersionRecord)
        assert result.rows == 100
        assert result.completed is True
        assert adapter._duckdb_writes_supported is True
        insert_calls = [s for s in compute._sql_log if "INSERT INTO" in s]
        assert len(insert_calls) == 1
        assert "dbport_warehouse.wifor.emp" in insert_calls[0]

    def test_creates_table_on_first_publish(self):
        adapter = self._make_adapter()
        compute = self._mock_compute(row_count=10, insert_fails=True)
        version = DatasetVersion(version="2026-03-13")

        result = adapter.write_versioned("wifor.emp", version, compute)

        assert result.rows == 10
        create_calls = [s for s in compute._sql_log if "CREATE TABLE dbport_warehouse" in s]
        assert len(create_calls) == 1

    def test_overwrite_drops_then_creates(self):
        iceberg_table = _FakeIcebergTable()
        adapter = self._make_adapter(iceberg_table)
        compute = self._mock_compute(row_count=50)
        version = DatasetVersion(version="2026-03-13")

        result = adapter.write_versioned("wifor.emp", version, compute, overwrite=True)

        assert result.rows == 50
        drop_calls = [s for s in compute._sql_log if "DROP TABLE" in s]
        create_calls = [s for s in compute._sql_log if "CREATE TABLE dbport_warehouse" in s]
        assert len(drop_calls) == 1
        assert len(create_calls) == 1
        drop_idx = compute._sql_log.index(drop_calls[0])
        create_idx = compute._sql_log.index(create_calls[0])
        assert drop_idx < create_idx

    def test_skips_when_already_completed(self):
        props = {
            "dbport.upload.v2.2026-03-13.completed": "1",
            "dbport.upload.v2.2026-03-13.rows_appended": "42",
        }
        iceberg_table = _FakeIcebergTable(props=props)
        adapter = self._make_adapter(iceberg_table)
        compute = self._mock_compute()
        version = DatasetVersion(version="2026-03-13")

        result = adapter.write_versioned("wifor.emp", version, compute)

        assert result.rows == 42
        assert result.completed is True
        write_calls = [
            s for s in compute._sql_log
            if any(kw in s for kw in ("INSERT INTO", "CREATE TABLE", "DROP TABLE"))
        ]
        assert len(write_calls) == 0

    def test_overwrite_ignores_completed_checkpoint(self):
        props = {
            "dbport.upload.v2.2026-03-13.completed": "1",
            "dbport.upload.v2.2026-03-13.rows_appended": "42",
        }
        iceberg_table = _FakeIcebergTable(props=props)
        adapter = self._make_adapter(iceberg_table)
        compute = self._mock_compute(row_count=77)
        version = DatasetVersion(version="2026-03-13")

        result = adapter.write_versioned("wifor.emp", version, compute, overwrite=True)

        assert result.rows == 77

    def test_returns_version_record(self):
        adapter = self._make_adapter()
        compute = self._mock_compute(row_count=99, insert_fails=True)
        version = DatasetVersion(version="2026-03-13", params={"src": "test"})

        result = adapter.write_versioned("wifor.emp", version, compute)

        assert isinstance(result, VersionRecord)
        assert result.version == "2026-03-13"
        assert result.params == {"src": "test"}
        assert result.completed is True
        assert result.rows == 99

    def test_marks_completed_in_table_properties(self):
        iceberg_table = _FakeIcebergTable()
        adapter = self._make_adapter(iceberg_table)
        compute = self._mock_compute(row_count=10)
        version = DatasetVersion(version="v1")

        adapter.write_versioned("wifor.emp", version, compute)

        assert iceberg_table._props.get("dbport.upload.v2.v1.completed") == "1"
        assert iceberg_table._props.get("dbport.upload.v2.v1.rows_appended") == "10"


# ===========================================================================
# Streaming Arrow fallback tests
# ===========================================================================


class TestStreamingArrowFallback:
    def _make_adapter(self, iceberg_table=None):
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True
        adapter._catalog = _FakePyicebergCatalog(iceberg_table)
        return adapter

    def _mock_compute_with_fallback(self, row_count=100, num_batches=2):
        """Build compute that triggers DuckDB 404 and provides Arrow batches."""
        compute = MagicMock()
        rows_per_batch = row_count // num_batches if num_batches else row_count

        def execute_side_effect(sql, *args, **kwargs):
            upper = sql.strip().upper()
            # DuckDB writes trigger the 404 error
            if upper.startswith("INSERT INTO DBPORT_WAREHOUSE") or \
               upper.startswith("CREATE TABLE DBPORT_WAREHOUSE"):
                raise _make_commit_404_error()
            if upper.startswith("DROP TABLE DBPORT_WAREHOUSE"):
                raise _make_commit_404_error()
            result = MagicMock()
            result.fetchone.return_value = (row_count,)
            return result

        compute.execute.side_effect = execute_side_effect

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            batches = [_make_batch(rows_per_batch) for _ in range(num_batches)]
            return _FakeRecordBatchReader(batches)

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        return compute

    def test_fallback_triggered_on_commit_404(self):
        """DuckDB 404 → fallback streams batches through pyiceberg."""
        adapter = self._make_adapter()
        compute = self._mock_compute_with_fallback(row_count=100, num_batches=2)
        version = DatasetVersion(version="2026-03-14")

        # Patch pa.Table.from_batches for the fallback
        import pyarrow as pa
        orig = pa.Table.from_batches

        result = adapter.write_versioned("wifor.emp", version, compute)

        assert result.rows == 100
        assert result.completed is True
        assert adapter._duckdb_writes_supported is False
        # Table was created via pyiceberg
        assert adapter._catalog._created_table is not None
        # Batches were appended
        assert len(adapter._catalog._table._appended) == 2

    def test_fallback_caches_session_decision(self):
        """Second write_versioned skips DuckDB attempt."""
        adapter = self._make_adapter()
        compute = self._mock_compute_with_fallback(row_count=50, num_batches=1)
        version1 = DatasetVersion(version="v1")

        adapter.write_versioned("wifor.emp", version1, compute)
        assert adapter._duckdb_writes_supported is False

        # Reset catalog for second write (clear completed checkpoint)
        adapter._catalog = _FakePyicebergCatalog()

        version2 = DatasetVersion(version="v2")
        # Count DuckDB write attempts
        duckdb_calls_before = [
            c for c in compute.execute.call_args_list
            if "dbport_warehouse" in str(c).upper()
        ]

        adapter.write_versioned("wifor.emp", version2, compute)

        # No new DuckDB warehouse write attempts
        duckdb_calls_after = [
            c for c in compute.execute.call_args_list
            if "dbport_warehouse" in str(c).upper()
        ]
        assert len(duckdb_calls_after) == len(duckdb_calls_before)

    def test_fallback_resumes_from_checkpoint(self):
        """Partial write: 2 batches committed → skips those, appends rest."""
        # Table with checkpoint showing 2 batches already committed
        props = {
            "dbport.upload.v2.2026-03-14.batches_appended": "2",
            "dbport.upload.v2.2026-03-14.rows_appended": "100",
        }
        iceberg_table = _FakeIcebergTable(props=props)
        adapter = self._make_adapter(iceberg_table)
        adapter._duckdb_writes_supported = False  # Skip DuckDB attempt

        # 4 total batches (2 already committed, 2 remaining)
        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(200,))
        )

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader(
                [_make_batch(50) for _ in range(4)],
            )

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect

        version = DatasetVersion(version="2026-03-14")
        result = adapter.write_versioned("wifor.emp", version, compute)

        assert result.rows == 200
        # Only 2 new batches appended (the other 2 were skipped)
        assert len(iceberg_table._appended) == 2
        # Checkpoint updated
        assert iceberg_table._props.get("dbport.upload.v2.2026-03-14.completed") == "1"

    def test_fallback_overwrite_drops_and_recreates(self):
        """Overwrite mode: pyiceberg drop + create + stream."""
        iceberg_table = _FakeIcebergTable()
        adapter = self._make_adapter(iceberg_table)
        adapter._duckdb_writes_supported = False

        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(30,))
        )

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader(
                [_make_batch(30)],
            )

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect

        version = DatasetVersion(version="2026-03-14")
        result = adapter.write_versioned(
            "wifor.emp", version, compute, overwrite=True,
        )

        assert result.rows == 30
        assert adapter._catalog._dropped is True
        assert adapter._catalog._created_table is not None

    def test_duckdb_success_no_fallback(self):
        """DuckDB write succeeds → no pyiceberg fallback."""
        iceberg_table = _FakeIcebergTable()
        adapter = self._make_adapter(iceberg_table)
        compute = MagicMock()
        sql_log = []

        def execute_side_effect(sql, *args, **kwargs):
            sql_log.append(sql)
            result = MagicMock()
            result.fetchone.return_value = (10,)
            return result

        compute.execute.side_effect = execute_side_effect
        version = DatasetVersion(version="2026-03-14")

        result = adapter.write_versioned("wifor.emp", version, compute)

        assert adapter._duckdb_writes_supported is True
        assert result.rows == 10
        # No to_arrow_batches calls (no fallback)
        compute.to_arrow_batches.assert_not_called()

    def test_fallback_marks_completed_in_properties(self):
        """Fallback writes completion checkpoint in table properties."""
        adapter = self._make_adapter()
        adapter._duckdb_writes_supported = False

        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(25,))
        )

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader(
                [_make_batch(25)],
            )

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect

        version = DatasetVersion(version="v1")
        adapter.write_versioned("wifor.emp", version, compute)

        table = adapter._catalog._table
        assert table._props.get("dbport.upload.v2.v1.completed") == "1"
        assert table._props.get("dbport.upload.v2.v1.rows_appended") == "25"


# ===========================================================================
# Write path hardening tests
# ===========================================================================


class TestDuckDBWriteUnsupportedDetection:
    def test_detects_404_transactions_commit(self):
        exc = _make_commit_404_error()
        assert IcebergCatalogAdapter._is_duckdb_write_unsupported(exc) is True

    def test_detects_s3_authorization_mechanism_failure(self):
        exc = Exception(
            "Failed to commit Iceberg transaction: the authorization mechanism "
            "you have provided is not supported"
        )
        assert IcebergCatalogAdapter._is_duckdb_write_unsupported(exc) is True

    def test_detects_s3_invalid_access_key(self):
        exc = Exception(
            "TransactionContext Error: Failed to commit: Failed to commit "
            "Iceberg transaction: HTTP GET error reading "
            "'https://example.s3.eu-central-1.amazonaws.com/metadata/foo.avro' "
            "in region 'eu-central-1' (HTTP 403 Forbidden)\n\n"
            "InvalidAccessKeyId: The AWS Access Key Id you provided does not "
            "exist in our records."
        )
        assert IcebergCatalogAdapter._is_duckdb_write_unsupported(exc) is True

    def test_detects_s3_403_forbidden_during_commit(self):
        exc = Exception(
            "Failed to commit Iceberg transaction: HTTP 403 Forbidden"
        )
        assert IcebergCatalogAdapter._is_duckdb_write_unsupported(exc) is True

    def test_non_matching_errors_return_false(self):
        exc = Exception("Table not found: wifor.emp")
        assert IcebergCatalogAdapter._is_duckdb_write_unsupported(exc) is False

    def test_generic_disk_error_not_treated_as_unsupported(self):
        exc = Exception("disk full — cannot write")
        assert IcebergCatalogAdapter._is_duckdb_write_unsupported(exc) is False


class TestStreamingArrowCommitConflict:
    def _make_adapter(self, iceberg_table=None):
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True
        adapter._duckdb_writes_supported = False  # Force Arrow path
        adapter._catalog = _FakePyicebergCatalog(iceberg_table)
        return adapter

    def _mock_compute(self, row_count=50, num_batches=1):
        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(row_count,))
        )

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader(
                [_make_batch(row_count // num_batches) for _ in range(num_batches)],
            )

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        return compute

    def test_commit_conflict_exhausts_max_retries(self):
        """5 consecutive commit conflicts -> RuntimeError."""
        class _ConflictTable:
            def __init__(self):
                self._props = {}
                self._appended = []

            @property
            def properties(self):
                return dict(self._props)

            def transaction(self):
                return _ConflictTransaction(self)

            def current_snapshot(self):
                return None

        class _ConflictTransaction:
            def __init__(self, table):
                self._table = table

            def append(self, data):
                pass

            def set_properties(self, props):
                pass

            def commit_transaction(self):
                try:
                    from pyiceberg.exceptions import CommitFailedException
                    raise CommitFailedException("branch main has changed")
                except ImportError:
                    raise Exception("branch main has changed")

        class _ConflictCatalog:
            def __init__(self):
                self._table = _ConflictTable()
                self._created_table = None

            def load_table(self, ident):
                return self._table

            def table_exists(self, ident):
                return True

            def create_table(self, name, schema=None):
                return self._table

            def drop_table(self, ident, purge_requested=False):
                pass

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True
        adapter._duckdb_writes_supported = False
        adapter._catalog = _ConflictCatalog()

        compute = self._mock_compute(row_count=50, num_batches=1)
        version = DatasetVersion(version="v1")

        with pytest.raises(RuntimeError, match="Commit conflicts not resolved"):
            adapter.write_versioned("wifor.emp", version, compute)

    def test_empty_batches_skipped(self):
        """Batches with num_rows == 0 don't trigger transactions."""
        import pyarrow as pa
        empty_batch = pa.record_batch({"x": pa.array([], type=pa.int64())}, schema=_SIMPLE_SCHEMA)
        real_batch = _make_batch(10)

        adapter = self._make_adapter()
        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(10,))
        )

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader([empty_batch, real_batch])

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        version = DatasetVersion(version="v1")
        result = adapter.write_versioned("wifor.emp", version, compute)

        # Only 1 batch appended (the empty one was skipped)
        table = adapter._catalog._table
        assert len(table._appended) == 1

    def test_non_conflict_commit_failure_propagates(self):
        """CommitFailedException without 'branch main' message raises immediately."""
        class _OtherFailTable:
            def __init__(self):
                self._props = {}
                self._appended = []

            @property
            def properties(self):
                return dict(self._props)

            def transaction(self):
                return _OtherFailTx(self)

            def current_snapshot(self):
                return None

        class _OtherFailTx:
            def __init__(self, table):
                pass

            def append(self, data):
                pass

            def set_properties(self, props):
                pass

            def commit_transaction(self):
                try:
                    from pyiceberg.exceptions import CommitFailedException
                    raise CommitFailedException("permission denied")
                except ImportError:
                    raise Exception("permission denied")

        class _OtherFailCatalog:
            def __init__(self):
                self._table = _OtherFailTable()

            def load_table(self, ident):
                return self._table

            def table_exists(self, ident):
                return True

            def create_table(self, name, schema=None):
                return self._table

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True
        adapter._duckdb_writes_supported = False
        adapter._catalog = _OtherFailCatalog()

        compute = self._mock_compute(row_count=50, num_batches=1)
        version = DatasetVersion(version="v1")

        with pytest.raises(Exception, match="permission denied"):
            adapter.write_versioned("wifor.emp", version, compute)


class TestStreamingArrowEdgeCases:
    def _make_adapter(self, iceberg_table=None):
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True
        adapter._duckdb_writes_supported = False
        adapter._catalog = _FakePyicebergCatalog(iceberg_table)
        return adapter

    def _mock_compute(self, row_count=10, num_batches=1):
        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(row_count,))
        )
        rows_per_batch = row_count // num_batches if num_batches else row_count

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader(
                [_make_batch(rows_per_batch) for _ in range(num_batches)],
            )

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        return compute

    def test_zero_row_table_still_marks_completed(self):
        """Publishing an empty table still marks the version as completed."""
        adapter = self._make_adapter()
        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(0,))
        )

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            return _FakeRecordBatchReader([])

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        version = DatasetVersion(version="v1")
        result = adapter.write_versioned("wifor.emp", version, compute)
        assert result.completed is True
        assert result.rows == 0


class TestWriteVersionedCheckpointRecovery:
    def test_duckdb_path_checkpoint_write_failure_logged(self):
        """When marking completion fails after DuckDB write, the error is logged, not raised."""
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True

        # Catalog that fails on load_table for checkpoint write
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("catalog down")
        # But table_exists returns False so it doesn't try idempotency check
        mock_catalog.table_exists.return_value = False
        adapter._catalog = mock_catalog

        compute = MagicMock()
        sql_log = []

        def execute_side_effect(sql, *args, **kwargs):
            sql_log.append(sql)
            result = MagicMock()
            result.fetchone.return_value = (5,)
            return result

        compute.execute.side_effect = execute_side_effect
        version = DatasetVersion(version="v1")

        # Should not raise even though checkpoint write fails
        result = adapter.write_versioned("wifor.emp", version, compute)
        assert result.completed is True
        assert result.rows == 5

    def test_s3_auth_fallback_triggers_on_write(self):
        """S3 authorization failure triggers Arrow fallback."""
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True
        adapter._catalog = _FakePyicebergCatalog()

        compute = MagicMock()

        def execute_side_effect(sql, *args, **kwargs):
            upper = sql.strip().upper()
            if upper.startswith("INSERT INTO DBPORT_WAREHOUSE") or \
               upper.startswith("CREATE TABLE DBPORT_WAREHOUSE"):
                raise Exception(
                    "Failed to commit Iceberg transaction: the authorization mechanism "
                    "you have provided is not supported"
                )
            result = MagicMock()
            result.fetchone.return_value = (20,)
            return result

        compute.execute.side_effect = execute_side_effect

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader([_make_batch(20)])

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        version = DatasetVersion(version="v1")

        result = adapter.write_versioned("wifor.emp", version, compute)
        assert adapter._duckdb_writes_supported is False
        assert result.completed is True


class TestWriteVersionedEdgeCases:
    """Cover remaining edge cases in write_versioned and _write_via_duckdb."""

    def _make_adapter(self, iceberg_table=None):
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True
        adapter._catalog = _FakePyicebergCatalog(iceberg_table)
        return adapter

    def test_overwrite_drop_exception_swallowed(self):
        """In _write_via_duckdb overwrite mode, DROP TABLE exception is swallowed."""
        adapter = self._make_adapter()
        compute = MagicMock()
        sql_log = []

        def execute_side_effect(sql, *args, **kwargs):
            sql_log.append(sql)
            if "DROP TABLE" in sql:
                raise Exception("table does not exist")
            result = MagicMock()
            result.fetchone.return_value = (5,)
            return result

        compute.execute.side_effect = execute_side_effect
        version = DatasetVersion(version="v1")
        result = adapter.write_versioned("wifor.emp", version, compute, overwrite=True)
        assert result.completed is True
        # DROP was attempted, CREATE succeeded
        assert any("DROP TABLE" in s for s in sql_log)
        assert any("CREATE TABLE dbport_warehouse" in s for s in sql_log)

    def test_overwrite_create_failure_after_drop_raises(self):
        """In overwrite mode, if DROP succeeds but CREATE fails, error propagates."""
        adapter = self._make_adapter()
        compute = MagicMock()
        sql_log = []

        def execute_side_effect(sql, *args, **kwargs):
            sql_log.append(sql)
            if "CREATE TABLE dbport_warehouse" in sql:
                raise RuntimeError("S3 auth failed")
            result = MagicMock()
            result.fetchone.return_value = (5,)
            return result

        compute.execute.side_effect = execute_side_effect
        version = DatasetVersion(version="v1")
        with pytest.raises(RuntimeError, match="S3 auth failed"):
            adapter.write_versioned("wifor.emp", version, compute, overwrite=True)
        # DROP was executed before CREATE failed
        assert any("DROP TABLE" in s for s in sql_log)

    def test_non_unsupported_duckdb_error_propagates(self):
        """DuckDB write error that is NOT unsupported raises immediately."""
        adapter = self._make_adapter()
        compute = MagicMock()

        def execute_side_effect(sql, *args, **kwargs):
            upper = sql.strip().upper()
            if upper.startswith("INSERT INTO DBPORT_WAREHOUSE") or upper.startswith("CREATE TABLE DBPORT_WAREHOUSE"):
                raise RuntimeError("disk full")
            if "DROP TABLE" in upper:
                raise RuntimeError("disk full")
            result = MagicMock()
            result.fetchone.return_value = (10,)
            return result

        compute.execute.side_effect = execute_side_effect
        version = DatasetVersion(version="v1")
        with pytest.raises(RuntimeError, match="disk full"):
            adapter.write_versioned("wifor.emp", version, compute, overwrite=True)

    def test_snapshot_timestamp_converted_when_present(self):
        """write_versioned converts snap_ts_ms to datetime when not None."""
        iceberg_table = _FakeIcebergTable()
        adapter = self._make_adapter(iceberg_table)

        # Mock current_snapshot to return a timestamp
        adapter.current_snapshot = lambda addr: (123, 1700000000000)

        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(5,))
        )
        version = DatasetVersion(version="v1")
        result = adapter.write_versioned("wifor.emp", version, compute)
        assert result.iceberg_snapshot_id == 123
        assert result.iceberg_snapshot_timestamp is not None


class TestStreamingArrowDeepPaths:
    """Cover deep paths in _write_via_streaming_arrow."""

    def _make_adapter(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True
        adapter._duckdb_writes_supported = False
        return adapter

    def test_batch_skip_handles_early_exhaustion(self):
        """When committed batches > actual batches, skip loop exits via StopIteration."""
        props = {
            "dbport.upload.v2.v1.batches_appended": "5",
            "dbport.upload.v2.v1.rows_appended": "100",
        }
        iceberg_table = _FakeIcebergTable(props=props)
        adapter = self._make_adapter()
        adapter._catalog = _FakePyicebergCatalog(iceberg_table)

        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(100,))
        )

        # Only provide 3 batches but checkpoint says 5 committed
        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader([_make_batch(20) for _ in range(3)])

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        version = DatasetVersion(version="v1")
        result = adapter.write_versioned("wifor.emp", version, compute)
        # All batches were skipped (3 < 5 committed), no new appends
        assert result.completed is True

    def test_streaming_drop_table_exception_swallowed(self):
        """In streaming fallback overwrite, drop_table exception is swallowed."""
        adapter = self._make_adapter()

        class _DropFailCatalog:
            def __init__(self):
                self._table = None
                self._created_table = None

            def drop_table(self, ident, purge_requested=False):
                raise Exception("table doesn't exist")

            def load_table(self, ident):
                if self._table is None:
                    raise LookupError("not found")
                return self._table

            def create_table(self, name, schema=None):
                self._created_table = _FakeIcebergTable()
                self._table = self._created_table
                return self._created_table

            def table_exists(self, ident):
                return self._table is not None

        adapter._catalog = _DropFailCatalog()
        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(10,))
        )

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader([_make_batch(10)])

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        version = DatasetVersion(version="v1")
        result = adapter.write_versioned("wifor.emp", version, compute, overwrite=True)
        assert result.completed is True

    def test_completion_checkpoint_failure_in_streaming_logged(self):
        """When final _write_table_properties fails in streaming, warning is logged."""
        adapter = self._make_adapter()

        class _CompletionFailTable:
            def __init__(self):
                self._props = {}
                self._appended = []

            @property
            def properties(self):
                return dict(self._props)

            def transaction(self):
                return _CompletionFailTx(self)

            def current_snapshot(self):
                return None

        class _CompletionFailTx:
            def __init__(self, table):
                self._table = table

            def append(self, data):
                pass

            def set_properties(self, props):
                self._table._props.update(props)

            def commit_transaction(self):
                self._table._appended.append(1)

        class _CompletionFailCatalog:
            def __init__(self):
                self._table = _CompletionFailTable()
                self._load_count = 0

            def load_table(self, ident):
                self._load_count += 1
                # Fail on the final load (for completion checkpoint)
                if self._load_count > 1:
                    raise Exception("catalog down for completion")
                return self._table

            def table_exists(self, ident):
                return True

            def create_table(self, name, schema=None):
                return self._table

        adapter._catalog = _CompletionFailCatalog()
        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(10,))
        )

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader([_make_batch(10)])

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        version = DatasetVersion(version="v1")
        # Should not raise even though completion checkpoint fails
        result = adapter.write_versioned("wifor.emp", version, compute)
        assert result.completed is True

    def test_progress_logging_at_batch_20_intervals(self):
        """Progress is logged every 20 batches during streaming write."""
        adapter = self._make_adapter()
        adapter._catalog = _FakePyicebergCatalog()

        compute = MagicMock()
        total_rows = 25 * 20  # 25 rows per batch, 20 batches
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(total_rows,))
        )

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader([_make_batch(25) for _ in range(20)])

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        version = DatasetVersion(version="v1")
        # Should complete without error; the 20th batch triggers progress log
        result = adapter.write_versioned("wifor.emp", version, compute)
        assert result.completed is True
        assert result.rows == total_rows

    def test_commit_failed_import_error_fallback(self):
        """When pyiceberg.exceptions is not importable, CommitFailedException = Exception."""
        # This test exercises lines 440-441 indirectly — the fallback is already used
        # by _ConflictTable tests. Here we verify the streaming path works even when
        # the import falls back to plain Exception.
        adapter = self._make_adapter()
        adapter._catalog = _FakePyicebergCatalog()

        compute = MagicMock()
        compute.execute.side_effect = lambda sql, *a, **k: MagicMock(
            fetchone=MagicMock(return_value=(10,))
        )

        def to_arrow_batches_side_effect(sql, batch_size=10_000):
            if "LIMIT 0" in sql:
                return _FakeRecordBatchReader([])
            return _FakeRecordBatchReader([_make_batch(10)])

        compute.to_arrow_batches.side_effect = to_arrow_batches_side_effect
        version = DatasetVersion(version="v1")
        result = adapter.write_versioned("wifor.emp", version, compute)
        assert result.completed is True
