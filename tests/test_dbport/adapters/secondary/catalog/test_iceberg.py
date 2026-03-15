"""Tests for adapters.secondary.catalog.iceberg."""

from __future__ import annotations

import builtins
from unittest.mock import MagicMock

import pytest

from dbport.adapters.secondary.catalog.iceberg import (
    IcebergCatalogAdapter,
    _write_column_docs,
    _write_table_properties,
)
from dbport.infrastructure.credentials import WarehouseCreds


def _make_creds(**overrides) -> WarehouseCreds:
    defaults = dict(
        catalog_uri="https://catalog.example.com",
        catalog_token="tok",
        warehouse="my_wh",
    )
    defaults.update(overrides)
    return WarehouseCreds(**defaults)


class TestIcebergCatalogAdapterInit:
    def test_stores_creds(self):
        creds = _make_creds()
        adapter = IcebergCatalogAdapter(creds)
        assert adapter._creds is creds

    def test_catalog_lazy_before_first_use(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        assert adapter._catalog is None

    def test_warehouse_not_attached_before_first_use(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        assert adapter._warehouse_attached is False

    def test_get_catalog_raises_runtime_error_without_pyiceberg(self, monkeypatch):
        """_get_catalog() raises RuntimeError if pyiceberg is not installed."""
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "pyiceberg" in name:
                raise ImportError("No module named 'pyiceberg'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        adapter = IcebergCatalogAdapter(_make_creds())
        with pytest.raises(RuntimeError, match="pyiceberg"):
            adapter._get_catalog()

    def test_get_catalog_uses_fsspec_file_io(self, monkeypatch):
        """_get_catalog() sets py-io-impl to FsspecFileIO for Supabase S3 compatibility."""
        captured = {}

        def fake_load_catalog(name, **kwargs):
            captured.update(kwargs)
            return object()

        monkeypatch.setattr(
            "pyiceberg.catalog.load_catalog",
            fake_load_catalog,
        )
        adapter = IcebergCatalogAdapter(_make_creds(
            s3_endpoint="https://s3.example.com",
            s3_access_key="key",
            s3_secret_key="secret",
        ))
        adapter._get_catalog()

        assert captured.get("py-io-impl") == "pyiceberg.io.fsspec.FsspecFileIO"

    def test_parse_address_valid(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        assert adapter._parse_address("wifor.foo") == ("wifor", "foo")

    def test_parse_address_invalid_raises(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        with pytest.raises(ValueError, match="Invalid table address"):
            adapter._parse_address("no_dot")


class TestEnsureWarehouseAttached:
    def test_loads_iceberg_extension(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (0,)

        adapter._ensure_warehouse_attached(compute)

        calls = [str(c) for c in compute.execute.call_args_list]
        assert any("LOAD iceberg" in c for c in calls)

    def test_configures_s3_path_style(self):
        adapter = IcebergCatalogAdapter(_make_creds(
            s3_endpoint="https://host.example.com/storage/v1/s3",
            s3_access_key="key",
            s3_secret_key="secret",
            s3_region="eu-west-1",
        ))
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (0,)

        adapter._ensure_warehouse_attached(compute)

        calls = [str(c) for c in compute.execute.call_args_list]
        assert any("s3_url_style" in c for c in calls)
        assert any("s3_endpoint" in c for c in calls)
        assert any("s3_access_key_id" in c for c in calls)
        assert any("s3_secret_access_key" in c for c in calls)
        assert any("s3_region" in c for c in calls)

    def test_creates_secret_and_attaches(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (0,)

        adapter._ensure_warehouse_attached(compute)

        calls = [str(c) for c in compute.execute.call_args_list]
        assert any("SECRET dbport_iceberg_catalog" in c for c in calls)
        assert any("ATTACH" in c for c in calls)

    def test_idempotent_attach(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (0,)

        adapter._ensure_warehouse_attached(compute)
        call_count_1 = compute.execute.call_count

        adapter._ensure_warehouse_attached(compute)
        call_count_2 = compute.execute.call_count

        # Second call should not execute any SQL
        assert call_count_2 == call_count_1

    def test_raises_when_iceberg_unavailable(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        compute = MagicMock()
        compute.execute.side_effect = RuntimeError("extension not found")

        with pytest.raises(RuntimeError, match="iceberg extension is required"):
            adapter._ensure_warehouse_attached(compute)

    def test_token_without_bearer_prefix(self):
        adapter = IcebergCatalogAdapter(_make_creds(catalog_token="my_token"))
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (0,)

        adapter._ensure_warehouse_attached(compute)

        calls = [str(c) for c in compute.execute.call_args_list]
        secret_call = next(c for c in calls if "SECRET" in c)
        assert "Bearer" not in secret_call
        assert "my_token" in secret_call


class TestIngestIntoCompute:
    """ingest_into_compute delegates to _ingest_via_arrow (Arrow is the primary path)."""

    def _make_adapter_with_mock_catalog(self):
        import pyarrow as pa
        arrow_schema = pa.schema([pa.field("id", pa.int32())])
        arrow_table = pa.table({"id": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(arrow_schema, arrow_table.to_batches())
        mock_scan = MagicMock()
        mock_scan.to_arrow_batch_reader.return_value = reader
        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.return_value = mock_scan
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_iceberg_table
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._catalog = mock_catalog
        return adapter, mock_catalog, mock_scan

    def test_uses_arrow_scan(self):
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, mock_scan = self._make_adapter_with_mock_catalog()
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        adapter.ingest_into_compute(InputDeclaration(table_address="estat.foo"), compute)

        mock_scan.to_arrow_batch_reader.assert_called_once()
        compute.register_arrow.assert_called_once()

    def test_applies_filters_as_pyiceberg_expressions(self):
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, mock_scan = self._make_adapter_with_mock_catalog()
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (1,)

        adapter.ingest_into_compute(
            InputDeclaration(table_address="estat.foo", filters={"freq": "A"}),
            compute,
        )

        mock_iceberg_table = adapter._catalog.load_table.return_value
        scan_call = mock_iceberg_table.scan.call_args
        assert scan_call.kwargs.get("row_filter") is not None

    def test_returns_row_count(self):
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, _ = self._make_adapter_with_mock_catalog()
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (42,)

        result = adapter.ingest_into_compute(
            InputDeclaration(table_address="estat.foo"), compute
        )
        assert result == 42

    def test_does_not_use_duckdb_warehouse(self):
        """Arrow path must not touch dbport_warehouse (no ATTACH needed)."""
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, _ = self._make_adapter_with_mock_catalog()
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (0,)

        adapter.ingest_into_compute(InputDeclaration(table_address="estat.foo"), compute)

        calls = [str(c) for c in compute.execute.call_args_list]
        assert not any("dbport_warehouse" in c for c in calls)


class TestIngestViaArrow:
    def _make_adapter_with_mock_catalog(self, monkeypatch, snapshot_id=99):
        """Return an adapter whose pyiceberg catalog is fully mocked."""
        import pyarrow as pa

        arrow_schema = pa.schema([pa.field("id", pa.int32())])
        arrow_table = pa.table({"id": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(arrow_schema, arrow_table.to_batches())

        mock_scan = MagicMock()
        mock_scan.to_arrow_batch_reader.return_value = reader

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.return_value = mock_scan

        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_iceberg_table

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._catalog = mock_catalog
        return adapter, mock_catalog, mock_scan

    def test_uses_pyiceberg_scan_and_registers_reader(self, monkeypatch):
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, mock_scan = self._make_adapter_with_mock_catalog(monkeypatch)
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        adapter._ingest_via_arrow(InputDeclaration(table_address="estat.bar"), compute)

        mock_scan.to_arrow_batch_reader.assert_called_once()
        compute.register_arrow.assert_called_once()
        view_name = compute.register_arrow.call_args[0][0]
        assert view_name == "_dbport_ingest_tmp"

    def test_creates_schema_and_ctas(self, monkeypatch):
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, _ = self._make_adapter_with_mock_catalog(monkeypatch)
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        adapter._ingest_via_arrow(InputDeclaration(table_address="estat.bar"), compute)

        calls = [str(c) for c in compute.execute.call_args_list]
        assert any("CREATE SCHEMA IF NOT EXISTS estat" in c for c in calls)
        assert any("CREATE OR REPLACE TABLE estat.bar" in c for c in calls)
        assert any("_dbport_ingest_tmp" in c for c in calls)

    def test_unregisters_view_after_ctas(self, monkeypatch):
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, _ = self._make_adapter_with_mock_catalog(monkeypatch)
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        adapter._ingest_via_arrow(InputDeclaration(table_address="estat.bar"), compute)

        compute.unregister_arrow.assert_called_once_with("_dbport_ingest_tmp")

    def test_unregisters_even_on_ctas_error(self, monkeypatch):
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, _ = self._make_adapter_with_mock_catalog(monkeypatch)
        compute = MagicMock()

        def _fail_on_ctas(sql, *args, **kwargs):
            if "CREATE OR REPLACE TABLE" in str(sql):
                raise RuntimeError("disk full")
            return MagicMock(fetchone=lambda: (0,))

        compute.execute.side_effect = _fail_on_ctas

        with pytest.raises(RuntimeError, match="disk full"):
            adapter._ingest_via_arrow(InputDeclaration(table_address="estat.bar"), compute)

        compute.unregister_arrow.assert_called_once_with("_dbport_ingest_tmp")

    def test_applies_filters_as_pyiceberg_expressions(self, monkeypatch):
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, mock_scan = self._make_adapter_with_mock_catalog(monkeypatch)
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (1,)

        adapter._ingest_via_arrow(
            InputDeclaration(table_address="estat.bar", filters={"freq": "A"}),
            compute,
        )

        # scan() should have been called with a row_filter (not None)
        mock_iceberg_table = adapter._catalog.load_table.return_value
        scan_call = mock_iceberg_table.scan.call_args
        assert scan_call is not None
        assert scan_call.kwargs.get("row_filter") is not None

    def test_returns_row_count(self, monkeypatch):
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, _ = self._make_adapter_with_mock_catalog(monkeypatch)
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (7,)

        result = adapter._ingest_via_arrow(
            InputDeclaration(table_address="estat.bar"), compute
        )
        assert result == 7

    def test_snapshot_id_passed_to_scan(self, monkeypatch):
        """snapshot_id is forwarded to iceberg_table.scan() (Fix A)."""
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, mock_scan = self._make_adapter_with_mock_catalog(monkeypatch)
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        adapter._ingest_via_arrow(
            InputDeclaration(table_address="estat.bar"),
            compute,
            snapshot_id=999,
        )

        mock_iceberg_table = adapter._catalog.load_table.return_value
        scan_call = mock_iceberg_table.scan.call_args
        assert scan_call.kwargs.get("snapshot_id") == 999

    def test_no_snapshot_id_omits_kwarg(self, monkeypatch):
        """When snapshot_id is None the kwarg is not forwarded to scan()."""
        from dbport.domain.entities.input import InputDeclaration

        adapter, _, _ = self._make_adapter_with_mock_catalog(monkeypatch)
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        adapter._ingest_via_arrow(InputDeclaration(table_address="estat.bar"), compute)

        mock_iceberg_table = adapter._catalog.load_table.return_value
        scan_call = mock_iceberg_table.scan.call_args
        assert "snapshot_id" not in (scan_call.kwargs or {})


class TestIsTransientS3Error:
    """_is_transient_s3_error classifies S3/network errors for retry decisions."""

    @pytest.mark.parametrize("msg", [
        "An error occurred (InvalidKey) when calling GetObject",
        "OSError: [Errno 5] An error occurred (InvalidKey)",
        "NoSuchKey: the specified key does not exist",
        "InvalidAccessKeyId: the key is invalid",
        "RequestTimeout: your socket connection timed out",
        "SlowDown: please reduce your request rate",
        "ServiceUnavailable: service is unavailable",
        "InternalError: we encountered an internal error",
        "Connection reset by peer",
        "Connection aborted",
        "Broken pipe",
        "IOError: failed to read file",
        "OSError: network unreachable",
        "HTTP 500 Internal Server Error",
        "HTTP 502 Bad Gateway",
        "HTTP 503 Service Unavailable",
        "HTTP 429 Too Many Requests",
    ])
    def test_transient_errors_detected(self, msg):
        exc = Exception(msg)
        assert IcebergCatalogAdapter._is_transient_s3_error(exc) is True

    @pytest.mark.parametrize("msg", [
        "Table not found in catalog",
        "Invalid table address",
        "Schema mismatch: expected int got string",
        "disk full",
        "Permission denied",
    ])
    def test_non_transient_errors_not_matched(self, msg):
        exc = Exception(msg)
        assert IcebergCatalogAdapter._is_transient_s3_error(exc) is False


class TestIngestRetry:
    """_ingest_via_arrow retries on transient S3 errors."""

    def _make_adapter_with_failing_scan(self, fail_count: int):
        """Return adapter where scan.to_arrow_batch_reader() fails N times then succeeds."""
        import pyarrow as pa

        arrow_schema = pa.schema([pa.field("id", pa.int32())])
        arrow_table = pa.table({"id": [1, 2, 3]})

        call_count = [0]

        def make_scan():
            mock_scan = MagicMock()

            def to_reader():
                call_count[0] += 1
                if call_count[0] <= fail_count:
                    raise OSError(
                        "[Errno 5] An error occurred (InvalidKey) when calling "
                        "the GetObject operation: Invalid key: "
                        "data/last_updated=2026-02-10T22%3A00%3A00/file.parquet"
                    )
                return pa.RecordBatchReader.from_batches(
                    arrow_schema, arrow_table.to_batches()
                )

            mock_scan.to_arrow_batch_reader = to_reader
            return mock_scan

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.side_effect = lambda **kw: make_scan()

        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_iceberg_table

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._catalog = mock_catalog
        # Speed up tests: no actual backoff
        adapter._INGEST_RETRY_BACKOFF = (0, 0, 0)
        return adapter, call_count

    def test_retries_on_transient_s3_error(self):
        from dbport.domain.entities.input import InputDeclaration

        adapter, call_count = self._make_adapter_with_failing_scan(fail_count=2)
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        result = adapter._ingest_via_arrow(
            InputDeclaration(table_address="estat.bar"), compute
        )
        assert result == 3
        assert call_count[0] == 3  # 2 failures + 1 success

    def test_gives_up_after_max_retries(self):
        from dbport.domain.entities.input import InputDeclaration

        adapter, call_count = self._make_adapter_with_failing_scan(fail_count=5)
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        with pytest.raises(OSError, match="InvalidKey"):
            adapter._ingest_via_arrow(
                InputDeclaration(table_address="estat.bar"), compute
            )
        assert call_count[0] == 3  # tried 3 times (max retries)

    def test_non_transient_error_not_retried(self):
        """Non-transient errors propagate immediately."""
        from dbport.domain.entities.input import InputDeclaration

        import pyarrow as pa

        arrow_schema = pa.schema([pa.field("id", pa.int32())])

        mock_scan = MagicMock()
        mock_scan.to_arrow_batch_reader.side_effect = RuntimeError("disk full")

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.return_value = mock_scan

        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_iceberg_table

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._catalog = mock_catalog
        adapter._INGEST_RETRY_BACKOFF = (0, 0, 0)

        compute = MagicMock()

        with pytest.raises(RuntimeError, match="disk full"):
            adapter._ingest_via_arrow(
                InputDeclaration(table_address="estat.bar"), compute
            )
        # Should have been called only once — no retry
        mock_scan.to_arrow_batch_reader.assert_called_once()


class TestResolveInputSnapshot:
    """Fix C: version resolution from dbport.metadata_json table properties."""

    def _make_adapter_with_metadata(self, metadata_json: str) -> IcebergCatalogAdapter:
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.properties = {"dbport.metadata_json": metadata_json}
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog
        # Stub get_table_property to read from our mock
        import json
        meta = json.loads(metadata_json)

        def _get_prop(table_address, key):
            return mock_table.properties.get(key)

        adapter.get_table_property = _get_prop
        return adapter

    def _meta_json(self, latest: str, versions: list) -> str:
        import json
        return json.dumps({
            "last_updated_data_at": latest,
            "versions": versions,
        })

    def test_returns_none_none_for_table_without_dbport_metadata(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter.get_table_property = lambda addr, key: None
        result = adapter.resolve_input_snapshot("wifor.foo", None)
        assert result == (None, None)

    def test_returns_latest_version_and_snapshot(self):
        meta = self._meta_json(
            latest="2026-03-09",
            versions=[
                {"version": "2025-01-01", "iceberg_snapshot_id": 10},
                {"version": "2026-03-09", "iceberg_snapshot_id": 99},
            ],
        )
        adapter = self._make_adapter_with_metadata(meta)
        version, snap = adapter.resolve_input_snapshot("wifor.foo", None)
        assert version == "2026-03-09"
        assert snap == 99

    def test_returns_specific_version_and_snapshot(self):
        meta = self._meta_json(
            latest="2026-03-09",
            versions=[
                {"version": "2025-01-01", "iceberg_snapshot_id": 10},
                {"version": "2026-03-09", "iceberg_snapshot_id": 99},
            ],
        )
        adapter = self._make_adapter_with_metadata(meta)
        version, snap = adapter.resolve_input_snapshot("wifor.foo", "2025-01-01")
        assert version == "2025-01-01"
        assert snap == 10

    def test_raises_for_unknown_specific_version(self):
        meta = self._meta_json(
            latest="2026-03-09",
            versions=[{"version": "2026-03-09", "iceberg_snapshot_id": 99}],
        )
        adapter = self._make_adapter_with_metadata(meta)
        with pytest.raises(ValueError, match="not found"):
            adapter.resolve_input_snapshot("wifor.foo", "2020-01-01")

    def test_returns_latest_version_without_snapshot_when_not_in_versions_list(self):
        """Pre-fix tables have last_updated_data_at but empty versions list."""
        meta = self._meta_json(latest="2026-03-09", versions=[])
        adapter = self._make_adapter_with_metadata(meta)
        version, snap = adapter.resolve_input_snapshot("wifor.foo", None)
        assert version == "2026-03-09"
        assert snap is None

    def test_malformed_metadata_returns_none_none(self):
        adapter = IcebergCatalogAdapter(_make_creds())
        adapter.get_table_property = lambda addr, key: "not valid json{"
        result = adapter.resolve_input_snapshot("wifor.foo", None)
        assert result == (None, None)


class TestCatalogConnectionFailures:
    def test_table_exists_returns_false_on_exception(self):
        """table_exists() swallows exceptions and returns False."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_catalog.table_exists.side_effect = ConnectionError("refused")
        adapter._catalog = mock_catalog
        assert adapter.table_exists("wifor.foo") is False

    def test_load_arrow_schema_propagates_on_missing_table(self):
        """load_arrow_schema() lets LookupError propagate."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = LookupError("not found")
        adapter._catalog = mock_catalog
        with pytest.raises(LookupError):
            adapter.load_arrow_schema("wifor.foo")

    def test_current_snapshot_returns_none_on_load_failure(self):
        """current_snapshot() returns (None, None) when load_table fails."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("network error")
        adapter._catalog = mock_catalog
        assert adapter.current_snapshot("wifor.foo") == (None, None)

    def test_current_snapshot_returns_none_when_no_snapshot(self):
        """current_snapshot() returns (None, None) when table has no snapshot."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.current_snapshot.return_value = None
        mock_table.metadata = MagicMock(current_snapshot_id=None)
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog
        assert adapter.current_snapshot("wifor.foo") == (None, None)

    def test_current_snapshot_falls_back_to_metadata_snapshot_id(self):
        """current_snapshot() reads metadata.current_snapshot_id when current_snapshot() fails."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.current_snapshot.side_effect = Exception("snap error")
        mock_table.metadata.current_snapshot_id = 42
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog
        snap_id, snap_ts = adapter.current_snapshot("wifor.foo")
        assert snap_id == 42
        assert snap_ts is None

    def test_current_snapshot_returns_snapshot_id_and_timestamp(self):
        """current_snapshot() returns both id and timestamp_ms from snapshot object."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_snap = MagicMock()
        mock_snap.snapshot_id = 123
        mock_snap.timestamp_ms = 1700000000000
        mock_table.current_snapshot.return_value = mock_snap
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog
        snap_id, snap_ts = adapter.current_snapshot("wifor.foo")
        assert snap_id == 123
        assert snap_ts == 1700000000000

    def test_get_table_property_returns_none_on_exception(self):
        """get_table_property() returns None when catalog raises."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("gone")
        adapter._catalog = mock_catalog
        assert adapter.get_table_property("wifor.foo", "any.key") is None

    def test_get_table_property_returns_value(self):
        """get_table_property() returns the value from table properties."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.properties = {"dbport.key": "value"}
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog
        assert adapter.get_table_property("wifor.foo", "dbport.key") == "value"

    def test_update_table_properties_delegates(self):
        """update_table_properties() delegates to _write_table_properties."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_tx = MagicMock()
        mock_table.transaction.return_value = mock_tx
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog
        adapter.update_table_properties("wifor.foo", {"k": "v"})
        mock_tx.set_properties.assert_called_once_with({"k": "v"})
        mock_tx.commit_transaction.assert_called_once()

    def test_update_column_docs_delegates(self):
        """update_column_docs() delegates to _write_column_docs."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_update = MagicMock()
        mock_table.update_schema.return_value.__enter__ = lambda self: mock_update
        mock_table.update_schema.return_value.__exit__ = MagicMock(return_value=False)
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog
        adapter.update_column_docs("wifor.foo", {"geo": "doc text"})
        mock_update.update_column.assert_called_once_with("geo", doc="doc text")


class TestEnsureWarehouseAttachedEdgeCases:
    def test_load_fails_install_then_load_succeeds(self):
        """When LOAD iceberg fails, tries INSTALL + LOAD."""
        adapter = IcebergCatalogAdapter(_make_creds())
        compute = MagicMock()
        call_count = [0]

        def execute_side_effect(sql, *args, **kwargs):
            call_count[0] += 1
            if sql == "LOAD iceberg" and call_count[0] == 1:
                raise RuntimeError("not found")
            result = MagicMock()
            result.fetchone.return_value = (0,)
            return result

        compute.execute.side_effect = execute_side_effect
        adapter._ensure_warehouse_attached(compute)
        calls = [str(c) for c in compute.execute.call_args_list]
        assert any("INSTALL iceberg" in c for c in calls)

    def test_already_attached_warehouse_skips_attach(self):
        """When warehouse already in duckdb_databases, ATTACH is skipped."""
        adapter = IcebergCatalogAdapter(_make_creds())
        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (1,)  # already attached
        adapter._ensure_warehouse_attached(compute)
        calls = [str(c) for c in compute.execute.call_args_list]
        assert not any("ATTACH" in c for c in calls)


class TestWriteTableProperties:
    def test_uses_transaction_api(self):
        """Prefers transaction -> set_properties -> commit_transaction."""
        mock_table = MagicMock()
        mock_tx = MagicMock()
        mock_table.transaction.return_value = mock_tx
        _write_table_properties(mock_table, {"k": "v"})
        mock_tx.set_properties.assert_called_once_with({"k": "v"})
        mock_tx.commit_transaction.assert_called_once()

    def test_falls_back_to_update_properties(self):
        """When no transaction API, uses update_properties context manager."""
        mock_table = MagicMock(spec=[])  # no attributes by default
        mock_updater = MagicMock()
        mock_table.update_properties = MagicMock(return_value=mock_updater)
        mock_updater.__enter__ = MagicMock(return_value=mock_updater)
        mock_updater.__exit__ = MagicMock(return_value=False)
        _write_table_properties(mock_table, {"k": "v"})
        mock_updater.set.assert_called_once_with("k", "v")

    def test_raises_when_no_api_available(self):
        """Raises RuntimeError when neither API is available."""
        mock_table = object()  # no transaction or update_properties
        with pytest.raises(RuntimeError, match="does not support writing table properties"):
            _write_table_properties(mock_table, {"k": "v"})


class TestWriteColumnDocs:
    def test_calls_update_schema_with_docs(self):
        mock_table = MagicMock()
        mock_update = MagicMock()
        mock_table.update_schema.return_value.__enter__ = lambda self: mock_update
        mock_table.update_schema.return_value.__exit__ = MagicMock(return_value=False)
        _write_column_docs(mock_table, {"geo": "Geographic area"})
        mock_update.update_column.assert_called_once_with("geo", doc="Geographic area")

    def test_raises_when_no_update_schema(self):
        mock_table = object()
        with pytest.raises(RuntimeError, match="does not support schema updates"):
            _write_column_docs(mock_table, {"geo": "doc"})


class TestLoadArrowSchema:
    def test_returns_arrow_schema(self):
        """load_arrow_schema returns the PyArrow schema from the warehouse table."""
        import pyarrow as pa

        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_schema = MagicMock()
        expected_arrow = pa.schema([pa.field("x", pa.int64())])
        mock_schema.as_arrow.return_value = expected_arrow
        mock_table = MagicMock()
        mock_table.schema.return_value = mock_schema
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog
        result = adapter.load_arrow_schema("wifor.foo")
        assert result == expected_arrow


class TestIngestViaArrowImportError:
    def test_raises_runtime_error_without_pyiceberg(self, monkeypatch):
        """_ingest_via_arrow raises RuntimeError if pyiceberg is not installed."""
        import builtins as _builtins
        from dbport.domain.entities.input import InputDeclaration

        real_import = _builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "pyiceberg" in name:
                raise ImportError("No module named 'pyiceberg'")
            return real_import(name, *args, **kwargs)

        adapter = IcebergCatalogAdapter(_make_creds())
        monkeypatch.setattr(_builtins, "__import__", mock_import)
        with pytest.raises(RuntimeError, match="pyiceberg is required"):
            adapter._ingest_via_arrow(InputDeclaration(table_address="estat.bar"), MagicMock())


class TestResolveInputSnapshotNoLastUpdated:
    def test_returns_none_none_when_last_updated_data_at_missing(self):
        """Metadata exists but lacks last_updated_data_at → (None, None)."""
        import json

        adapter = IcebergCatalogAdapter(_make_creds())
        # Metadata with versions but no last_updated_data_at
        adapter.get_table_property = lambda addr, key: json.dumps({"versions": []})
        result = adapter.resolve_input_snapshot("wifor.foo", None)
        assert result == (None, None)


class TestCurrentSnapshotMetadataFallbackException:
    def test_returns_none_none_when_metadata_access_raises(self):
        """current_snapshot() returns (None, None) when metadata fallback also throws."""
        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        # current_snapshot() raises
        mock_table.current_snapshot.side_effect = Exception("snap error")
        # metadata access also raises (property that throws)
        type(mock_table).metadata = property(lambda self: (_ for _ in ()).throw(Exception("meta broken")))
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog
        assert adapter.current_snapshot("wifor.foo") == (None, None)


class TestStreamingArrowCommitFailedImportError:
    def test_fallback_when_pyiceberg_exceptions_unavailable(self, monkeypatch):
        """_write_via_streaming_arrow falls back to Exception when CommitFailedException is not importable."""
        import builtins as _builtins

        real_import = _builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "pyiceberg.exceptions":
                raise ImportError("No module named 'pyiceberg.exceptions'")
            return real_import(name, *args, **kwargs)

        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        ns_table = MagicMock()
        ns_table.properties = {}
        mock_catalog.load_table.return_value = ns_table
        adapter._catalog = mock_catalog

        monkeypatch.setattr(_builtins, "__import__", mock_import)

        from dbport.domain.entities.version import DatasetVersion

        version = DatasetVersion(version="2026-03-09")
        mock_compute = MagicMock()
        import pyarrow as pa
        empty_reader = pa.RecordBatchReader.from_batches(
            pa.schema([pa.field("x", pa.int32())]),
            [],
        )
        mock_compute.to_arrow_batches.return_value = empty_reader

        # Should execute without ImportError — the fallback assignment works
        # With empty batches the method completes without writing anything
        adapter._write_via_streaming_arrow(
            "wifor.emp", version, mock_compute, overwrite=False, total_rows=0
        )
        # If we reach here, the import fallback worked correctly


class TestProgressCallbackIntegration:
    """Test that progress callbacks fire correctly during ingest and write."""

    def test_ingest_fires_progress_callbacks(self):
        """_ingest_via_arrow calls started/finished on the progress callback."""
        import pyarrow as pa

        from dbport.domain.entities.input import InputDeclaration
        from dbport.infrastructure.progress import progress_callback

        arrow_schema = pa.schema([pa.field("id", pa.int32())])
        arrow_table = pa.table({"id": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(arrow_schema, arrow_table.to_batches())

        mock_scan = MagicMock()
        mock_scan.to_arrow_batch_reader.return_value = reader
        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.return_value = mock_scan
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_iceberg_table

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._catalog = mock_catalog

        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            adapter._ingest_via_arrow(
                InputDeclaration(table_address="estat.bar"), compute
            )
        finally:
            progress_callback.reset(token)

        cb.started.assert_called_once()
        assert "estat.bar" in cb.started.call_args[0][0]
        cb.finished.assert_called_once()
        assert "3" in cb.finished.call_args[0][0]

    def test_ingest_retry_fires_failed_callback(self):
        """On transient error, the progress callback's failed() is called."""
        import pyarrow as pa

        from dbport.domain.entities.input import InputDeclaration
        from dbport.infrastructure.progress import progress_callback

        arrow_schema = pa.schema([pa.field("id", pa.int32())])
        arrow_table = pa.table({"id": [1, 2, 3]})
        call_count = [0]

        def make_scan():
            mock_scan = MagicMock()
            def to_reader():
                call_count[0] += 1
                if call_count[0] <= 1:
                    raise OSError("InvalidKey: transient error")
                return pa.RecordBatchReader.from_batches(
                    arrow_schema, arrow_table.to_batches()
                )
            mock_scan.to_arrow_batch_reader = to_reader
            return mock_scan

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.side_effect = lambda **kw: make_scan()
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_iceberg_table

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._catalog = mock_catalog
        adapter._INGEST_RETRY_BACKOFF = (0, 0, 0)

        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            adapter._ingest_via_arrow(
                InputDeclaration(table_address="estat.bar"), compute
            )
        finally:
            progress_callback.reset(token)

        # First attempt: started + failed; second attempt: started + finished
        assert cb.started.call_count == 2
        cb.failed.assert_called_once()
        assert "retrying" in cb.failed.call_args[0][0]

    def test_ingest_final_failure_calls_finished_not_failed(self):
        """When all retries exhausted, cb.finished() is called (not failed)."""
        from dbport.domain.entities.input import InputDeclaration
        from dbport.infrastructure.progress import progress_callback

        mock_scan = MagicMock()
        mock_scan.to_arrow_batch_reader.side_effect = OSError("InvalidKey: persistent")
        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.return_value = mock_scan
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_iceberg_table

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._catalog = mock_catalog
        adapter._INGEST_RETRY_BACKOFF = (0, 0, 0)

        compute = MagicMock()

        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            with pytest.raises(OSError):
                adapter._ingest_via_arrow(
                    InputDeclaration(table_address="estat.bar"), compute
                )
        finally:
            progress_callback.reset(token)

        # Last attempt calls finished() (not failed) before raising
        assert cb.finished.call_count >= 1

    def test_streaming_arrow_fires_progress_callbacks(self):
        """_write_via_streaming_arrow calls started/update/finished."""
        import pyarrow as pa

        from dbport.domain.entities.version import DatasetVersion
        from dbport.infrastructure.progress import progress_callback

        arrow_schema = pa.schema([pa.field("x", pa.int32())])
        batches = [pa.record_batch({"x": list(range(10))}, schema=arrow_schema)]
        reader = pa.RecordBatchReader.from_batches(arrow_schema, batches)

        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.properties = {}
        mock_catalog.load_table.return_value = mock_table
        mock_catalog.create_table.return_value = mock_table

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._catalog = mock_catalog

        mock_compute = MagicMock()
        mock_compute.to_arrow_batches.return_value = reader

        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            adapter._write_via_streaming_arrow(
                "wifor.emp", DatasetVersion(version="2026-03-15"),
                mock_compute, overwrite=False, total_rows=10,
            )
        finally:
            progress_callback.reset(token)

        cb.started.assert_called_once()
        assert "wifor.emp" in cb.started.call_args[0][0]
        cb.update.assert_called()
        cb.finished.assert_called_once()
        assert "wifor.emp" in cb.finished.call_args[0][0]

    def test_write_versioned_already_completed_fires_log(self):
        """write_versioned logs via cb.log() when version already completed."""
        from dbport.domain.entities.version import DatasetVersion
        from dbport.infrastructure.progress import progress_callback

        adapter = IcebergCatalogAdapter(_make_creds())
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.properties = {
            "dbport.upload.v2.2026-03-15.completed": "1",
            "dbport.upload.v2.2026-03-15.rows_appended": "100",
        }
        mock_snap = MagicMock()
        mock_snap.snapshot_id = 42
        mock_snap.timestamp_ms = 1700000000000
        mock_table.current_snapshot.return_value = mock_snap
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog

        mock_compute = MagicMock()

        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            result = adapter.write_versioned(
                "wifor.emp",
                DatasetVersion(version="2026-03-15"),
                mock_compute,
                overwrite=False,
            )
        finally:
            progress_callback.reset(token)

        cb.log.assert_called_once()
        assert "already completed" in cb.log.call_args[0][0]
        assert result.completed is True
        assert result.rows == 100

    def test_write_versioned_duckdb_fallback_fires_failed_and_log(self):
        """write_versioned calls cb.log and cb.failed on DuckDB→Arrow fallback."""
        import pyarrow as pa

        from dbport.domain.entities.version import DatasetVersion
        from dbport.infrastructure.progress import progress_callback

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True

        # DuckDB write fails with 404 (unsupported)
        mock_compute = MagicMock()
        mock_compute.execute.side_effect = [
            # COUNT(*) query
            MagicMock(fetchone=MagicMock(return_value=(5,))),
            # DuckDB write attempt — fails
            RuntimeError("transactions/commit returned 404 Not Found"),
        ]

        # Arrow fallback path
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.properties = {}
        mock_snap = MagicMock()
        mock_snap.snapshot_id = 99
        mock_snap.timestamp_ms = 1700000000000
        mock_table.current_snapshot.return_value = mock_snap
        mock_catalog.load_table.return_value = mock_table
        mock_catalog.create_table.return_value = mock_table
        adapter._catalog = mock_catalog

        # Empty reader for streaming fallback
        arrow_schema = pa.schema([pa.field("x", pa.int32())])
        reader = pa.RecordBatchReader.from_batches(arrow_schema, [])
        mock_compute.to_arrow_batches.return_value = reader

        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            adapter.write_versioned(
                "wifor.emp",
                DatasetVersion(version="2026-03-15"),
                mock_compute,
                overwrite=False,
            )
        finally:
            progress_callback.reset(token)

        cb.log.assert_called_once_with("Switching to streaming Arrow fallback")
        cb.failed.assert_called_once()
        assert "Arrow fallback" in cb.failed.call_args[0][0]

    def test_ingest_retry_without_failed_method_calls_finished(self):
        """When cb has no failed() method, retry calls cb.finished() instead."""
        import pyarrow as pa

        from dbport.domain.entities.input import InputDeclaration
        from dbport.infrastructure.progress import progress_callback

        arrow_schema = pa.schema([pa.field("id", pa.int32())])
        arrow_table = pa.table({"id": [1, 2, 3]})
        call_count = [0]

        def make_scan():
            mock_scan = MagicMock()
            def to_reader():
                call_count[0] += 1
                if call_count[0] <= 1:
                    raise OSError("InvalidKey: transient error")
                return pa.RecordBatchReader.from_batches(
                    arrow_schema, arrow_table.to_batches()
                )
            mock_scan.to_arrow_batch_reader = to_reader
            return mock_scan

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.side_effect = lambda **kw: make_scan()
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_iceberg_table

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._catalog = mock_catalog
        adapter._INGEST_RETRY_BACKOFF = (0, 0, 0)

        compute = MagicMock()
        compute.execute.return_value.fetchone.return_value = (3,)

        # Use a callback WITHOUT a failed() method
        cb = MagicMock(spec=["started", "update", "log", "finished"])
        token = progress_callback.set(cb)
        try:
            adapter._ingest_via_arrow(
                InputDeclaration(table_address="estat.bar"), compute
            )
        finally:
            progress_callback.reset(token)

        # On retry, finished() should be called (since no failed() exists)
        assert cb.finished.call_count >= 1

    def test_write_versioned_duckdb_success_fires_finished(self):
        """write_versioned calls cb.finished() on DuckDB success path."""
        from dbport.domain.entities.version import DatasetVersion
        from dbport.infrastructure.progress import progress_callback

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True

        # Patch _write_via_duckdb to succeed
        adapter._write_via_duckdb = MagicMock()

        mock_compute = MagicMock()
        mock_compute.execute.return_value.fetchone.return_value = (5,)

        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.properties = {}
        mock_snap = MagicMock()
        mock_snap.snapshot_id = 99
        mock_snap.timestamp_ms = 1700000000000
        mock_table.current_snapshot.return_value = mock_snap
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog

        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            result = adapter.write_versioned(
                "wifor.emp",
                DatasetVersion(version="2026-03-15"),
                mock_compute,
                overwrite=False,
            )
        finally:
            progress_callback.reset(token)

        cb.started.assert_called_once()
        cb.finished.assert_called_once()
        assert "Published" in cb.finished.call_args[0][0]
        assert result.rows == 5

    def test_write_versioned_duckdb_fallback_without_failed_method(self):
        """When cb has no failed(), DuckDB fallback calls cb.finished() instead."""
        import pyarrow as pa

        from dbport.domain.entities.version import DatasetVersion
        from dbport.infrastructure.progress import progress_callback

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True

        adapter._write_via_duckdb = MagicMock(
            side_effect=RuntimeError("transactions/commit returned 404 Not Found")
        )

        mock_compute = MagicMock()
        mock_compute.execute.return_value.fetchone.return_value = (5,)

        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.properties = {}
        mock_snap = MagicMock()
        mock_snap.snapshot_id = 99
        mock_snap.timestamp_ms = 1700000000000
        mock_table.current_snapshot.return_value = mock_snap
        mock_catalog.load_table.return_value = mock_table
        mock_catalog.create_table.return_value = mock_table
        adapter._catalog = mock_catalog

        arrow_schema = pa.schema([pa.field("x", pa.int32())])
        reader = pa.RecordBatchReader.from_batches(arrow_schema, [])
        mock_compute.to_arrow_batches.return_value = reader

        # Use a callback WITHOUT a failed() method
        cb = MagicMock(spec=["started", "update", "log", "finished"])
        token = progress_callback.set(cb)
        try:
            adapter.write_versioned(
                "wifor.emp",
                DatasetVersion(version="2026-03-15"),
                mock_compute,
                overwrite=False,
            )
        finally:
            progress_callback.reset(token)

        cb.log.assert_called_once_with("Switching to streaming Arrow fallback")
        # Without failed(), falls back to finished()
        assert any(
            "Arrow fallback" in str(c) for c in cb.finished.call_args_list
        )

    def test_write_versioned_duckdb_non_fallback_error_calls_finished(self):
        """write_versioned calls cb.finished() on non-fallback DuckDB errors."""
        from dbport.domain.entities.version import DatasetVersion
        from dbport.infrastructure.progress import progress_callback

        adapter = IcebergCatalogAdapter(_make_creds())
        adapter._warehouse_attached = True

        # Patch _write_via_duckdb to raise a non-fallback error
        adapter._write_via_duckdb = MagicMock(
            side_effect=RuntimeError("disk full — not a fallback error")
        )

        mock_compute = MagicMock()
        mock_compute.execute.return_value.fetchone.return_value = (5,)

        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.properties = {}
        mock_catalog.load_table.return_value = mock_table
        adapter._catalog = mock_catalog

        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            with pytest.raises(RuntimeError, match="disk full"):
                adapter.write_versioned(
                    "wifor.emp",
                    DatasetVersion(version="2026-03-15"),
                    mock_compute,
                    overwrite=False,
                )
        finally:
            progress_callback.reset(token)

        cb.started.assert_called_once()
        cb.finished.assert_called_once_with()
