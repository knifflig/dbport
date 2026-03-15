"""Tests for application.services.publish."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from dbport.application.services.publish import PublishService
from dbport.domain.entities.dataset import Dataset
from dbport.domain.entities.schema import ColumnDef, DatasetSchema, SqlDdl
from dbport.domain.entities.version import DatasetVersion, VersionRecord

_NOW = datetime(2026, 3, 9, 14, 0, 0, tzinfo=UTC)
_DDL = "CREATE OR REPLACE TABLE wifor.emp (geo VARCHAR, year SMALLINT)"


def _make_schema() -> DatasetSchema:
    return DatasetSchema(
        ddl=SqlDdl(statement=_DDL),
        columns=(
            ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),
            ColumnDef(name="year", pos=1, sql_type="SMALLINT"),
        ),
    )


def _make_version_record(version="2026-03-09", completed=True) -> VersionRecord:
    return VersionRecord(
        version=version,
        published_at=_NOW,
        rows=100,
        completed=completed,
    )


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeCatalog:
    def __init__(self, table_exists=False):
        self._table_exists = table_exists
        self.written_versions: list[str] = []
        self.last_overwrite: bool = False
        self.properties: dict[str, str] = {}
        self.column_docs: dict[str, str] = {}

    def table_exists(self, table_address):
        return self._table_exists

    def load_arrow_schema(self, table_address):
        import pyarrow as pa
        return pa.schema([pa.field("geo", pa.string()), pa.field("year", pa.int16())])

    def current_snapshot(self, table_address):
        return 999, None

    def scan_to_arrow_batches(self, declaration):
        return iter([])

    def get_table_property(self, table_address, key):
        return self.properties.get(key)

    def write_versioned(self, table_address, version, compute, overwrite=False):
        self.written_versions.append(version.version)
        self.last_overwrite = overwrite
        return _make_version_record(version=version.version)

    def update_table_properties(self, table_address, properties):
        self.properties.update(properties)

    def update_column_docs(self, table_address, column_docs):
        self.column_docs.update(column_docs)


class _FakeCompute:
    def __init__(self):
        import pyarrow as pa
        self._table = pa.table({"geo": pa.array(["DE"], pa.string()), "year": pa.array([2020], pa.int16())})

    def execute(self, sql, parameters=None):
        return _FakeResult()

    def execute_file(self, path):
        pass

    def relation_exists(self, schema, table):
        return True

    def to_arrow_batches(self, sql, batch_size=10_000):
        return self._table.to_reader()

    def ensure_extensions(self):
        pass

    def close(self):
        pass


class _FakeResult:
    def fetchone(self):
        return None

    def fetchall(self):
        return []

    @property
    def schema_arrow(self):
        import pyarrow as pa
        return pa.schema([pa.field("geo", pa.string()), pa.field("year", pa.int16())])


class _FakeLock:
    def __init__(self, schema=None):
        self._schema = schema or _make_schema()
        self._ingest_records = []
        self._versions: list[VersionRecord] = []
        self._codelist_entries = {}

    def read_schema(self):
        return self._schema

    def write_schema(self, schema):
        self._schema = schema

    def read_codelist_entries(self):
        return self._codelist_entries

    def write_codelist_entry(self, entry):
        self._codelist_entries[entry.column_name] = entry

    def read_ingest_records(self):
        return self._ingest_records

    def write_ingest_record(self, record):
        self._ingest_records.append(record)

    def read_versions(self):
        return list(self._versions)

    def append_version(self, record):
        self._versions.append(record)


class _FakeMetadata:
    def __init__(self, tmp_path: Path):
        self._tmp_path = tmp_path

    def build_metadata_json(self, key, version, inputs, codelists, previous_metadata_json=None, snapshot_id=None):
        return b'{"schema_version":1}\n'

    def generate_codelist_bytes(self, codelists, compute, output_table):
        return {}

    def attach_to_table(self, table_address, metadata_bytes, codelist_bytes, codelist_entries, catalog):
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPublishServiceNoSchema:
    def test_raises_when_no_schema(self, tmp_path: Path):
        dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        lock = _FakeLock(schema=None)
        lock._schema = None  # override

        svc = PublishService(
            dataset, _FakeCatalog(), _FakeCompute(), lock, _FakeMetadata(tmp_path)
        )
        with pytest.raises(RuntimeError, match="No schema"):
            svc.execute(DatasetVersion(version="2026-03-09"))


class TestPublishServiceIdempotent:
    def test_skips_completed_version(self, tmp_path: Path):
        dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        lock = _FakeLock()
        lock._versions = [_make_version_record(version="2026-03-09", completed=True)]
        catalog = _FakeCatalog()

        svc = PublishService(
            dataset, catalog, _FakeCompute(), lock, _FakeMetadata(tmp_path)
        )
        result = svc.execute(DatasetVersion(version="2026-03-09"))

        # Should return the existing record without calling write_versioned
        assert result.version == "2026-03-09"
        assert catalog.written_versions == []


class TestPublishServiceIdempotentProgressCallback:
    def test_skip_fires_progress_callback(self, tmp_path: Path):
        """progress_callback.log is called when version already completed."""
        from dbport.infrastructure.progress import progress_callback

        dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        lock = _FakeLock()
        lock._versions = [_make_version_record(version="2026-03-09", completed=True)]

        svc = PublishService(
            dataset, _FakeCatalog(), _FakeCompute(), lock, _FakeMetadata(tmp_path)
        )

        logged: list[str] = []

        class _CB:
            def log(self, msg: str) -> None:
                logged.append(msg)

        token = progress_callback.set(_CB())
        try:
            svc.execute(DatasetVersion(version="2026-03-09"))
        finally:
            progress_callback.reset(token)

        assert any("already completed" in m for m in logged)


class TestPublishServiceFullFlow:
    def test_calls_write_versioned(self, tmp_path: Path):
        dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        catalog = _FakeCatalog()
        svc = PublishService(
            dataset, catalog, _FakeCompute(), _FakeLock(), _FakeMetadata(tmp_path)
        )
        svc.execute(DatasetVersion(version="2026-03-09"))
        assert "2026-03-09" in catalog.written_versions

    def test_appends_version_record_to_lock(self, tmp_path: Path):
        dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        lock = _FakeLock()
        catalog = _FakeCatalog()
        svc = PublishService(
            dataset, catalog, _FakeCompute(), lock, _FakeMetadata(tmp_path)
        )
        svc.execute(DatasetVersion(version="2026-03-09"))
        versions = lock.read_versions()
        assert len(versions) == 1
        assert versions[0].version == "2026-03-09"

    def test_returns_version_record(self, tmp_path: Path):
        dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        svc = PublishService(
            dataset, _FakeCatalog(), _FakeCompute(), _FakeLock(), _FakeMetadata(tmp_path)
        )
        result = svc.execute(DatasetVersion(version="2026-03-09"))
        assert isinstance(result, VersionRecord)
        assert result.version == "2026-03-09"


class TestPublishServiceDryMode:
    def _dataset(self, tmp_path: Path) -> Dataset:
        return Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )

    def test_dry_does_not_write_to_warehouse(self, tmp_path: Path):
        catalog = _FakeCatalog()
        svc = PublishService(
            self._dataset(tmp_path), catalog, _FakeCompute(), _FakeLock(), _FakeMetadata(tmp_path)
        )
        svc.execute(DatasetVersion(version="2026-03-09", mode="dry"))
        assert catalog.written_versions == []

    def test_dry_returns_uncompleted_version_record(self, tmp_path: Path):
        svc = PublishService(
            self._dataset(tmp_path), _FakeCatalog(), _FakeCompute(), _FakeLock(), _FakeMetadata(tmp_path)
        )
        result = svc.execute(DatasetVersion(version="2026-03-09", mode="dry"))
        assert isinstance(result, VersionRecord)
        assert result.completed is False
        assert result.rows == 0

    def test_dry_does_not_append_version_to_lock(self, tmp_path: Path):
        lock = _FakeLock()
        svc = PublishService(
            self._dataset(tmp_path), _FakeCatalog(), _FakeCompute(), lock, _FakeMetadata(tmp_path)
        )
        svc.execute(DatasetVersion(version="2026-03-09", mode="dry"))
        assert lock.read_versions() == []

    def test_dry_raises_on_missing_schema(self, tmp_path: Path):
        lock = _FakeLock(schema=None)
        lock._schema = None
        svc = PublishService(
            self._dataset(tmp_path), _FakeCatalog(), _FakeCompute(), lock, _FakeMetadata(tmp_path)
        )
        with pytest.raises(RuntimeError, match="No schema"):
            svc.execute(DatasetVersion(version="2026-03-09", mode="dry"))


class TestPublishServiceRefreshMode:
    def _dataset(self, tmp_path: Path) -> Dataset:
        return Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )

    def test_refresh_overwrites_completed_version(self, tmp_path: Path):
        lock = _FakeLock()
        lock._versions = [_make_version_record(version="2026-03-09", completed=True)]
        catalog = _FakeCatalog()
        svc = PublishService(
            self._dataset(tmp_path), catalog, _FakeCompute(), lock, _FakeMetadata(tmp_path)
        )
        svc.execute(DatasetVersion(version="2026-03-09", mode="refresh"))
        assert "2026-03-09" in catalog.written_versions

    def test_refresh_passes_overwrite_true_to_catalog(self, tmp_path: Path):
        catalog = _FakeCatalog()
        svc = PublishService(
            self._dataset(tmp_path), catalog, _FakeCompute(), _FakeLock(), _FakeMetadata(tmp_path)
        )
        svc.execute(DatasetVersion(version="2026-03-09", mode="refresh"))
        assert catalog.last_overwrite is True

    def test_default_mode_passes_overwrite_false(self, tmp_path: Path):
        catalog = _FakeCatalog()
        svc = PublishService(
            self._dataset(tmp_path), catalog, _FakeCompute(), _FakeLock(), _FakeMetadata(tmp_path)
        )
        svc.execute(DatasetVersion(version="2026-03-09"))
        assert catalog.last_overwrite is False


class TestPublishServiceConnectionErrors:
    def _dataset(self, tmp_path: Path) -> Dataset:
        return Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )

    def test_schema_drift_check_non_drift_exception_continues(self, tmp_path: Path):
        """Non-SchemaDriftError during schema check logs warning but continues publish."""

        class _DriftErrorCatalog(_FakeCatalog):
            def __init__(self):
                super().__init__(table_exists=True)

            def load_arrow_schema(self, table_address):
                raise ConnectionError("catalog unreachable during schema check")

        catalog = _DriftErrorCatalog()
        svc = PublishService(
            self._dataset(tmp_path), catalog, _FakeCompute(), _FakeLock(), _FakeMetadata(tmp_path)
        )
        # Should NOT raise — the ConnectionError is caught and logged as warning
        result = svc.execute(DatasetVersion(version="2026-03-10"))
        assert result.version == "2026-03-10"

    def test_schema_drift_error_propagates(self, tmp_path: Path):
        """SchemaDriftError during schema check IS propagated."""
        from dbport.adapters.secondary.catalog.drift import SchemaDriftError

        class _SchemaDriftCatalog(_FakeCatalog):
            def __init__(self):
                super().__init__(table_exists=True)

            def load_arrow_schema(self, table_address):
                import pyarrow as pa
                # Return a schema that doesn't match the compute schema
                return pa.schema([pa.field("totally_different", pa.float64())])

        catalog = _SchemaDriftCatalog()
        svc = PublishService(
            self._dataset(tmp_path), catalog, _FakeCompute(), _FakeLock(), _FakeMetadata(tmp_path)
        )
        with pytest.raises(SchemaDriftError):
            svc.execute(DatasetVersion(version="2026-03-10"))

    def test_write_versioned_error_propagates(self, tmp_path: Path):
        """Errors from write_versioned are not swallowed."""

        class _WriteErrorCatalog(_FakeCatalog):
            def write_versioned(self, table_address, version, compute, overwrite=False):
                raise RuntimeError("write failed")

        catalog = _WriteErrorCatalog()
        svc = PublishService(
            self._dataset(tmp_path), catalog, _FakeCompute(), _FakeLock(), _FakeMetadata(tmp_path)
        )
        with pytest.raises(RuntimeError, match="write failed"):
            svc.execute(DatasetVersion(version="2026-03-10"))
