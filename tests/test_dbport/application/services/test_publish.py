"""Tests for application.services.publish."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from dbport.application.services.publish import PublishService
from dbport.domain.entities.dataset import Dataset
from dbport.domain.entities.schema import ColumnDef, DatasetSchema, SqlDdl
from dbport.domain.entities.version import DatasetVersion, VersionRecord

_NOW = datetime(2026, 3, 9, 14, 0, 0, tzinfo=UTC)
_DDL = "CREATE OR REPLACE TABLE wifor.emp (geo VARCHAR, year SMALLINT)"


def _make_schema() -> DatasetSchema:
    """Build a test DatasetSchema."""
    return DatasetSchema(
        ddl=SqlDdl(statement=_DDL),
        columns=(
            ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),
            ColumnDef(name="year", pos=1, sql_type="SMALLINT"),
        ),
    )


def _make_version_record(
    version: str = "2026-03-09",
    *,
    completed: bool = True,
) -> VersionRecord:
    """Build a test VersionRecord."""
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
    """Minimal ICatalog stub for publish tests."""

    def __init__(self, *, table_exists: bool = False) -> None:
        self._table_exists = table_exists
        self.written_versions: list[str] = []
        self.last_overwrite: bool = False
        self.properties: dict[str, str] = {}
        self.column_docs: dict[str, str] = {}

    def table_exists(self, table_address: str) -> bool:
        """Check if table exists."""
        return self._table_exists

    def load_arrow_schema(self, table_address: str) -> pa.Schema:
        """Load Arrow schema for table."""
        return pa.schema(
            [
                pa.field("geo", pa.string()),
                pa.field("year", pa.int16()),
            ]
        )

    def current_snapshot(self, table_address: str) -> tuple[int, None]:
        """Return current snapshot."""
        return 999, None

    def scan_to_arrow_batches(
        self,
        declaration: object,
    ) -> iter:
        """Return empty iterator."""
        return iter([])

    def get_table_property(
        self,
        table_address: str,
        key: str,
    ) -> str | None:
        """Get table property."""
        return self.properties.get(key)

    def write_versioned(
        self,
        table_address: str,
        version: DatasetVersion,
        compute: object,
        *,
        overwrite: bool = False,
    ) -> VersionRecord:
        """Record written version."""
        self.written_versions.append(version.version)
        self.last_overwrite = overwrite
        return _make_version_record(version=version.version)

    def update_table_properties(
        self,
        table_address: str,
        properties: dict[str, str],
    ) -> None:
        """Record updated properties."""
        self.properties.update(properties)

    def update_column_docs(
        self,
        table_address: str,
        column_docs: dict[str, str],
    ) -> None:
        """Record updated column docs."""
        self.column_docs.update(column_docs)


class _FakeCompute:
    """Minimal ICompute stub for publish tests."""

    def __init__(self) -> None:
        self._table = pa.table(
            {
                "geo": pa.array(["DE"], pa.string()),
                "year": pa.array([2020], pa.int16()),
            }
        )

    def execute(
        self,
        sql: str,
        parameters: dict[str, str] | None = None,
    ) -> _FakeResult:
        """Execute SQL and return fake result."""
        return _FakeResult()

    def execute_file(self, path: str) -> None:
        """No-op."""

    def relation_exists(self, schema: str, table: str) -> bool:
        """Check if relation exists."""
        return True

    def to_arrow_batches(
        self,
        sql: str,
        batch_size: int = 10_000,
    ) -> pa.RecordBatchReader:
        """Return Arrow reader."""
        return self._table.to_reader()

    def ensure_extensions(self) -> None:
        """No-op."""

    def close(self) -> None:
        """No-op."""


class _FakeResult:
    """Minimal result stub."""

    def fetchone(self) -> None:
        """Return None."""
        return None

    def fetchall(self) -> list[object]:
        """Return empty list."""
        return []

    @property
    def schema_arrow(self) -> pa.Schema:
        """Return Arrow schema."""
        return pa.schema(
            [
                pa.field("geo", pa.string()),
                pa.field("year", pa.int16()),
            ]
        )


class _FakeLock:
    """Minimal ILockStore stub for publish tests."""

    def __init__(self, schema: DatasetSchema | None = None) -> None:
        self._schema = schema or _make_schema()
        self._ingest_records: list[object] = []
        self._versions: list[VersionRecord] = []
        self._codelist_entries: dict[str, object] = {}

    def read_schema(self) -> DatasetSchema | None:
        """Read schema from lock."""
        return self._schema

    def write_schema(self, schema: DatasetSchema) -> None:
        """Write schema to lock."""
        self._schema = schema

    def read_codelist_entries(self) -> dict[str, object]:
        """Read codelist entries."""
        return self._codelist_entries

    def write_codelist_entry(self, entry: object) -> None:
        """Write codelist entry."""
        self._codelist_entries[entry.column_name] = entry  # type: ignore[attr-defined]

    def read_ingest_records(self) -> list[object]:
        """Read ingest records."""
        return self._ingest_records

    def write_ingest_record(self, record: object) -> None:
        """Write ingest record."""
        self._ingest_records.append(record)

    def read_versions(self) -> list[VersionRecord]:
        """Read versions."""
        return list(self._versions)

    def append_version(self, record: VersionRecord) -> None:
        """Append version record."""
        self._versions.append(record)


class _FakeMetadata:
    """Minimal IMetadataStore stub for publish tests."""

    def __init__(self, tmp_path: Path) -> None:
        self._tmp_path = tmp_path

    def build_metadata_json(
        self,
        key: object,
        version: object,
        inputs: object,
        codelists: object,
        previous_metadata_json: bytes | None = None,
        snapshot_id: int | None = None,
    ) -> bytes:
        """Build metadata JSON bytes."""
        return b'{"schema_version":1}\n'

    def generate_codelist_bytes(
        self,
        codelists: object,
        compute: object,
        output_table: str,
    ) -> dict[str, bytes]:
        """Generate codelist bytes."""
        return {}

    def attach_to_table(
        self,
        table_address: str,
        metadata_bytes: bytes,
        codelist_bytes: dict[str, bytes],
        codelist_entries: object,
        catalog: object,
    ) -> None:
        """Attach metadata to table."""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPublishServiceNoSchema:
    """Tests for PublishService when no schema defined."""

    def test_raises_when_no_schema(self, tmp_path: Path) -> None:
        """Test raises RuntimeError when no schema."""
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
            dataset,
            _FakeCatalog(),
            _FakeCompute(),
            lock,
            _FakeMetadata(tmp_path),
        )
        with pytest.raises(RuntimeError, match="No schema"):
            svc.execute(DatasetVersion(version="2026-03-09"))


class TestPublishServiceIdempotent:
    """Tests for PublishService idempotent behavior."""

    def test_skips_completed_version(self, tmp_path: Path) -> None:
        """Test skips already-completed version."""
        dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        lock = _FakeLock()
        lock._versions = [
            _make_version_record(version="2026-03-09", completed=True),
        ]
        catalog = _FakeCatalog()

        svc = PublishService(
            dataset,
            catalog,
            _FakeCompute(),
            lock,
            _FakeMetadata(tmp_path),
        )
        result = svc.execute(DatasetVersion(version="2026-03-09"))

        # Should return the existing record without calling write_versioned
        assert result.version == "2026-03-09"
        assert catalog.written_versions == []


class TestPublishServiceIdempotentProgressCallback:
    """Tests for PublishService idempotent progress callback."""

    def test_skip_fires_progress_callback(self, tmp_path: Path) -> None:
        """Test progress_callback.log is called when version already completed."""
        from dbport.infrastructure.progress import progress_callback

        dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        lock = _FakeLock()
        lock._versions = [
            _make_version_record(version="2026-03-09", completed=True),
        ]

        svc = PublishService(
            dataset,
            _FakeCatalog(),
            _FakeCompute(),
            lock,
            _FakeMetadata(tmp_path),
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
    """Tests for PublishService full publish flow."""

    def test_calls_write_versioned(self, tmp_path: Path) -> None:
        """Test write_versioned is called."""
        dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        catalog = _FakeCatalog()
        svc = PublishService(
            dataset,
            catalog,
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        svc.execute(DatasetVersion(version="2026-03-09"))
        assert "2026-03-09" in catalog.written_versions

    def test_appends_version_record_to_lock(self, tmp_path: Path) -> None:
        """Test version record is appended to lock."""
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
            dataset,
            catalog,
            _FakeCompute(),
            lock,
            _FakeMetadata(tmp_path),
        )
        svc.execute(DatasetVersion(version="2026-03-09"))
        versions = lock.read_versions()
        assert len(versions) == 1
        assert versions[0].version == "2026-03-09"

    def test_returns_version_record(self, tmp_path: Path) -> None:
        """Test execute returns a VersionRecord."""
        dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        svc = PublishService(
            dataset,
            _FakeCatalog(),
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        result = svc.execute(DatasetVersion(version="2026-03-09"))
        assert isinstance(result, VersionRecord)
        assert result.version == "2026-03-09"


class TestPublishServiceDryMode:
    """Tests for PublishService dry mode."""

    def _dataset(self, tmp_path: Path) -> Dataset:
        """Build a test Dataset."""
        return Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )

    def test_dry_does_not_write_to_warehouse(
        self,
        tmp_path: Path,
    ) -> None:
        """Test dry mode does not write to warehouse."""
        catalog = _FakeCatalog()
        svc = PublishService(
            self._dataset(tmp_path),
            catalog,
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        svc.execute(DatasetVersion(version="2026-03-09", mode="dry"))
        assert catalog.written_versions == []

    def test_dry_returns_uncompleted_version_record(
        self,
        tmp_path: Path,
    ) -> None:
        """Test dry mode returns uncompleted VersionRecord."""
        svc = PublishService(
            self._dataset(tmp_path),
            _FakeCatalog(),
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        result = svc.execute(DatasetVersion(version="2026-03-09", mode="dry"))
        assert isinstance(result, VersionRecord)
        assert result.completed is False
        assert result.rows == 0

    def test_dry_does_not_append_version_to_lock(
        self,
        tmp_path: Path,
    ) -> None:
        """Test dry mode does not append version to lock."""
        lock = _FakeLock()
        svc = PublishService(
            self._dataset(tmp_path),
            _FakeCatalog(),
            _FakeCompute(),
            lock,
            _FakeMetadata(tmp_path),
        )
        svc.execute(DatasetVersion(version="2026-03-09", mode="dry"))
        assert lock.read_versions() == []

    def test_dry_raises_on_missing_schema(self, tmp_path: Path) -> None:
        """Test dry mode raises on missing schema."""
        lock = _FakeLock(schema=None)
        lock._schema = None
        svc = PublishService(
            self._dataset(tmp_path),
            _FakeCatalog(),
            _FakeCompute(),
            lock,
            _FakeMetadata(tmp_path),
        )
        with pytest.raises(RuntimeError, match="No schema"):
            svc.execute(DatasetVersion(version="2026-03-09", mode="dry"))


class TestPublishServiceRefreshMode:
    """Tests for PublishService refresh mode."""

    def _dataset(self, tmp_path: Path) -> Dataset:
        """Build a test Dataset."""
        return Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )

    def test_refresh_overwrites_completed_version(
        self,
        tmp_path: Path,
    ) -> None:
        """Test refresh mode overwrites completed version."""
        lock = _FakeLock()
        lock._versions = [
            _make_version_record(version="2026-03-09", completed=True),
        ]
        catalog = _FakeCatalog()
        svc = PublishService(
            self._dataset(tmp_path),
            catalog,
            _FakeCompute(),
            lock,
            _FakeMetadata(tmp_path),
        )
        svc.execute(DatasetVersion(version="2026-03-09", mode="refresh"))
        assert "2026-03-09" in catalog.written_versions

    def test_refresh_passes_overwrite_true_to_catalog(
        self,
        tmp_path: Path,
    ) -> None:
        """Test refresh mode passes overwrite=True to catalog."""
        catalog = _FakeCatalog()
        svc = PublishService(
            self._dataset(tmp_path),
            catalog,
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        svc.execute(DatasetVersion(version="2026-03-09", mode="refresh"))
        assert catalog.last_overwrite is True

    def test_default_mode_passes_overwrite_false(
        self,
        tmp_path: Path,
    ) -> None:
        """Test default mode passes overwrite=False."""
        catalog = _FakeCatalog()
        svc = PublishService(
            self._dataset(tmp_path),
            catalog,
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        svc.execute(DatasetVersion(version="2026-03-09"))
        assert catalog.last_overwrite is False


class TestPublishServiceConnectionErrors:
    """Tests for PublishService connection error handling."""

    def _dataset(self, tmp_path: Path) -> Dataset:
        """Build a test Dataset."""
        return Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )

    def test_schema_drift_check_non_network_exception_continues(
        self,
        tmp_path: Path,
    ) -> None:
        """Non-OSError during schema check logs warning but continues."""

        class _DriftErrorCatalog(_FakeCatalog):
            def __init__(self) -> None:
                super().__init__(table_exists=True)

            def load_arrow_schema(self, table_address: str) -> pa.Schema:
                raise TypeError("unexpected type mapping")

        catalog = _DriftErrorCatalog()
        svc = PublishService(
            self._dataset(tmp_path),
            catalog,
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        # Should NOT raise
        result = svc.execute(DatasetVersion(version="2026-03-10"))
        assert result.version == "2026-03-10"

    def test_schema_drift_check_connection_error_blocks_publish(
        self,
        tmp_path: Path,
    ) -> None:
        """ConnectionError during schema check blocks publish."""

        class _ConnErrorCatalog(_FakeCatalog):
            def __init__(self) -> None:
                super().__init__(table_exists=True)

            def load_arrow_schema(self, table_address: str) -> pa.Schema:
                raise ConnectionError("catalog unreachable")

        catalog = _ConnErrorCatalog()
        svc = PublishService(
            self._dataset(tmp_path),
            catalog,
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        with pytest.raises(RuntimeError, match="Cannot verify warehouse schema"):
            svc.execute(DatasetVersion(version="2026-03-10"))

    def test_schema_drift_check_timeout_error_blocks_publish(
        self,
        tmp_path: Path,
    ) -> None:
        """TimeoutError during schema check blocks publish."""

        class _TimeoutCatalog(_FakeCatalog):
            def __init__(self) -> None:
                super().__init__(table_exists=True)

            def load_arrow_schema(self, table_address: str) -> pa.Schema:
                raise TimeoutError("connection timed out")

        catalog = _TimeoutCatalog()
        svc = PublishService(
            self._dataset(tmp_path),
            catalog,
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        with pytest.raises(RuntimeError, match="Cannot verify warehouse schema"):
            svc.execute(DatasetVersion(version="2026-03-10"))

    def test_schema_drift_error_propagates(self, tmp_path: Path) -> None:
        """SchemaDriftError during schema check IS propagated."""
        from dbport.adapters.secondary.catalog.drift import SchemaDriftError

        class _SchemaDriftCatalog(_FakeCatalog):
            def __init__(self) -> None:
                super().__init__(table_exists=True)

            def load_arrow_schema(self, table_address: str) -> pa.Schema:
                # Return a schema that doesn't match the compute schema
                return pa.schema(
                    [
                        pa.field("totally_different", pa.float64()),
                    ]
                )

        catalog = _SchemaDriftCatalog()
        svc = PublishService(
            self._dataset(tmp_path),
            catalog,
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        with pytest.raises(SchemaDriftError):
            svc.execute(DatasetVersion(version="2026-03-10"))

    def test_write_versioned_error_propagates(
        self,
        tmp_path: Path,
    ) -> None:
        """Errors from write_versioned are not swallowed."""

        class _WriteErrorCatalog(_FakeCatalog):
            def write_versioned(
                self,
                table_address: str,
                version: DatasetVersion,
                compute: object,
                *,
                overwrite: bool = False,
            ) -> VersionRecord:
                raise RuntimeError("write failed")

        catalog = _WriteErrorCatalog()
        svc = PublishService(
            self._dataset(tmp_path),
            catalog,
            _FakeCompute(),
            _FakeLock(),
            _FakeMetadata(tmp_path),
        )
        with pytest.raises(RuntimeError, match="write failed"):
            svc.execute(DatasetVersion(version="2026-03-10"))
