"""Tests for application.services.schema."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
import pytest

from dbport.adapters.secondary.catalog.drift import SchemaDriftError
from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter
from dbport.adapters.secondary.lock.toml import TomlLockAdapter
from dbport.application.services.schema import DefineSchemaService

_DDL = "CREATE OR REPLACE TABLE inputs.emp (geo VARCHAR, year SMALLINT, value DOUBLE)"


@pytest.fixture
def compute(tmp_path: Path) -> Generator[DuckDBComputeAdapter]:
    """Create a DuckDB compute adapter for testing."""
    ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
    yield ad
    ad.close()


@pytest.fixture
def lock(tmp_path: Path) -> TomlLockAdapter:
    """Create a TOML lock adapter for testing."""
    return TomlLockAdapter(tmp_path / "dbport.lock")


class TestDefineSchemaServiceInline:
    """Tests for DefineSchemaService with inline DDL."""

    def test_returns_dataset_schema(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test execute returns a DatasetSchema."""
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(_DDL, base_dir="/tmp")
        assert schema is not None
        assert schema.ddl.statement == _DDL

    def test_columns_parsed_correctly(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test columns are parsed correctly."""
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(_DDL, base_dir="/tmp")
        names = [c.name for c in schema.columns]
        assert names == ["geo", "year", "value"]

    def test_column_positions_zero_based(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test column positions are zero-based."""
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(_DDL, base_dir="/tmp")
        assert schema.columns[0].pos == 0
        assert schema.columns[1].pos == 1
        assert schema.columns[2].pos == 2

    def test_schema_persisted_to_lock(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test schema is persisted to lock file."""
        svc = DefineSchemaService(compute, lock)
        svc.execute(_DDL, base_dir="/tmp")
        persisted = lock.read_schema()
        assert persisted is not None
        assert persisted.ddl.statement == _DDL

    def test_codelist_entries_seeded_by_write_schema(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test codelist entries are seeded by write_schema."""
        svc = DefineSchemaService(compute, lock)
        svc.execute(_DDL, base_dir="/tmp")
        entries = lock.read_codelist_entries()
        assert "geo" in entries
        assert "year" in entries
        assert "value" in entries

    def test_table_created_in_duckdb(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test table is created in DuckDB."""
        svc = DefineSchemaService(compute, lock)
        svc.execute(_DDL, base_dir="/tmp")
        assert compute.relation_exists("inputs", "emp")

    def test_second_execute_overwrites_previous(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test second execute overwrites previous schema."""
        svc = DefineSchemaService(compute, lock)
        svc.execute(_DDL, base_dir="/tmp")
        new_ddl = "CREATE OR REPLACE TABLE inputs.emp (id INT)"
        schema = svc.execute(new_ddl, base_dir="/tmp")
        assert len(schema.columns) == 1
        assert schema.columns[0].name == "id"


class TestDefineSchemaServiceFromFile:
    """Tests for DefineSchemaService reading DDL from file."""

    def test_reads_ddl_from_sql_file(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
        tmp_path: Path,
    ) -> None:
        """Test DDL is read from SQL file."""
        sql_file = tmp_path / "create.sql"
        sql_file.write_text(_DDL, encoding="utf-8")
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute("create.sql", base_dir=str(tmp_path))
        assert len(schema.columns) == 3

    def test_absolute_path_resolved_directly(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
        tmp_path: Path,
    ) -> None:
        """Test absolute path is resolved directly."""
        sql_file = tmp_path / "abs.sql"
        sql_file.write_text(_DDL, encoding="utf-8")
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(str(sql_file), base_dir="/ignored")
        assert len(schema.columns) == 3

    def test_invalid_ddl_raises_value_error(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test invalid DDL raises ValueError."""
        svc = DefineSchemaService(compute, lock)
        with pytest.raises(ValueError, match="table address"):
            svc.execute("NOT A DDL", base_dir="/tmp")


class TestDefineSchemaServiceUnqualifiedTable:
    """Tests for DefineSchemaService with unqualified table names."""

    def test_unqualified_table_uses_main_schema(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """CREATE TABLE without schema prefix uses 'main' schema."""
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(
            "CREATE TABLE my_table (id INT)",
            base_dir="/tmp",
        )
        assert len(schema.columns) == 1
        assert schema.columns[0].name == "id"


class TestDefineSchemaServicePathTraversal:
    """Tests for DefineSchemaService path traversal rejection."""

    def test_relative_traversal_rejected(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
        tmp_path: Path,
    ) -> None:
        """Test relative path traversal is rejected."""
        svc = DefineSchemaService(compute, lock)
        with pytest.raises(ValueError, match="escapes base directory"):
            svc.execute("../../etc/passwd.sql", base_dir=str(tmp_path))

    def test_valid_relative_path_still_works(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
        tmp_path: Path,
    ) -> None:
        """Test valid relative path still works."""
        sub = tmp_path / "sql"
        sub.mkdir()
        (sub / "create.sql").write_text(_DDL, encoding="utf-8")
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute("sql/create.sql", base_dir=str(tmp_path))
        assert len(schema.columns) == 3

    def test_absolute_path_bypasses_check(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
        tmp_path: Path,
    ) -> None:
        """Test absolute path bypasses traversal check."""
        sql_file = tmp_path / "abs.sql"
        sql_file.write_text(_DDL, encoding="utf-8")
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(str(sql_file), base_dir="/ignored")
        assert len(schema.columns) == 3


class _FakeCatalog:
    """Minimal catalog stub for schema drift tests."""

    def __init__(self, schema: pa.Schema, *, exists: bool = True) -> None:
        self._schema = schema
        self._exists = exists

    def table_exists(self, _table_address: str) -> bool:
        """Check if table exists."""
        return self._exists

    def load_arrow_schema(self, _table_address: str) -> pa.Schema:
        """Load Arrow schema for table."""
        return self._schema


class TestDefineSchemaServiceSchemaDrift:
    """Tests for DefineSchemaService schema drift detection."""

    def test_schema_drift_fails_early(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test schema drift is detected early."""
        svc = DefineSchemaService(compute, lock).with_catalog(
            _FakeCatalog(pa.schema([("id", pa.int64())])),
            "inputs.emp",
        )

        with pytest.raises(SchemaDriftError, match="Schema drift detected"):
            svc.execute(_DDL, base_dir="/tmp")


class _FakeCatalogRaises:
    """Catalog that raises a non-SchemaDriftError exception."""

    def table_exists(self, _table_address: str) -> bool:
        """Raise ConnectionError."""
        raise ConnectionError("network timeout")

    def load_arrow_schema(self, _table_address: str) -> pa.Schema:
        """Raise ConnectionError."""
        raise ConnectionError("network timeout")


class TestDefineSchemaServiceDriftCheckWarning:
    """Cover non-SchemaDriftError exception logged as warning."""

    def test_non_drift_error_logged_as_warning(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Non-SchemaDriftError exceptions are caught and logged as warnings."""
        import logging

        svc = DefineSchemaService(compute, lock).with_catalog(
            _FakeCatalogRaises(),
            "inputs.emp",
        )

        with caplog.at_level(logging.WARNING):
            schema = svc.execute(_DDL, base_dir="/tmp")

        # Schema should still be created despite the warning
        assert schema is not None
        assert len(schema.columns) == 3
        assert any("Schema drift check skipped" in r.message for r in caplog.records)


class _FakeCatalogTableNotExists:
    """Catalog where the output table does not exist yet."""

    def table_exists(self, _table_address: str) -> bool:
        """Return False."""
        return False

    def load_arrow_schema(self, _table_address: str) -> pa.Schema:
        """Should not be called."""
        raise AssertionError("should not be called")


class TestDefineSchemaServiceTableNotExists:
    """Cover schema.py: table_exists returns False, drift check skipped."""

    def test_drift_check_skipped_when_table_not_exists(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test drift check skipped when table does not exist."""
        svc = DefineSchemaService(compute, lock).with_catalog(
            _FakeCatalogTableNotExists(),
            "inputs.emp",
        )
        schema = svc.execute(_DDL, base_dir="/tmp")
        assert schema is not None
        assert len(schema.columns) == 3
