"""Tests for application.services.auto_schema."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
import pytest

from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter
from dbport.adapters.secondary.lock.toml import TomlLockAdapter
from dbport.application.services.auto_schema import (
    AutoSchemaService,
    _arrow_type_to_duckdb,
)
from dbport.domain.entities.schema import ColumnDef, DatasetSchema, SqlDdl

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeCatalog:
    def __init__(
        self,
        *,
        exists: bool = False,
        arrow_schema: pa.Schema | None = None,
    ) -> None:
        self._exists = exists
        self._arrow_schema = arrow_schema

    def table_exists(self, table_address: str) -> bool:
        """Check if table exists."""
        return self._exists

    def load_arrow_schema(self, table_address: str) -> pa.Schema:
        """Load Arrow schema for table."""
        assert self._arrow_schema is not None
        return self._arrow_schema


# ---------------------------------------------------------------------------
# Arrow type mapping
# ---------------------------------------------------------------------------


class TestArrowTypeToDuckdb:
    """Tests for _arrow_type_to_duckdb."""

    def test_string(self) -> None:
        """Test string type maps to VARCHAR."""
        assert _arrow_type_to_duckdb(pa.string()) == "VARCHAR"

    def test_large_string(self) -> None:
        """Test large_string type maps to VARCHAR."""
        assert _arrow_type_to_duckdb(pa.large_string()) == "VARCHAR"

    def test_int32(self) -> None:
        """Test int32 type maps to INTEGER."""
        assert _arrow_type_to_duckdb(pa.int32()) == "INTEGER"

    def test_int64(self) -> None:
        """Test int64 type maps to BIGINT."""
        assert _arrow_type_to_duckdb(pa.int64()) == "BIGINT"

    def test_float64(self) -> None:
        """Test float64 type maps to DOUBLE."""
        assert _arrow_type_to_duckdb(pa.float64()) == "DOUBLE"

    def test_bool(self) -> None:
        """Test bool type maps to BOOLEAN."""
        assert _arrow_type_to_duckdb(pa.bool_()) == "BOOLEAN"

    def test_timestamp_utc(self) -> None:
        """Test timestamp with UTC maps to TIMESTAMPTZ."""
        assert _arrow_type_to_duckdb(pa.timestamp("us", tz="UTC")) == "TIMESTAMPTZ"

    def test_timestamp_no_tz(self) -> None:
        """Test timestamp without tz maps to TIMESTAMP."""
        assert _arrow_type_to_duckdb(pa.timestamp("us")) == "TIMESTAMP"

    def test_date(self) -> None:
        """Test date32 type maps to DATE."""
        assert _arrow_type_to_duckdb(pa.date32()) == "DATE"

    def test_unknown_falls_back_to_varchar(self) -> None:
        """Test unknown type falls back to VARCHAR."""
        assert _arrow_type_to_duckdb(pa.list_(pa.int32())) == "VARCHAR"

    def test_smallint(self) -> None:
        """Test int16 type maps to SMALLINT."""
        assert _arrow_type_to_duckdb(pa.int16()) == "SMALLINT"

    def test_decimal128(self) -> None:
        """Test decimal128 type maps to DECIMAL."""
        assert _arrow_type_to_duckdb(pa.decimal128(10, 2)) == "DECIMAL(10, 2)"


# ---------------------------------------------------------------------------
# AutoSchemaService
# ---------------------------------------------------------------------------


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


_WAREHOUSE_SCHEMA = pa.schema(
    [
        pa.field("geo", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("value", pa.float64()),
    ]
)


class TestAutoSchemaServiceTableExists:
    """Tests for AutoSchemaService when table exists."""

    def test_returns_schema_when_table_exists(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test schema returned when table exists."""
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        schema = svc.execute("wifor.emp")
        assert schema is not None
        assert len(schema.columns) == 3

    def test_columns_mapped_correctly(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test columns are mapped correctly from Arrow schema."""
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        schema = svc.execute("wifor.emp")
        assert schema.columns[0].name == "geo"
        assert schema.columns[0].sql_type == "VARCHAR"
        assert schema.columns[1].name == "year"
        assert schema.columns[1].sql_type == "SMALLINT"
        assert schema.columns[2].name == "value"
        assert schema.columns[2].sql_type == "DOUBLE"

    def test_source_is_warehouse(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test schema source is warehouse."""
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        schema = svc.execute("wifor.emp")
        assert schema.source == "warehouse"

    def test_ddl_synthesized(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test DDL is synthesized from Arrow schema."""
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        schema = svc.execute("wifor.emp")
        assert "CREATE OR REPLACE TABLE wifor.emp" in schema.ddl.statement
        assert "geo VARCHAR" in schema.ddl.statement

    def test_table_created_in_duckdb(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test table is created in DuckDB."""
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        svc.execute("wifor.emp")
        assert compute.relation_exists("wifor", "emp")

    def test_schema_persisted_to_lock(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test schema is persisted to lock file."""
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        svc.execute("wifor.emp")
        persisted = lock.read_schema()
        assert persisted is not None
        assert persisted.source == "warehouse"
        assert len(persisted.columns) == 3

    def test_codelist_entries_seeded(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test codelist entries are seeded for all columns."""
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        svc.execute("wifor.emp")
        entries = lock.read_codelist_entries()
        assert "geo" in entries
        assert "year" in entries
        assert "value" in entries


class TestAutoSchemaServiceTableNotExists:
    """Tests for AutoSchemaService when table does not exist."""

    def test_returns_none_when_table_missing(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test None returned when table is missing."""
        catalog = _FakeCatalog(exists=False)
        svc = AutoSchemaService(catalog, compute, lock)
        assert svc.execute("wifor.emp") is None

    def test_no_schema_persisted(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test no schema persisted when table missing."""
        catalog = _FakeCatalog(exists=False)
        svc = AutoSchemaService(catalog, compute, lock)
        svc.execute("wifor.emp")
        assert lock.read_schema() is None


class TestAutoSchemaServiceSkipLocal:
    """Tests for AutoSchemaService skipping when local schema exists."""

    def test_skips_when_local_schema_exists(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test auto-schema skipped when local schema exists."""
        # Pre-populate lock with a user-declared schema
        local_schema = DatasetSchema(
            ddl=SqlDdl(statement="CREATE TABLE wifor.emp (id INT)"),
            columns=(ColumnDef(name="id", pos=0, sql_type="INT"),),
            source="local",
        )
        lock.write_schema(local_schema)

        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        assert svc.execute("wifor.emp") is None

    def test_local_schema_preserved(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """Test local schema is preserved when auto-schema skipped."""
        local_schema = DatasetSchema(
            ddl=SqlDdl(statement="CREATE TABLE wifor.emp (id INT)"),
            columns=(ColumnDef(name="id", pos=0, sql_type="INT"),),
            source="local",
        )
        lock.write_schema(local_schema)

        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        svc.execute("wifor.emp")

        persisted = lock.read_schema()
        assert persisted.source == "local"
        assert len(persisted.columns) == 1


class TestAutoSchemaServiceRefreshWarehouse:
    """Tests for AutoSchemaService refreshing warehouse schema."""

    def test_refreshes_when_source_is_warehouse(
        self,
        compute: DuckDBComputeAdapter,
        lock: TomlLockAdapter,
    ) -> None:
        """If lock has source='warehouse', auto-detect runs again (refreshes)."""
        old_schema = DatasetSchema(
            ddl=SqlDdl(statement="CREATE TABLE wifor.emp (geo VARCHAR)"),
            columns=(ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),),
            source="warehouse",
        )
        lock.write_schema(old_schema)

        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        schema = svc.execute("wifor.emp")
        assert schema is not None
        assert len(schema.columns) == 3  # refreshed with full schema
