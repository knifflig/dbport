"""Tests for application.services.auto_schema."""

from __future__ import annotations

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
    def __init__(self, *, exists: bool = False, arrow_schema: pa.Schema | None = None):
        self._exists = exists
        self._arrow_schema = arrow_schema

    def table_exists(self, table_address: str) -> bool:
        return self._exists

    def load_arrow_schema(self, table_address: str) -> pa.Schema:
        assert self._arrow_schema is not None
        return self._arrow_schema


# ---------------------------------------------------------------------------
# Arrow type mapping
# ---------------------------------------------------------------------------


class TestArrowTypeToDuckdb:
    def test_string(self):
        assert _arrow_type_to_duckdb(pa.string()) == "VARCHAR"

    def test_large_string(self):
        assert _arrow_type_to_duckdb(pa.large_string()) == "VARCHAR"

    def test_int32(self):
        assert _arrow_type_to_duckdb(pa.int32()) == "INTEGER"

    def test_int64(self):
        assert _arrow_type_to_duckdb(pa.int64()) == "BIGINT"

    def test_float64(self):
        assert _arrow_type_to_duckdb(pa.float64()) == "DOUBLE"

    def test_bool(self):
        assert _arrow_type_to_duckdb(pa.bool_()) == "BOOLEAN"

    def test_timestamp_utc(self):
        assert _arrow_type_to_duckdb(pa.timestamp("us", tz="UTC")) == "TIMESTAMPTZ"

    def test_timestamp_no_tz(self):
        assert _arrow_type_to_duckdb(pa.timestamp("us")) == "TIMESTAMP"

    def test_date(self):
        assert _arrow_type_to_duckdb(pa.date32()) == "DATE"

    def test_unknown_falls_back_to_varchar(self):
        assert _arrow_type_to_duckdb(pa.list_(pa.int32())) == "VARCHAR"

    def test_smallint(self):
        assert _arrow_type_to_duckdb(pa.int16()) == "SMALLINT"

    def test_decimal128(self):
        assert _arrow_type_to_duckdb(pa.decimal128(10, 2)) == "DECIMAL(10, 2)"


# ---------------------------------------------------------------------------
# AutoSchemaService
# ---------------------------------------------------------------------------


@pytest.fixture
def compute(tmp_path: Path) -> DuckDBComputeAdapter:
    ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
    yield ad
    ad.close()


@pytest.fixture
def lock(tmp_path: Path) -> TomlLockAdapter:
    return TomlLockAdapter(tmp_path / "dbport.lock")


_WAREHOUSE_SCHEMA = pa.schema([
    pa.field("geo", pa.string()),
    pa.field("year", pa.int16()),
    pa.field("value", pa.float64()),
])


class TestAutoSchemaServiceTableExists:
    def test_returns_schema_when_table_exists(self, compute, lock):
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        schema = svc.execute("wifor.emp")
        assert schema is not None
        assert len(schema.columns) == 3

    def test_columns_mapped_correctly(self, compute, lock):
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        schema = svc.execute("wifor.emp")
        assert schema.columns[0].name == "geo"
        assert schema.columns[0].sql_type == "VARCHAR"
        assert schema.columns[1].name == "year"
        assert schema.columns[1].sql_type == "SMALLINT"
        assert schema.columns[2].name == "value"
        assert schema.columns[2].sql_type == "DOUBLE"

    def test_source_is_warehouse(self, compute, lock):
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        schema = svc.execute("wifor.emp")
        assert schema.source == "warehouse"

    def test_ddl_synthesized(self, compute, lock):
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        schema = svc.execute("wifor.emp")
        assert "CREATE OR REPLACE TABLE wifor.emp" in schema.ddl.statement
        assert "geo VARCHAR" in schema.ddl.statement

    def test_table_created_in_duckdb(self, compute, lock):
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        svc.execute("wifor.emp")
        assert compute.relation_exists("wifor", "emp")

    def test_schema_persisted_to_lock(self, compute, lock):
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        svc.execute("wifor.emp")
        persisted = lock.read_schema()
        assert persisted is not None
        assert persisted.source == "warehouse"
        assert len(persisted.columns) == 3

    def test_codelist_entries_seeded(self, compute, lock):
        catalog = _FakeCatalog(exists=True, arrow_schema=_WAREHOUSE_SCHEMA)
        svc = AutoSchemaService(catalog, compute, lock)
        svc.execute("wifor.emp")
        entries = lock.read_codelist_entries()
        assert "geo" in entries
        assert "year" in entries
        assert "value" in entries


class TestAutoSchemaServiceTableNotExists:
    def test_returns_none_when_table_missing(self, compute, lock):
        catalog = _FakeCatalog(exists=False)
        svc = AutoSchemaService(catalog, compute, lock)
        assert svc.execute("wifor.emp") is None

    def test_no_schema_persisted(self, compute, lock):
        catalog = _FakeCatalog(exists=False)
        svc = AutoSchemaService(catalog, compute, lock)
        svc.execute("wifor.emp")
        assert lock.read_schema() is None


class TestAutoSchemaServiceSkipLocal:
    def test_skips_when_local_schema_exists(self, compute, lock):
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

    def test_local_schema_preserved(self, compute, lock):
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
    def test_refreshes_when_source_is_warehouse(self, compute, lock):
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
