"""Tests for application.services.schema."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from dbport.adapters.secondary.catalog.drift import SchemaDriftError
from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter
from dbport.adapters.secondary.lock.toml import TomlLockAdapter
from dbport.application.services.schema import DefineSchemaService

_DDL = "CREATE OR REPLACE TABLE inputs.emp (geo VARCHAR, year SMALLINT, value DOUBLE)"


@pytest.fixture
def compute(tmp_path: Path) -> DuckDBComputeAdapter:
    ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
    yield ad
    ad.close()


@pytest.fixture
def lock(tmp_path: Path) -> TomlLockAdapter:
    return TomlLockAdapter(tmp_path / "dbport.lock")


class TestDefineSchemaServiceInline:
    def test_returns_dataset_schema(self, compute, lock):
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(_DDL, base_dir="/tmp")
        assert schema is not None
        assert schema.ddl.statement == _DDL

    def test_columns_parsed_correctly(self, compute, lock):
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(_DDL, base_dir="/tmp")
        names = [c.name for c in schema.columns]
        assert names == ["geo", "year", "value"]

    def test_column_positions_zero_based(self, compute, lock):
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(_DDL, base_dir="/tmp")
        assert schema.columns[0].pos == 0
        assert schema.columns[1].pos == 1
        assert schema.columns[2].pos == 2

    def test_schema_persisted_to_lock(self, compute, lock):
        svc = DefineSchemaService(compute, lock)
        svc.execute(_DDL, base_dir="/tmp")
        persisted = lock.read_schema()
        assert persisted is not None
        assert persisted.ddl.statement == _DDL

    def test_codelist_entries_seeded_by_write_schema(self, compute, lock):
        svc = DefineSchemaService(compute, lock)
        svc.execute(_DDL, base_dir="/tmp")
        entries = lock.read_codelist_entries()
        assert "geo" in entries
        assert "year" in entries
        assert "value" in entries

    def test_table_created_in_duckdb(self, compute, lock):
        svc = DefineSchemaService(compute, lock)
        svc.execute(_DDL, base_dir="/tmp")
        assert compute.relation_exists("inputs", "emp")

    def test_second_execute_overwrites_previous(self, compute, lock):
        svc = DefineSchemaService(compute, lock)
        svc.execute(_DDL, base_dir="/tmp")
        new_ddl = "CREATE OR REPLACE TABLE inputs.emp (id INT)"
        schema = svc.execute(new_ddl, base_dir="/tmp")
        assert len(schema.columns) == 1
        assert schema.columns[0].name == "id"


class TestDefineSchemaServiceFromFile:
    def test_reads_ddl_from_sql_file(self, compute, lock, tmp_path: Path):
        sql_file = tmp_path / "create.sql"
        sql_file.write_text(_DDL, encoding="utf-8")
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute("create.sql", base_dir=str(tmp_path))
        assert len(schema.columns) == 3

    def test_absolute_path_resolved_directly(self, compute, lock, tmp_path: Path):
        sql_file = tmp_path / "abs.sql"
        sql_file.write_text(_DDL, encoding="utf-8")
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(str(sql_file), base_dir="/ignored")
        assert len(schema.columns) == 3

    def test_invalid_ddl_raises_value_error(self, compute, lock):
        svc = DefineSchemaService(compute, lock)
        with pytest.raises(ValueError, match="table address"):
            svc.execute("NOT A DDL", base_dir="/tmp")


class TestDefineSchemaServiceUnqualifiedTable:
    def test_unqualified_table_uses_main_schema(self, compute, lock):
        """CREATE TABLE without schema prefix uses 'main' schema."""
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute("CREATE TABLE my_table (id INT)", base_dir="/tmp")
        assert len(schema.columns) == 1
        assert schema.columns[0].name == "id"


class TestDefineSchemaServicePathTraversal:
    def test_relative_traversal_rejected(self, compute, lock, tmp_path: Path):
        svc = DefineSchemaService(compute, lock)
        with pytest.raises(ValueError, match="escapes base directory"):
            svc.execute("../../etc/passwd.sql", base_dir=str(tmp_path))

    def test_valid_relative_path_still_works(self, compute, lock, tmp_path: Path):
        sub = tmp_path / "sql"
        sub.mkdir()
        (sub / "create.sql").write_text(_DDL, encoding="utf-8")
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute("sql/create.sql", base_dir=str(tmp_path))
        assert len(schema.columns) == 3

    def test_absolute_path_bypasses_check(self, compute, lock, tmp_path: Path):
        sql_file = tmp_path / "abs.sql"
        sql_file.write_text(_DDL, encoding="utf-8")
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(str(sql_file), base_dir="/ignored")
        assert len(schema.columns) == 3


# ---------------------------------------------------------------------------
# Schema drift check (warehouse comparison)
# ---------------------------------------------------------------------------

_WAREHOUSE_ARROW = pa.schema([
    pa.field("geo", pa.string()),
    pa.field("year", pa.int16()),
    pa.field("value", pa.float64()),
])


class _FakeCatalog:
    """Minimal catalog double for drift-check tests."""

    def __init__(
        self,
        table_exists: bool = False,
        arrow_schema: Any = None,
        raise_on_load: Exception | None = None,
    ):
        self._table_exists = table_exists
        self._arrow_schema = arrow_schema
        self._raise_on_load = raise_on_load

    def table_exists(self, table_address: str) -> bool:
        return self._table_exists

    def load_arrow_schema(self, table_address: str) -> Any:
        if self._raise_on_load is not None:
            raise self._raise_on_load
        return self._arrow_schema


class TestDefineSchemaServiceDriftCheck:
    """Early schema drift detection against the warehouse."""

    def test_no_drift_when_schemas_match(self, compute, lock):
        catalog = _FakeCatalog(table_exists=True, arrow_schema=_WAREHOUSE_ARROW)
        svc = DefineSchemaService(compute, lock, catalog=catalog, table_address="inputs.emp")
        schema = svc.execute(_DDL, base_dir="/tmp")
        assert len(schema.columns) == 3

    def test_drift_raises_when_column_added(self, compute, lock):
        warehouse = pa.schema([
            pa.field("geo", pa.string()),
            pa.field("year", pa.int16()),
        ])
        catalog = _FakeCatalog(table_exists=True, arrow_schema=warehouse)
        svc = DefineSchemaService(compute, lock, catalog=catalog, table_address="inputs.emp")
        with pytest.raises(SchemaDriftError, match="value"):
            svc.execute(_DDL, base_dir="/tmp")

    def test_drift_raises_when_column_removed(self, compute, lock):
        warehouse = pa.schema([
            pa.field("geo", pa.string()),
            pa.field("year", pa.int16()),
            pa.field("value", pa.float64()),
            pa.field("extra", pa.string()),
        ])
        catalog = _FakeCatalog(table_exists=True, arrow_schema=warehouse)
        svc = DefineSchemaService(compute, lock, catalog=catalog, table_address="inputs.emp")
        with pytest.raises(SchemaDriftError, match="extra"):
            svc.execute(_DDL, base_dir="/tmp")

    def test_drift_raises_when_type_changed(self, compute, lock):
        warehouse = pa.schema([
            pa.field("geo", pa.string()),
            pa.field("year", pa.int32()),  # int32 vs SMALLINT (int16)
            pa.field("value", pa.float64()),
        ])
        catalog = _FakeCatalog(table_exists=True, arrow_schema=warehouse)
        svc = DefineSchemaService(compute, lock, catalog=catalog, table_address="inputs.emp")
        with pytest.raises(SchemaDriftError, match="year"):
            svc.execute(_DDL, base_dir="/tmp")

    def test_no_check_when_table_does_not_exist(self, compute, lock):
        catalog = _FakeCatalog(table_exists=False)
        svc = DefineSchemaService(compute, lock, catalog=catalog, table_address="inputs.emp")
        schema = svc.execute(_DDL, base_dir="/tmp")
        assert len(schema.columns) == 3

    def test_no_check_when_catalog_is_none(self, compute, lock):
        svc = DefineSchemaService(compute, lock)
        schema = svc.execute(_DDL, base_dir="/tmp")
        assert len(schema.columns) == 3

    def test_non_drift_exception_swallowed(self, compute, lock):
        catalog = _FakeCatalog(
            table_exists=True,
            raise_on_load=RuntimeError("network timeout"),
        )
        svc = DefineSchemaService(compute, lock, catalog=catalog, table_address="inputs.emp")
        schema = svc.execute(_DDL, base_dir="/tmp")
        assert len(schema.columns) == 3
