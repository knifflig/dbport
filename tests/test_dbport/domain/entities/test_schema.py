"""Tests for domain.entities.schema."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbport.domain.entities.schema import ColumnDef, DatasetSchema, SqlDdl


_SAMPLE_DDL = "CREATE OR REPLACE TABLE wifor.emp (geo VARCHAR, year SMALLINT, value DOUBLE)"


class TestSqlDdl:
    def test_construction(self):
        ddl = SqlDdl(statement=_SAMPLE_DDL)
        assert ddl.statement == _SAMPLE_DDL

    def test_frozen(self):
        ddl = SqlDdl(statement=_SAMPLE_DDL)
        with pytest.raises((TypeError, ValidationError)):
            ddl.statement = "other"  # type: ignore[misc]

    def test_missing_statement(self):
        with pytest.raises(ValidationError):
            SqlDdl()  # type: ignore[call-arg]

    def test_equality(self):
        a = SqlDdl(statement=_SAMPLE_DDL)
        b = SqlDdl(statement=_SAMPLE_DDL)
        assert a == b

    def test_inequality(self):
        a = SqlDdl(statement=_SAMPLE_DDL)
        b = SqlDdl(statement="CREATE TABLE other (id INT)")
        assert a != b


class TestColumnDef:
    def test_construction(self):
        col = ColumnDef(name="geo", pos=0, sql_type="VARCHAR")
        assert col.name == "geo"
        assert col.pos == 0
        assert col.sql_type == "VARCHAR"

    def test_frozen(self):
        col = ColumnDef(name="geo", pos=0, sql_type="VARCHAR")
        with pytest.raises((TypeError, ValidationError)):
            col.name = "other"  # type: ignore[misc]

    def test_pos_zero(self):
        col = ColumnDef(name="ts", pos=0, sql_type="TIMESTAMPTZ")
        assert col.pos == 0

    def test_missing_name(self):
        with pytest.raises(ValidationError):
            ColumnDef(pos=0, sql_type="VARCHAR")  # type: ignore[call-arg]

    def test_missing_pos(self):
        with pytest.raises(ValidationError):
            ColumnDef(name="geo", sql_type="VARCHAR")  # type: ignore[call-arg]

    def test_missing_sql_type(self):
        with pytest.raises(ValidationError):
            ColumnDef(name="geo", pos=0)  # type: ignore[call-arg]

    def test_equality(self):
        a = ColumnDef(name="geo", pos=0, sql_type="VARCHAR")
        b = ColumnDef(name="geo", pos=0, sql_type="VARCHAR")
        assert a == b

    def test_inequality_name(self):
        a = ColumnDef(name="geo", pos=0, sql_type="VARCHAR")
        b = ColumnDef(name="year", pos=0, sql_type="VARCHAR")
        assert a != b

    def test_inequality_sql_type(self):
        a = ColumnDef(name="year", pos=0, sql_type="SMALLINT")
        b = ColumnDef(name="year", pos=0, sql_type="INTEGER")
        assert a != b


class TestDatasetSchema:
    def _make_schema(self):
        ddl = SqlDdl(statement=_SAMPLE_DDL)
        cols = (
            ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),
            ColumnDef(name="year", pos=1, sql_type="SMALLINT"),
            ColumnDef(name="value", pos=2, sql_type="DOUBLE"),
        )
        return DatasetSchema(ddl=ddl, columns=cols)

    def test_construction(self):
        schema = self._make_schema()
        assert schema.ddl.statement == _SAMPLE_DDL
        assert len(schema.columns) == 3

    def test_columns_tuple(self):
        schema = self._make_schema()
        assert isinstance(schema.columns, tuple)

    def test_columns_order(self):
        schema = self._make_schema()
        assert schema.columns[0].name == "geo"
        assert schema.columns[1].name == "year"
        assert schema.columns[2].name == "value"

    def test_frozen(self):
        schema = self._make_schema()
        with pytest.raises((TypeError, ValidationError)):
            schema.columns = ()  # type: ignore[misc]

    def test_empty_columns(self):
        ddl = SqlDdl(statement="CREATE TABLE t ()")
        schema = DatasetSchema(ddl=ddl, columns=())
        assert schema.columns == ()

    def test_missing_ddl(self):
        cols = (ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),)
        with pytest.raises(ValidationError):
            DatasetSchema(columns=cols)  # type: ignore[call-arg]

    def test_missing_columns(self):
        ddl = SqlDdl(statement=_SAMPLE_DDL)
        with pytest.raises(ValidationError):
            DatasetSchema(ddl=ddl)  # type: ignore[call-arg]
