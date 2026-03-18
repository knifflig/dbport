"""Tests for domain.entities.schema."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbport.domain.entities.schema import ColumnDef, DatasetSchema, SqlDdl

_SAMPLE_DDL = "CREATE OR REPLACE TABLE wifor.emp (geo VARCHAR, year SMALLINT, value DOUBLE)"


class TestSqlDdl:
    """Tests for SqlDdl."""

    def test_construction(self) -> None:
        """Test basic construction."""
        ddl = SqlDdl(statement=_SAMPLE_DDL)
        assert ddl.statement == _SAMPLE_DDL

    def test_frozen(self) -> None:
        """Test that instances are immutable."""
        ddl = SqlDdl(statement=_SAMPLE_DDL)
        with pytest.raises((TypeError, ValidationError)):
            ddl.statement = "other"  # type: ignore[misc]

    def test_missing_statement(self) -> None:
        """Test that statement is required."""
        with pytest.raises(ValidationError):
            SqlDdl()  # type: ignore[call-arg]

    def test_equality(self) -> None:
        """Test equality of identical DDLs."""
        a = SqlDdl(statement=_SAMPLE_DDL)
        b = SqlDdl(statement=_SAMPLE_DDL)
        assert a == b

    def test_inequality(self) -> None:
        """Test inequality of different DDLs."""
        a = SqlDdl(statement=_SAMPLE_DDL)
        b = SqlDdl(statement="CREATE TABLE other (id INT)")
        assert a != b


class TestColumnDef:
    """Tests for ColumnDef."""

    def test_construction(self) -> None:
        """Test basic construction."""
        col = ColumnDef(name="geo", pos=0, sql_type="VARCHAR")
        assert col.name == "geo"
        assert col.pos == 0
        assert col.sql_type == "VARCHAR"

    def test_frozen(self) -> None:
        """Test that instances are immutable."""
        col = ColumnDef(name="geo", pos=0, sql_type="VARCHAR")
        with pytest.raises((TypeError, ValidationError)):
            col.name = "other"  # type: ignore[misc]

    def test_pos_zero(self) -> None:
        """Test that pos zero is valid."""
        col = ColumnDef(name="ts", pos=0, sql_type="TIMESTAMPTZ")
        assert col.pos == 0

    def test_missing_name(self) -> None:
        """Test that name is required."""
        with pytest.raises(ValidationError):
            ColumnDef(pos=0, sql_type="VARCHAR")  # type: ignore[call-arg]

    def test_missing_pos(self) -> None:
        """Test that pos is required."""
        with pytest.raises(ValidationError):
            ColumnDef(name="geo", sql_type="VARCHAR")  # type: ignore[call-arg]

    def test_missing_sql_type(self) -> None:
        """Test that sql_type is required."""
        with pytest.raises(ValidationError):
            ColumnDef(name="geo", pos=0)  # type: ignore[call-arg]

    def test_equality(self) -> None:
        """Test equality of identical column defs."""
        a = ColumnDef(name="geo", pos=0, sql_type="VARCHAR")
        b = ColumnDef(name="geo", pos=0, sql_type="VARCHAR")
        assert a == b

    def test_inequality_name(self) -> None:
        """Test inequality when name differs."""
        a = ColumnDef(name="geo", pos=0, sql_type="VARCHAR")
        b = ColumnDef(name="year", pos=0, sql_type="VARCHAR")
        assert a != b

    def test_inequality_sql_type(self) -> None:
        """Test inequality when sql_type differs."""
        a = ColumnDef(name="year", pos=0, sql_type="SMALLINT")
        b = ColumnDef(name="year", pos=0, sql_type="INTEGER")
        assert a != b


class TestDatasetSchema:
    """Tests for DatasetSchema."""

    def _make_schema(self) -> DatasetSchema:
        """Build a sample DatasetSchema."""
        ddl = SqlDdl(statement=_SAMPLE_DDL)
        cols = (
            ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),
            ColumnDef(name="year", pos=1, sql_type="SMALLINT"),
            ColumnDef(name="value", pos=2, sql_type="DOUBLE"),
        )
        return DatasetSchema(ddl=ddl, columns=cols)

    def test_construction(self) -> None:
        """Test basic construction."""
        schema = self._make_schema()
        assert schema.ddl.statement == _SAMPLE_DDL
        assert len(schema.columns) == 3

    def test_columns_tuple(self) -> None:
        """Test that columns is a tuple."""
        schema = self._make_schema()
        assert isinstance(schema.columns, tuple)

    def test_columns_order(self) -> None:
        """Test that column order is preserved."""
        schema = self._make_schema()
        assert schema.columns[0].name == "geo"
        assert schema.columns[1].name == "year"
        assert schema.columns[2].name == "value"

    def test_frozen(self) -> None:
        """Test that instances are immutable."""
        schema = self._make_schema()
        with pytest.raises((TypeError, ValidationError)):
            schema.columns = ()  # type: ignore[misc]

    def test_empty_columns(self) -> None:
        """Test construction with empty columns tuple."""
        ddl = SqlDdl(statement="CREATE TABLE t ()")
        schema = DatasetSchema(ddl=ddl, columns=())
        assert schema.columns == ()

    def test_missing_ddl(self) -> None:
        """Test that ddl is required."""
        cols = (ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),)
        with pytest.raises(ValidationError):
            DatasetSchema(columns=cols)  # type: ignore[call-arg]

    def test_missing_columns(self) -> None:
        """Test that columns is required."""
        ddl = SqlDdl(statement=_SAMPLE_DDL)
        with pytest.raises(ValidationError):
            DatasetSchema(ddl=ddl)  # type: ignore[call-arg]
