"""Tests for domain.entities.input."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbport.domain.entities.input import IngestRecord, InputDeclaration


class TestInputDeclaration:
    def test_construction_minimal(self):
        decl = InputDeclaration(table_address="estat.nama_10r_3empers")
        assert decl.table_address == "estat.nama_10r_3empers"
        assert decl.filters is None

    def test_construction_with_filters(self):
        decl = InputDeclaration(
            table_address="estat.nama_10r_3empers",
            filters={"wstatus": "EMP", "nace_r2": "TOTAL"},
        )
        assert decl.filters == {"wstatus": "EMP", "nace_r2": "TOTAL"}

    def test_frozen(self):
        decl = InputDeclaration(table_address="estat.nama_10r_3empers")
        with pytest.raises((TypeError, ValidationError)):
            decl.table_address = "other.table"  # type: ignore[misc]

    def test_missing_table_address(self):
        with pytest.raises(ValidationError):
            InputDeclaration()  # type: ignore[call-arg]

    def test_equality(self):
        a = InputDeclaration(table_address="estat.nama_10r_3empers", filters={"wstatus": "EMP"})
        b = InputDeclaration(table_address="estat.nama_10r_3empers", filters={"wstatus": "EMP"})
        assert a == b

    def test_inequality_table_address(self):
        a = InputDeclaration(table_address="estat.foo")
        b = InputDeclaration(table_address="estat.bar")
        assert a != b

    def test_inequality_filters(self):
        a = InputDeclaration(table_address="estat.foo", filters={"wstatus": "EMP"})
        b = InputDeclaration(table_address="estat.foo", filters={"wstatus": "SAL"})
        assert a != b

    def test_no_filters_default(self):
        decl = InputDeclaration(table_address="wifor.cl_nuts2024")
        assert decl.filters is None


class TestIngestRecord:
    def test_construction_minimal(self):
        record = IngestRecord(table_address="estat.nama_10r_3empers")
        assert record.table_address == "estat.nama_10r_3empers"
        assert record.last_snapshot_id is None
        assert record.last_snapshot_timestamp_ms is None
        assert record.rows_loaded is None
        assert record.filters is None

    def test_construction_full(self):
        record = IngestRecord(
            table_address="estat.nama_10r_3empers",
            last_snapshot_id=123456789,
            last_snapshot_timestamp_ms=1741478400000,
            rows_loaded=500_000,
            filters={"wstatus": "EMP"},
        )
        assert record.last_snapshot_id == 123456789
        assert record.last_snapshot_timestamp_ms == 1741478400000
        assert record.rows_loaded == 500_000
        assert record.filters == {"wstatus": "EMP"}

    def test_frozen(self):
        record = IngestRecord(table_address="estat.nama_10r_3empers")
        with pytest.raises((TypeError, ValidationError)):
            record.last_snapshot_id = 999  # type: ignore[misc]

    def test_missing_table_address(self):
        with pytest.raises(ValidationError):
            IngestRecord()  # type: ignore[call-arg]

    def test_equality(self):
        a = IngestRecord(table_address="estat.foo", last_snapshot_id=1)
        b = IngestRecord(table_address="estat.foo", last_snapshot_id=1)
        assert a == b

    def test_inequality(self):
        a = IngestRecord(table_address="estat.foo", last_snapshot_id=1)
        b = IngestRecord(table_address="estat.foo", last_snapshot_id=2)
        assert a != b
