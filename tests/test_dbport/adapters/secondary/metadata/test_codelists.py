"""Tests for adapters.secondary.metadata.codelists."""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest

from dbport.adapters.secondary.metadata.codelists import (
    generate_csv_for_attached,
    generate_csv_for_column,
)
from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter


@pytest.fixture
def duckdb(tmp_path: Path) -> DuckDBComputeAdapter:
    ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
    ad.execute("CREATE TABLE outputs.data (geo VARCHAR, year INT, value DOUBLE)")
    ad.execute("INSERT INTO outputs.data VALUES ('DE', 2020, 1.0), ('FR', 2021, 2.0), ('DE', 2022, 3.0)")
    yield ad
    ad.close()


class TestGenerateCsvForColumn:
    def test_returns_bytes(self, duckdb):
        result = generate_csv_for_column(duckdb, "outputs.data", "geo")
        assert isinstance(result, bytes)

    def test_csv_has_header(self, duckdb):
        result = generate_csv_for_column(duckdb, "outputs.data", "geo")
        lines = result.decode("utf-8").splitlines()
        assert lines[0] == "code,name"

    def test_distinct_values_written(self, duckdb):
        result = generate_csv_for_column(duckdb, "outputs.data", "geo")
        reader = csv.reader(io.StringIO(result.decode("utf-8")))
        next(reader)  # skip header
        codes = sorted(row[0] for row in reader)
        assert codes == ["DE", "FR"]

    def test_code_and_name_are_same(self, duckdb):
        result = generate_csv_for_column(duckdb, "outputs.data", "geo")
        reader = csv.reader(io.StringIO(result.decode("utf-8")))
        next(reader)  # skip header
        for row in reader:
            assert row[0] == row[1]

    def test_sorted_output(self, duckdb):
        result = generate_csv_for_column(duckdb, "outputs.data", "geo")
        reader = csv.reader(io.StringIO(result.decode("utf-8")))
        next(reader)
        codes = [row[0] for row in reader]
        assert codes == sorted(codes)


class TestGenerateCsvForAttached:
    def test_returns_bytes(self, duckdb):
        result = generate_csv_for_attached(duckdb, "outputs.data")
        assert isinstance(result, bytes)

    def test_full_table_exported(self, duckdb):
        result = generate_csv_for_attached(duckdb, "outputs.data")
        reader = csv.reader(io.StringIO(result.decode("utf-8")))
        header = next(reader)
        assert header == ["geo", "year", "value"]
        rows = list(reader)
        assert len(rows) == 3

    def test_all_columns_present(self, duckdb):
        result = generate_csv_for_attached(duckdb, "outputs.data")
        lines = result.decode("utf-8").splitlines()
        assert lines[0] == "geo,year,value"
