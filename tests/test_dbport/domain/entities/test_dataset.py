"""Tests for domain.entities.dataset."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbport.domain.entities.dataset import Dataset, DatasetKey


class TestDatasetKey:
    def test_construction(self):
        key = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        assert key.agency == "wifor"
        assert key.dataset_id == "emp__regional_trends"

    def test_table_address(self):
        key = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        assert key.table_address == "wifor.emp__regional_trends"

    def test_table_address_different_agency(self):
        key = DatasetKey(agency="estat", dataset_id="nama_10r_3empers")
        assert key.table_address == "estat.nama_10r_3empers"

    def test_frozen_agency(self):
        key = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        with pytest.raises((TypeError, ValidationError)):
            key.agency = "other"  # type: ignore[misc]

    def test_frozen_dataset_id(self):
        key = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        with pytest.raises((TypeError, ValidationError)):
            key.dataset_id = "other"  # type: ignore[misc]

    def test_missing_agency(self):
        with pytest.raises(ValidationError):
            DatasetKey(dataset_id="emp__regional_trends")  # type: ignore[call-arg]

    def test_missing_dataset_id(self):
        with pytest.raises(ValidationError):
            DatasetKey(agency="wifor")  # type: ignore[call-arg]

    def test_equality(self):
        a = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        b = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        assert a == b

    def test_inequality_agency(self):
        a = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        b = DatasetKey(agency="estat", dataset_id="emp__regional_trends")
        assert a != b

    def test_inequality_dataset_id(self):
        a = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        b = DatasetKey(agency="wifor", dataset_id="other_dataset")
        assert a != b


def _make_dataset(**kwargs) -> Dataset:
    defaults = dict(
        agency="wifor",
        dataset_id="emp__regional_trends",
        duckdb_path="/data/emp.duckdb",
        lock_path="/project/dbport.lock",
        model_root="/project/models/emp",
    )
    defaults.update(kwargs)
    return Dataset(**defaults)


class TestDataset:
    def test_construction(self):
        ds = _make_dataset()
        assert ds.agency == "wifor"
        assert ds.dataset_id == "emp__regional_trends"
        assert ds.duckdb_path == "/data/emp.duckdb"
        assert ds.lock_path == "/project/dbport.lock"
        assert ds.model_root == "/project/models/emp"

    def test_table_address_inherited(self):
        ds = _make_dataset(agency="estat", dataset_id="nama_10r", duckdb_path="/data/nama.duckdb")
        assert ds.table_address == "estat.nama_10r"

    def test_frozen(self):
        ds = _make_dataset()
        with pytest.raises((TypeError, ValidationError)):
            ds.duckdb_path = "/other/path.duckdb"  # type: ignore[misc]

    def test_missing_duckdb_path(self):
        with pytest.raises(ValidationError):
            Dataset(agency="wifor", dataset_id="emp", lock_path="/dbport.lock", model_root="/m")  # type: ignore[call-arg]

    def test_missing_lock_path(self):
        with pytest.raises(ValidationError):
            Dataset(agency="wifor", dataset_id="emp", duckdb_path="/data/emp.duckdb", model_root="/m")  # type: ignore[call-arg]

    def test_missing_model_root(self):
        with pytest.raises(ValidationError):
            Dataset(agency="wifor", dataset_id="emp", duckdb_path="/data/emp.duckdb", lock_path="/dbport.lock")  # type: ignore[call-arg]

    def test_is_dataset_key(self):
        ds = _make_dataset()
        assert isinstance(ds, DatasetKey)
