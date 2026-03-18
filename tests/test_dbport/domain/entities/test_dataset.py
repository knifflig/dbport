"""Tests for domain.entities.dataset."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbport.domain.entities.dataset import Dataset, DatasetKey


class TestDatasetKey:
    """Tests for DatasetKey."""

    def test_construction(self) -> None:
        """Test basic construction."""
        key = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        assert key.agency == "wifor"
        assert key.dataset_id == "emp__regional_trends"

    def test_table_address(self) -> None:
        """Test table_address property."""
        key = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        assert key.table_address == "wifor.emp__regional_trends"

    def test_table_address_different_agency(self) -> None:
        """Test table_address with different agency."""
        key = DatasetKey(agency="estat", dataset_id="nama_10r_3empers")
        assert key.table_address == "estat.nama_10r_3empers"

    def test_frozen_agency(self) -> None:
        """Test that agency is immutable."""
        key = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        with pytest.raises((TypeError, ValidationError)):
            key.agency = "other"  # type: ignore[misc]

    def test_frozen_dataset_id(self) -> None:
        """Test that dataset_id is immutable."""
        key = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        with pytest.raises((TypeError, ValidationError)):
            key.dataset_id = "other"  # type: ignore[misc]

    def test_missing_agency(self) -> None:
        """Test that agency is required."""
        with pytest.raises(ValidationError):
            DatasetKey(dataset_id="emp__regional_trends")  # type: ignore[call-arg]

    def test_missing_dataset_id(self) -> None:
        """Test that dataset_id is required."""
        with pytest.raises(ValidationError):
            DatasetKey(agency="wifor")  # type: ignore[call-arg]

    def test_equality(self) -> None:
        """Test equality of identical keys."""
        a = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        b = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        assert a == b

    def test_inequality_agency(self) -> None:
        """Test inequality when agency differs."""
        a = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        b = DatasetKey(agency="estat", dataset_id="emp__regional_trends")
        assert a != b

    def test_inequality_dataset_id(self) -> None:
        """Test inequality when dataset_id differs."""
        a = DatasetKey(agency="wifor", dataset_id="emp__regional_trends")
        b = DatasetKey(agency="wifor", dataset_id="other_dataset")
        assert a != b


def _make_dataset(**kwargs: str) -> Dataset:
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
    """Tests for Dataset."""

    def test_construction(self) -> None:
        """Test basic construction."""
        ds = _make_dataset()
        assert ds.agency == "wifor"
        assert ds.dataset_id == "emp__regional_trends"
        assert ds.duckdb_path == "/data/emp.duckdb"
        assert ds.lock_path == "/project/dbport.lock"
        assert ds.model_root == "/project/models/emp"

    def test_table_address_inherited(self) -> None:
        """Test table_address inherited from DatasetKey."""
        ds = _make_dataset(
            agency="estat",
            dataset_id="nama_10r",
            duckdb_path="/data/nama.duckdb",
        )
        assert ds.table_address == "estat.nama_10r"

    def test_frozen(self) -> None:
        """Test that instances are immutable."""
        ds = _make_dataset()
        with pytest.raises((TypeError, ValidationError)):
            ds.duckdb_path = "/other/path.duckdb"  # type: ignore[misc]

    def test_missing_duckdb_path(self) -> None:
        """Test that duckdb_path is required."""
        with pytest.raises(ValidationError):
            Dataset(  # type: ignore[call-arg]
                agency="wifor",
                dataset_id="emp",
                lock_path="/dbport.lock",
                model_root="/m",
            )

    def test_missing_lock_path(self) -> None:
        """Test that lock_path is required."""
        with pytest.raises(ValidationError):
            Dataset(  # type: ignore[call-arg]
                agency="wifor",
                dataset_id="emp",
                duckdb_path="/data/emp.duckdb",
                model_root="/m",
            )

    def test_missing_model_root(self) -> None:
        """Test that model_root is required."""
        with pytest.raises(ValidationError):
            Dataset(  # type: ignore[call-arg]
                agency="wifor",
                dataset_id="emp",
                duckdb_path="/data/emp.duckdb",
                lock_path="/dbport.lock",
            )

    def test_is_dataset_key(self) -> None:
        """Test that Dataset is a DatasetKey."""
        ds = _make_dataset()
        assert isinstance(ds, DatasetKey)
