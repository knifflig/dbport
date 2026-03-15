"""Tests for domain.entities.version."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from dbport.domain.entities.version import DatasetVersion, VersionRecord


_NOW = datetime(2026, 3, 9, 14, 32, 0, tzinfo=timezone.utc)


class TestDatasetVersion:
    def test_construction_minimal(self):
        v = DatasetVersion(version="2026-03-09")
        assert v.version == "2026-03-09"
        assert v.params is None

    def test_construction_with_params(self):
        v = DatasetVersion(version="2026-03-09", params={"wstatus": "EMP", "nace_r2": "TOTAL"})
        assert v.params == {"wstatus": "EMP", "nace_r2": "TOTAL"}

    def test_frozen(self):
        v = DatasetVersion(version="2026-03-09")
        with pytest.raises((TypeError, ValidationError)):
            v.version = "2026-01-01"  # type: ignore[misc]

    def test_missing_version(self):
        with pytest.raises(ValidationError):
            DatasetVersion()  # type: ignore[call-arg]

    def test_equality(self):
        a = DatasetVersion(version="2026-03-09", params={"wstatus": "EMP"})
        b = DatasetVersion(version="2026-03-09", params={"wstatus": "EMP"})
        assert a == b

    def test_inequality_version(self):
        a = DatasetVersion(version="2026-03-09")
        b = DatasetVersion(version="2026-01-01")
        assert a != b

    def test_inequality_params(self):
        a = DatasetVersion(version="2026-03-09", params={"wstatus": "EMP"})
        b = DatasetVersion(version="2026-03-09", params={"wstatus": "SAL"})
        assert a != b

    def test_params_none_default(self):
        v = DatasetVersion(version="2026-03-09")
        assert v.params is None


class TestVersionRecord:
    def test_construction_minimal(self):
        record = VersionRecord(version="2026-03-09", published_at=_NOW)
        assert record.version == "2026-03-09"
        assert record.published_at == _NOW
        assert record.iceberg_snapshot_id is None
        assert record.iceberg_snapshot_timestamp is None
        assert record.params is None
        assert record.rows is None
        assert record.completed is False

    def test_construction_full(self):
        record = VersionRecord(
            version="2026-03-09",
            published_at=_NOW,
            iceberg_snapshot_id=987654321,
            iceberg_snapshot_timestamp=_NOW,
            params={"wstatus": "EMP"},
            rows=1_234_567,
            completed=True,
        )
        assert record.iceberg_snapshot_id == 987654321
        assert record.rows == 1_234_567
        assert record.completed is True

    def test_completed_default_false(self):
        record = VersionRecord(version="2026-03-09", published_at=_NOW)
        assert record.completed is False

    def test_frozen(self):
        record = VersionRecord(version="2026-03-09", published_at=_NOW)
        with pytest.raises((TypeError, ValidationError)):
            record.completed = True  # type: ignore[misc]

    def test_missing_version(self):
        with pytest.raises(ValidationError):
            VersionRecord(published_at=_NOW)  # type: ignore[call-arg]

    def test_missing_published_at(self):
        with pytest.raises(ValidationError):
            VersionRecord(version="2026-03-09")  # type: ignore[call-arg]

    def test_equality(self):
        a = VersionRecord(version="2026-03-09", published_at=_NOW, completed=True)
        b = VersionRecord(version="2026-03-09", published_at=_NOW, completed=True)
        assert a == b

    def test_inequality_completed(self):
        a = VersionRecord(version="2026-03-09", published_at=_NOW, completed=False)
        b = VersionRecord(version="2026-03-09", published_at=_NOW, completed=True)
        assert a != b
