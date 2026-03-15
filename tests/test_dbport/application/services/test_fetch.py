"""Tests for application.services.fetch."""

from __future__ import annotations

from datetime import UTC, datetime

from dbport.application.services.fetch import FetchService
from dbport.domain.entities.dataset import DatasetKey


class _FakeCatalog:
    def __init__(self, exists=True):
        self._exists = exists
        self.properties_written: list[dict] = []

    def table_exists(self, table_address):
        return self._exists

    def update_table_properties(self, table_address, properties):
        self.properties_written.append({"table": table_address, "props": properties})

    def current_snapshot(self, table_address):
        return None, None

    def load_arrow_schema(self, table_address):
        return None


class TestFetchService:
    def _key(self) -> DatasetKey:
        return DatasetKey(agency="wifor", dataset_id="emp")

    def test_returns_utc_datetime(self):
        catalog = _FakeCatalog(exists=True)
        svc = FetchService(self._key(), catalog)
        result = svc.execute()
        assert isinstance(result, datetime)
        assert result.tzinfo == UTC

    def test_writes_last_fetched_at_property(self):
        catalog = _FakeCatalog(exists=True)
        svc = FetchService(self._key(), catalog)
        svc.execute()
        assert len(catalog.properties_written) == 1
        props = catalog.properties_written[0]["props"]
        assert "dbport.last_fetched_at" in props

    def test_property_written_to_correct_table(self):
        catalog = _FakeCatalog(exists=True)
        svc = FetchService(self._key(), catalog)
        svc.execute()
        assert catalog.properties_written[0]["table"] == "wifor.emp"

    def test_no_error_when_table_does_not_exist(self):
        catalog = _FakeCatalog(exists=False)
        svc = FetchService(self._key(), catalog)
        result = svc.execute()  # should not raise
        assert isinstance(result, datetime)
        assert catalog.properties_written == []

    def test_timestamp_string_is_iso_format_with_z(self):
        catalog = _FakeCatalog(exists=True)
        svc = FetchService(self._key(), catalog)
        svc.execute()
        ts_str = catalog.properties_written[0]["props"]["dbport.last_fetched_at"]
        assert ts_str.endswith("Z")


class TestFetchServiceErrorHandling:
    def _key(self) -> DatasetKey:
        return DatasetKey(agency="wifor", dataset_id="emp")

    def test_table_exists_exception_silently_caught(self):
        """When table_exists raises, FetchService does not propagate."""

        class _ErrorCatalog:
            def table_exists(self, table_address):
                raise ConnectionError("network error")

            def update_table_properties(self, table_address, properties):
                pass

        catalog = _ErrorCatalog()
        svc = FetchService(self._key(), catalog)
        result = svc.execute()  # should not raise
        assert isinstance(result, datetime)

    def test_update_properties_exception_silently_caught(self):
        """When update_table_properties raises, FetchService does not propagate."""

        class _UpdateErrorCatalog:
            def table_exists(self, table_address):
                return True

            def update_table_properties(self, table_address, properties):
                raise RuntimeError("catalog write failed")

        catalog = _UpdateErrorCatalog()
        svc = FetchService(self._key(), catalog)
        result = svc.execute()  # should not raise
        assert isinstance(result, datetime)
