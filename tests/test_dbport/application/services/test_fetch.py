"""Tests for application.services.fetch."""

from __future__ import annotations

from datetime import UTC, datetime

from dbport.application.services.fetch import FetchService
from dbport.domain.entities.dataset import DatasetKey


class _FakeCatalog:
    """Minimal ICatalog stub for fetch tests."""

    def __init__(self, *, exists: bool = True) -> None:
        self._exists = exists
        self.properties_written: list[dict[str, str | dict[str, str]]] = []

    def table_exists(self, table_address: str) -> bool:
        """Check if table exists."""
        return self._exists

    def update_table_properties(
        self,
        table_address: str,
        properties: dict[str, str],
    ) -> None:
        """Record written properties."""
        self.properties_written.append({"table": table_address, "props": properties})

    def current_snapshot(self, table_address: str) -> tuple[None, None]:
        """Return empty snapshot."""
        return None, None

    def load_arrow_schema(self, table_address: str) -> None:
        """Return no schema."""
        return None


class TestFetchService:
    """Tests for FetchService."""

    def _key(self) -> DatasetKey:
        """Build a test dataset key."""
        return DatasetKey(agency="wifor", dataset_id="emp")

    def test_returns_utc_datetime(self) -> None:
        """Test that execute returns a UTC datetime."""
        catalog = _FakeCatalog(exists=True)
        svc = FetchService(self._key(), catalog)
        result = svc.execute()
        assert isinstance(result, datetime)
        assert result.tzinfo == UTC

    def test_writes_last_fetched_at_property(self) -> None:
        """Test that last_fetched_at property is written."""
        catalog = _FakeCatalog(exists=True)
        svc = FetchService(self._key(), catalog)
        svc.execute()
        assert len(catalog.properties_written) == 1
        props = catalog.properties_written[0]["props"]
        assert "dbport.last_fetched_at" in props

    def test_property_written_to_correct_table(self) -> None:
        """Test property is written to the correct table address."""
        catalog = _FakeCatalog(exists=True)
        svc = FetchService(self._key(), catalog)
        svc.execute()
        assert catalog.properties_written[0]["table"] == "wifor.emp"

    def test_no_error_when_table_does_not_exist(self) -> None:
        """Test no error raised when table does not exist."""
        catalog = _FakeCatalog(exists=False)
        svc = FetchService(self._key(), catalog)
        result = svc.execute()  # should not raise
        assert isinstance(result, datetime)
        assert catalog.properties_written == []

    def test_timestamp_string_is_iso_format_with_z(self) -> None:
        """Test timestamp string ends with Z."""
        catalog = _FakeCatalog(exists=True)
        svc = FetchService(self._key(), catalog)
        svc.execute()
        ts_str = catalog.properties_written[0]["props"]["dbport.last_fetched_at"]
        assert ts_str.endswith("Z")


class TestFetchServiceErrorHandling:
    """Tests for FetchService error handling."""

    def _key(self) -> DatasetKey:
        """Build a test dataset key."""
        return DatasetKey(agency="wifor", dataset_id="emp")

    def test_table_exists_exception_silently_caught(self) -> None:
        """When table_exists raises, FetchService does not propagate."""

        class _ErrorCatalog:
            def table_exists(self, table_address: str) -> bool:
                raise ConnectionError("network error")

            def update_table_properties(
                self,
                table_address: str,
                properties: dict[str, str],
            ) -> None:
                pass

        catalog = _ErrorCatalog()
        svc = FetchService(self._key(), catalog)
        result = svc.execute()  # should not raise
        assert isinstance(result, datetime)

    def test_update_properties_exception_silently_caught(self) -> None:
        """When update_table_properties raises, FetchService does not propagate."""

        class _UpdateErrorCatalog:
            def table_exists(self, table_address: str) -> bool:
                return True

            def update_table_properties(
                self,
                table_address: str,
                properties: dict[str, str],
            ) -> None:
                raise RuntimeError("catalog write failed")

        catalog = _UpdateErrorCatalog()
        svc = FetchService(self._key(), catalog)
        result = svc.execute()  # should not raise
        assert isinstance(result, datetime)
