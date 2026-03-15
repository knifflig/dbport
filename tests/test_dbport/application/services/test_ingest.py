"""Tests for application.services.ingest."""

from __future__ import annotations

from pathlib import Path

import pytest

from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter
from dbport.adapters.secondary.lock.toml import TomlLockAdapter
from dbport.application.services.ingest import IngestService
from dbport.domain.entities.input import InputDeclaration


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeCatalog:
    """Minimal ICatalog stub that injects data directly into DuckDB."""

    def __init__(self, snapshot_id=100, dbport_version: str | None = None):
        self._snapshot_id = snapshot_id
        self._dbport_version = dbport_version  # simulates an DBPort-published table
        self.ingest_called: list = []

    def resolve_input_snapshot(self, table_address, version):
        """Return DBPort version→snapshot if configured, else (None, None)."""
        if self._dbport_version is None:
            return None, None
        resolved = version or self._dbport_version
        return resolved, self._snapshot_id

    def current_snapshot(self, table_address):
        return self._snapshot_id, None

    def ingest_into_compute(self, declaration, compute, snapshot_id=None):
        """Seed two rows directly into DuckDB without any Arrow/S3 involvement."""
        self.ingest_called.append((declaration, snapshot_id))
        parts = declaration.table_address.split(".", 1)
        schema_name, table_name = parts if len(parts) == 2 else ("main", parts[0])
        compute.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        compute.execute(
            f"CREATE OR REPLACE TABLE {schema_name}.{table_name} AS "
            "SELECT 'DE' AS geo, 2020 AS year, 1.0 AS value "
            "UNION ALL "
            "SELECT 'FR', 2021, 2.0"
        )
        return 2

    def table_exists(self, table_address):
        return True

    def load_arrow_schema(self, table_address):
        return None

    def update_table_properties(self, table_address, properties):
        pass

    def update_column_docs(self, table_address, column_docs):
        pass

    def write_versioned(self, table_address, version, compute, overwrite=False):
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def compute(tmp_path: Path) -> DuckDBComputeAdapter:
    ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
    # Create the wifor schema for testing
    ad.execute("CREATE SCHEMA IF NOT EXISTS wifor")
    yield ad
    ad.close()


@pytest.fixture
def lock(tmp_path: Path) -> TomlLockAdapter:
    return TomlLockAdapter(tmp_path / "dbport.lock")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIngestServiceLoad:
    def test_loads_table_into_duckdb(self, compute, lock):
        catalog = _FakeCatalog(snapshot_id=100)
        svc = IngestService(catalog, compute, lock)
        declaration = InputDeclaration(table_address="wifor.emp")
        svc.execute(declaration)
        assert compute.relation_exists("wifor", "emp")

    def test_returns_ingest_record(self, compute, lock):
        catalog = _FakeCatalog(snapshot_id=100)
        svc = IngestService(catalog, compute, lock)
        declaration = InputDeclaration(table_address="wifor.emp")
        record = svc.execute(declaration)
        assert record.table_address == "wifor.emp"
        assert record.last_snapshot_id == 100
        assert record.rows_loaded == 2

    def test_record_persisted_to_lock(self, compute, lock):
        catalog = _FakeCatalog(snapshot_id=42)
        svc = IngestService(catalog, compute, lock)
        declaration = InputDeclaration(table_address="wifor.emp")
        svc.execute(declaration)
        records = lock.read_ingest_records()
        assert len(records) == 1
        assert records[0].last_snapshot_id == 42

    def test_filters_stored_in_record(self, compute, lock):
        catalog = _FakeCatalog(snapshot_id=7)
        svc = IngestService(catalog, compute, lock)
        declaration = InputDeclaration(table_address="wifor.emp", filters={"wstatus": "EMP"})
        record = svc.execute(declaration)
        assert record.filters == {"wstatus": "EMP"}


class TestIngestServiceSkip:
    def test_skips_when_snapshot_unchanged_and_relation_exists(self, compute, lock):
        catalog = _FakeCatalog(snapshot_id=100)
        svc = IngestService(catalog, compute, lock)
        declaration = InputDeclaration(table_address="wifor.emp")

        # First load
        svc.execute(declaration)
        assert compute.relation_exists("wifor", "emp")

        # Second load — should be skipped (snapshot unchanged)
        # Overwrite catalog.scan_to_arrow_batches to detect if called again
        scanned = []

        def patched_scan(decl):
            scanned.append(decl)
            return iter([])

        catalog.scan_to_arrow_batches = patched_scan
        svc.execute(declaration)
        assert scanned == []  # scan not called → skipped

    def test_re_ingests_when_snapshot_changes(self, compute, lock):
        catalog = _FakeCatalog(snapshot_id=100)
        svc = IngestService(catalog, compute, lock)
        declaration = InputDeclaration(table_address="wifor.emp")

        # First load at snapshot 100
        svc.execute(declaration)

        # Snapshot changes to 200
        catalog._snapshot_id = 200
        record = svc.execute(declaration)
        assert record.last_snapshot_id == 200


class TestIngestServiceVersionPinning:
    """Fix A + Fix C: snapshot pinning and version resolution."""

    def test_non_dbport_table_falls_back_to_current_snapshot(self, compute, lock):
        """Tables without DBPort metadata use current_snapshot() as before."""
        catalog = _FakeCatalog(snapshot_id=55, dbport_version=None)
        svc = IngestService(catalog, compute, lock)
        record = svc.execute(InputDeclaration(table_address="wifor.emp"))
        assert record.last_snapshot_id == 55
        assert record.version is None

    def test_dbport_table_resolves_latest_version(self, compute, lock):
        """DBPort tables resolve the latest version from metadata."""
        catalog = _FakeCatalog(snapshot_id=99, dbport_version="2026-01-01")
        svc = IngestService(catalog, compute, lock)
        record = svc.execute(InputDeclaration(table_address="wifor.emp"))
        assert record.last_snapshot_id == 99
        assert record.version == "2026-01-01"

    def test_explicit_version_pin_is_forwarded(self, compute, lock):
        """An explicit version is forwarded to resolve_input_snapshot."""
        catalog = _FakeCatalog(snapshot_id=77, dbport_version="2026-01-01")
        svc = IngestService(catalog, compute, lock)
        record = svc.execute(
            InputDeclaration(table_address="wifor.emp", version="2025-01-01")
        )
        assert record.version == "2025-01-01"
        assert record.last_snapshot_id == 77

    def test_snapshot_id_passed_to_ingest(self, compute, lock):
        """Resolved snapshot_id is forwarded to ingest_into_compute (Fix A)."""
        catalog = _FakeCatalog(snapshot_id=42, dbport_version="2026-01-01")
        svc = IngestService(catalog, compute, lock)
        svc.execute(InputDeclaration(table_address="wifor.emp"))
        _decl, snap_id = catalog.ingest_called[-1]
        assert snap_id == 42

    def test_non_dbport_snapshot_id_also_forwarded(self, compute, lock):
        """Even for non-DBPort tables the current snapshot_id is forwarded."""
        catalog = _FakeCatalog(snapshot_id=33, dbport_version=None)
        svc = IngestService(catalog, compute, lock)
        svc.execute(InputDeclaration(table_address="wifor.emp"))
        _decl, snap_id = catalog.ingest_called[-1]
        assert snap_id == 33

    def test_version_persisted_in_lock(self, compute, lock):
        """Resolved version is written into the IngestRecord in dbport.lock."""
        catalog = _FakeCatalog(snapshot_id=10, dbport_version="2026-03-14")
        svc = IngestService(catalog, compute, lock)
        svc.execute(InputDeclaration(table_address="wifor.emp"))
        records = lock.read_ingest_records()
        assert records[0].version == "2026-03-14"


class TestIngestServiceConnectionErrors:
    """Test error handling in the ingest service."""

    def test_resolve_snapshot_exception_falls_back_to_current_snapshot(self, compute, lock):
        """Non-ValueError from resolve_input_snapshot triggers fallback to current_snapshot."""

        class _ErrorCatalog(_FakeCatalog):
            def resolve_input_snapshot(self, table_address, version):
                raise ConnectionError("catalog unreachable")

        catalog = _ErrorCatalog(snapshot_id=55)
        svc = IngestService(catalog, compute, lock)
        record = svc.execute(InputDeclaration(table_address="wifor.emp"))
        # Should fall back to current_snapshot() which returns 55
        assert record.last_snapshot_id == 55
        assert record.rows_loaded == 2

    def test_explicit_version_not_found_propagates_value_error(self, compute, lock):
        """ValueError from resolve_input_snapshot (explicit version not found) propagates."""

        class _VersionErrorCatalog(_FakeCatalog):
            def resolve_input_snapshot(self, table_address, version):
                raise ValueError("Version '2020-01-01' not found")

        catalog = _VersionErrorCatalog(snapshot_id=55)
        svc = IngestService(catalog, compute, lock)
        with pytest.raises(ValueError, match="not found"):
            svc.execute(InputDeclaration(table_address="wifor.emp", version="2020-01-01"))

    def test_ingest_catalog_error_propagates(self, compute, lock):
        """Errors from ingest_into_compute are not swallowed."""

        class _IngestErrorCatalog(_FakeCatalog):
            def ingest_into_compute(self, declaration, compute, snapshot_id=None):
                raise RuntimeError("Arrow stream failed")

        catalog = _IngestErrorCatalog(snapshot_id=100)
        svc = IngestService(catalog, compute, lock)
        with pytest.raises(RuntimeError, match="Arrow stream failed"):
            svc.execute(InputDeclaration(table_address="wifor.emp"))
