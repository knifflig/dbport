"""Tests for adapters.primary.columns."""

from __future__ import annotations

import pytest

from dbport.adapters.primary.columns import ColumnConfig, ColumnRegistry
from dbport.domain.entities.codelist import CodelistEntry


# ---------------------------------------------------------------------------
# In-memory ILockStore test double
# ---------------------------------------------------------------------------

class _InMemoryLock:
    """Minimal in-memory ILockStore for testing."""

    def __init__(self, entries: dict[str, CodelistEntry] | None = None) -> None:
        self._entries: dict[str, CodelistEntry] = dict(entries or {})

    def read_schema(self):
        return None

    def write_schema(self, schema) -> None:
        pass

    def read_codelist_entries(self) -> dict[str, CodelistEntry]:
        return dict(self._entries)

    def write_codelist_entry(self, entry: CodelistEntry) -> None:
        self._entries[entry.column_name] = entry

    def read_ingest_records(self):
        return []

    def write_ingest_record(self, record) -> None:
        pass

    def read_versions(self):
        return []

    def append_version(self, record) -> None:
        pass


def _make_existing_entry(column_name: str = "geo", pos: int = 0) -> CodelistEntry:
    return CodelistEntry(
        column_name=column_name,
        column_pos=pos,
        codelist_id=column_name,
    )


# ---------------------------------------------------------------------------
# ColumnConfig.meta() tests
# ---------------------------------------------------------------------------

class TestColumnConfigMeta:
    def test_meta_codelist_id_updates_id(self):
        lock = _InMemoryLock({"geo": _make_existing_entry("geo")})
        cfg = ColumnConfig("geo", lock)
        cfg.meta(codelist_id="NUTS2024")
        entry = lock.read_codelist_entries()["geo"]
        assert entry.codelist_id == "NUTS2024"

    def test_meta_codelist_kind(self):
        lock = _InMemoryLock({"geo": _make_existing_entry("geo")})
        cfg = ColumnConfig("geo", lock)
        cfg.meta(codelist_kind="hierarchical")
        entry = lock.read_codelist_entries()["geo"]
        assert entry.codelist_kind == "hierarchical"

    def test_meta_codelist_type(self):
        lock = _InMemoryLock({"geo": _make_existing_entry("geo")})
        cfg = ColumnConfig("geo", lock)
        cfg.meta(codelist_type="categorical")
        entry = lock.read_codelist_entries()["geo"]
        assert entry.codelist_type == "categorical"

    def test_meta_codelist_labels(self):
        lock = _InMemoryLock({"geo": _make_existing_entry("geo")})
        cfg = ColumnConfig("geo", lock)
        cfg.meta(codelist_labels={"en": "Geography", "de": "Geographie"})
        entry = lock.read_codelist_entries()["geo"]
        assert entry.codelist_labels == {"en": "Geography", "de": "Geographie"}

    def test_meta_all_fields(self):
        lock = _InMemoryLock({"geo": _make_existing_entry("geo")})
        cfg = ColumnConfig("geo", lock)
        cfg.meta(
            codelist_id="NUTS2024",
            codelist_type="categorical",
            codelist_kind="hierarchical",
            codelist_labels={"en": "NUTS 2024"},
        )
        entry = lock.read_codelist_entries()["geo"]
        assert entry.codelist_id == "NUTS2024"
        assert entry.codelist_type == "categorical"
        assert entry.codelist_kind == "hierarchical"
        assert entry.codelist_labels == {"en": "NUTS 2024"}

    def test_meta_creates_entry_when_none_exists(self):
        lock = _InMemoryLock()
        cfg = ColumnConfig("geo", lock)
        cfg.meta(codelist_kind="hierarchical")
        entry = lock.read_codelist_entries()["geo"]
        assert entry.column_name == "geo"
        assert entry.codelist_kind == "hierarchical"

    def test_meta_preserves_existing_fields(self):
        existing = CodelistEntry(
            column_name="geo",
            column_pos=2,
            codelist_id="NUTS2024",
            attach_table="wifor.cl_nuts2024",
        )
        lock = _InMemoryLock({"geo": existing})
        cfg = ColumnConfig("geo", lock)
        cfg.meta(codelist_kind="hierarchical")
        entry = lock.read_codelist_entries()["geo"]
        assert entry.column_pos == 2
        assert entry.codelist_id == "NUTS2024"
        assert entry.attach_table == "wifor.cl_nuts2024"
        assert entry.codelist_kind == "hierarchical"

    def test_meta_returns_self(self):
        lock = _InMemoryLock()
        cfg = ColumnConfig("geo", lock)
        result = cfg.meta(codelist_id="NUTS2024")
        assert result is cfg

    def test_meta_chaining(self):
        lock = _InMemoryLock()
        cfg = ColumnConfig("geo", lock)
        cfg.meta(codelist_id="NUTS2024").meta(codelist_kind="hierarchical")
        entry = lock.read_codelist_entries()["geo"]
        assert entry.codelist_id == "NUTS2024"
        assert entry.codelist_kind == "hierarchical"

    def test_meta_no_args_is_noop(self):
        existing = _make_existing_entry("geo")
        lock = _InMemoryLock({"geo": existing})
        cfg = ColumnConfig("geo", lock)
        cfg.meta()
        entry = lock.read_codelist_entries()["geo"]
        assert entry == existing


# ---------------------------------------------------------------------------
# ColumnConfig.attach() tests
# ---------------------------------------------------------------------------

class TestColumnConfigAttach:
    def test_attach_sets_attach_table(self):
        lock = _InMemoryLock({"geo": _make_existing_entry("geo")})
        cfg = ColumnConfig("geo", lock)
        cfg.attach(table="wifor.cl_nuts2024")
        entry = lock.read_codelist_entries()["geo"]
        assert entry.attach_table == "wifor.cl_nuts2024"

    def test_attach_creates_entry_when_none_exists(self):
        lock = _InMemoryLock()
        cfg = ColumnConfig("geo", lock)
        cfg.attach(table="wifor.cl_nuts2024")
        entry = lock.read_codelist_entries()["geo"]
        assert entry.column_name == "geo"
        assert entry.attach_table == "wifor.cl_nuts2024"

    def test_attach_preserves_existing_meta(self):
        existing = CodelistEntry(
            column_name="geo",
            column_pos=1,
            codelist_id="NUTS2024",
            codelist_kind="hierarchical",
        )
        lock = _InMemoryLock({"geo": existing})
        cfg = ColumnConfig("geo", lock)
        cfg.attach(table="wifor.cl_nuts2024")
        entry = lock.read_codelist_entries()["geo"]
        assert entry.codelist_id == "NUTS2024"
        assert entry.codelist_kind == "hierarchical"
        assert entry.attach_table == "wifor.cl_nuts2024"

    def test_attach_returns_self(self):
        lock = _InMemoryLock()
        cfg = ColumnConfig("geo", lock)
        result = cfg.attach(table="wifor.cl_nuts2024")
        assert result is cfg

    def test_attach_after_meta_chaining(self):
        lock = _InMemoryLock()
        cfg = ColumnConfig("geo", lock)
        cfg.meta(codelist_id="NUTS2024").attach(table="wifor.cl_nuts2024")
        entry = lock.read_codelist_entries()["geo"]
        assert entry.codelist_id == "NUTS2024"
        assert entry.attach_table == "wifor.cl_nuts2024"


# ---------------------------------------------------------------------------
# ColumnRegistry tests
# ---------------------------------------------------------------------------

class TestColumnRegistry:
    def test_attribute_access_returns_column_config(self):
        lock = _InMemoryLock()
        registry = ColumnRegistry(lock)
        col = registry.geo
        assert isinstance(col, ColumnConfig)

    def test_same_column_returns_same_instance(self):
        lock = _InMemoryLock()
        registry = ColumnRegistry(lock)
        assert registry.geo is registry.geo

    def test_different_columns_are_different_instances(self):
        lock = _InMemoryLock()
        registry = ColumnRegistry(lock)
        assert registry.geo is not registry.year

    def test_private_attribute_raises_attribute_error(self):
        lock = _InMemoryLock()
        registry = ColumnRegistry(lock)
        with pytest.raises(AttributeError):
            _ = registry._private

    def test_refresh_clears_cache(self):
        lock = _InMemoryLock()
        registry = ColumnRegistry(lock)
        original = registry.geo
        registry._refresh()
        refreshed = registry.geo
        assert original is not refreshed

    def test_column_name_passed_to_config(self):
        lock = _InMemoryLock()
        registry = ColumnRegistry(lock)
        cfg = registry.my_column
        assert cfg._name == "my_column"

    def test_lock_passed_to_config(self):
        lock = _InMemoryLock()
        registry = ColumnRegistry(lock)
        cfg = registry.geo
        assert cfg._lock is lock
