"""Tests for adapters.secondary.lock.toml."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from dbport.adapters.secondary.lock.toml import TomlLockAdapter
from dbport.domain.entities.codelist import CodelistEntry
from dbport.domain.entities.input import IngestRecord
from dbport.domain.entities.schema import ColumnDef, DatasetSchema, SqlDdl
from dbport.domain.entities.version import VersionRecord

_DDL = "CREATE OR REPLACE TABLE wifor.emp (geo VARCHAR, year SMALLINT, value DOUBLE)"
_DDL_MULTILINE = (
    "CREATE OR REPLACE TABLE wifor.emp (\n"
    "    geo   VARCHAR NOT NULL,\n"
    "    year  SMALLINT NOT NULL,\n"
    "    value DOUBLE\n"
    ");"
)
_NOW = datetime(2026, 3, 9, 14, 32, 0, tzinfo=UTC)


@pytest.fixture
def lock(tmp_path: Path) -> TomlLockAdapter:
    return TomlLockAdapter(
        tmp_path / "dbport.lock",
        model_key="wifor.emp",
        model_root="models/emp",
        duckdb_path="models/emp/data/emp.duckdb",
    )


def _make_schema() -> DatasetSchema:
    return DatasetSchema(
        ddl=SqlDdl(statement=_DDL),
        columns=(
            ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),
            ColumnDef(name="year", pos=1, sql_type="SMALLINT"),
            ColumnDef(name="value", pos=2, sql_type="DOUBLE"),
        ),
    )


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class TestTomlLockSchema:
    def test_read_schema_empty_file(self, lock: TomlLockAdapter):
        assert lock.read_schema() is None

    def test_read_schema_nonexistent_file(self, tmp_path: Path):
        adapter = TomlLockAdapter(
            tmp_path / "nonexistent.lock",
            model_key="wifor.emp",
            model_root=".",
            duckdb_path="",
        )
        assert adapter.read_schema() is None

    def test_write_and_read_schema(self, lock: TomlLockAdapter):
        schema = _make_schema()
        lock.write_schema(schema)
        result = lock.read_schema()
        assert result is not None
        assert result.ddl.statement == _DDL
        assert len(result.columns) == 3
        assert result.columns[0].name == "geo"
        assert result.columns[0].sql_type == "VARCHAR"
        assert result.columns[1].name == "year"
        assert result.columns[2].name == "value"

    def test_write_schema_persists_to_file(self, lock: TomlLockAdapter):
        lock.write_schema(_make_schema())
        assert lock._path.exists()

    def test_write_schema_overwrites(self, lock: TomlLockAdapter):
        lock.write_schema(_make_schema())
        new_ddl = "CREATE TABLE wifor.emp (id INT)"
        new_schema = DatasetSchema(
            ddl=SqlDdl(statement=new_ddl),
            columns=(ColumnDef(name="id", pos=0, sql_type="INT"),),
        )
        lock.write_schema(new_schema)
        result = lock.read_schema()
        assert result.ddl.statement == new_ddl
        assert len(result.columns) == 1

    def test_write_schema_seeds_default_codelist_ids(self, lock: TomlLockAdapter):
        lock.write_schema(_make_schema())
        entries = lock.read_codelist_entries()
        assert "geo" in entries
        assert entries["geo"].codelist_id == "geo"
        assert entries["year"].codelist_id == "year"

    def test_model_header_written_to_lock_file(self, lock: TomlLockAdapter):
        """Model metadata (agency, dataset_id, model_root) appears in the file."""
        lock.write_schema(_make_schema())
        content = lock._path.read_text()
        assert "wifor" in content
        assert "models/emp" in content

    def test_multiline_ddl_roundtrip(self, lock: TomlLockAdapter):
        """Multi-line DDL must survive write → file → read without TOML parse errors."""
        schema = DatasetSchema(
            ddl=SqlDdl(statement=_DDL_MULTILINE),
            columns=(
                ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),
                ColumnDef(name="year", pos=1, sql_type="SMALLINT"),
                ColumnDef(name="value", pos=2, sql_type="DOUBLE"),
            ),
        )
        lock.write_schema(schema)
        # File must be valid TOML (no parse error)
        result = lock.read_schema()
        assert result is not None
        assert result.ddl.statement == _DDL_MULTILINE


# ---------------------------------------------------------------------------
# Codelist entries
# ---------------------------------------------------------------------------

class TestTomlLockCodelistEntries:
    def test_read_empty_returns_empty_dict(self, lock: TomlLockAdapter):
        assert lock.read_codelist_entries() == {}

    def test_write_and_read_entry(self, lock: TomlLockAdapter):
        lock.write_schema(_make_schema())
        entry = CodelistEntry(
            column_name="geo",
            column_pos=0,
            codelist_id="NUTS2024",

            codelist_kind="hierarchical",
        )
        lock.write_codelist_entry(entry)
        entries = lock.read_codelist_entries()
        assert entries["geo"].codelist_id == "NUTS2024"
        assert entries["geo"].codelist_kind == "hierarchical"

    def test_write_entry_upserts_by_column_name(self, lock: TomlLockAdapter):
        lock.write_schema(_make_schema())
        e1 = CodelistEntry(
            column_name="geo", column_pos=0, codelist_id="geo"
        )
        e2 = CodelistEntry(
            column_name="geo", column_pos=0, codelist_id="NUTS2024"
        )
        lock.write_codelist_entry(e1)
        lock.write_codelist_entry(e2)
        entries = lock.read_codelist_entries()
        assert entries["geo"].codelist_id == "NUTS2024"

    def test_write_entry_for_new_column(self, lock: TomlLockAdapter):
        entry = CodelistEntry(
            column_name="new_col", column_pos=5, codelist_id="new_col"
        )
        lock.write_codelist_entry(entry)
        entries = lock.read_codelist_entries()
        assert "new_col" in entries

    def test_attach_table_persisted(self, lock: TomlLockAdapter):
        lock.write_schema(_make_schema())
        entry = CodelistEntry(
            column_name="geo",
            column_pos=0,
            codelist_id="NUTS2024",

            attach_table="wifor.cl_nuts2024",
        )
        lock.write_codelist_entry(entry)
        entries = lock.read_codelist_entries()
        assert entries["geo"].attach_table == "wifor.cl_nuts2024"

    def test_labels_persisted(self, lock: TomlLockAdapter):
        lock.write_schema(_make_schema())
        entry = CodelistEntry(
            column_name="geo",
            column_pos=0,
            codelist_id="NUTS2024",

            codelist_labels={"en": "NUTS 2024", "de": "NUTS 2024 DE"},
        )
        lock.write_codelist_entry(entry)
        entries = lock.read_codelist_entries()
        assert entries["geo"].codelist_labels == {"en": "NUTS 2024", "de": "NUTS 2024 DE"}

    def test_sql_type_preserved_after_codelist_update(self, lock: TomlLockAdapter):
        lock.write_schema(_make_schema())
        entry = CodelistEntry(
            column_name="geo", column_pos=0, codelist_id="NUTS2024"
        )
        lock.write_codelist_entry(entry)
        result = lock.read_schema()
        assert result.columns[0].sql_type == "VARCHAR"


# ---------------------------------------------------------------------------
# Ingest records
# ---------------------------------------------------------------------------

class TestTomlLockIngestRecords:
    def test_read_empty(self, lock: TomlLockAdapter):
        assert lock.read_ingest_records() == []

    def test_write_and_read_record(self, lock: TomlLockAdapter):
        record = IngestRecord(
            table_address="estat.foo",
            last_snapshot_id=123,
            rows_loaded=500,
            filters={"wstatus": "EMP"},
        )
        lock.write_ingest_record(record)
        records = lock.read_ingest_records()
        assert len(records) == 1
        assert records[0].table_address == "estat.foo"
        assert records[0].last_snapshot_id == 123
        assert records[0].rows_loaded == 500
        assert records[0].filters == {"wstatus": "EMP"}

    def test_write_multiple_records(self, lock: TomlLockAdapter):
        lock.write_ingest_record(IngestRecord(table_address="estat.foo", last_snapshot_id=1))
        lock.write_ingest_record(IngestRecord(table_address="wifor.bar", last_snapshot_id=2))
        records = lock.read_ingest_records()
        addresses = {r.table_address for r in records}
        assert addresses == {"estat.foo", "wifor.bar"}

    def test_write_record_upserts_by_address(self, lock: TomlLockAdapter):
        lock.write_ingest_record(IngestRecord(table_address="estat.foo", last_snapshot_id=1))
        lock.write_ingest_record(IngestRecord(table_address="estat.foo", last_snapshot_id=999))
        records = lock.read_ingest_records()
        assert len(records) == 1
        assert records[0].last_snapshot_id == 999

    def test_write_record_without_snapshot(self, lock: TomlLockAdapter):
        lock.write_ingest_record(IngestRecord(table_address="estat.foo"))
        records = lock.read_ingest_records()
        assert records[0].last_snapshot_id is None


# ---------------------------------------------------------------------------
# Versions
# ---------------------------------------------------------------------------

class TestTomlLockVersions:
    def test_read_empty(self, lock: TomlLockAdapter):
        assert lock.read_versions() == []

    def test_append_and_read_version(self, lock: TomlLockAdapter):
        record = VersionRecord(
            version="2026-03-09",
            published_at=_NOW,
            rows=1_234,
            completed=True,
        )
        lock.append_version(record)
        versions = lock.read_versions()
        assert len(versions) == 1
        assert versions[0].version == "2026-03-09"
        assert versions[0].rows == 1_234
        assert versions[0].completed is True
        assert versions[0].published_at == _NOW

    def test_append_multiple_versions(self, lock: TomlLockAdapter):
        lock.append_version(VersionRecord(version="2026-01-01", published_at=_NOW))
        lock.append_version(VersionRecord(version="2026-03-09", published_at=_NOW))
        versions = lock.read_versions()
        assert len(versions) == 2
        assert versions[0].version == "2026-01-01"
        assert versions[1].version == "2026-03-09"

    def test_append_version_upserts_by_version_string(self, lock: TomlLockAdapter):
        lock.append_version(VersionRecord(version="2026-03-09", published_at=_NOW, completed=False))
        lock.append_version(VersionRecord(version="2026-03-09", published_at=_NOW, completed=True))
        versions = lock.read_versions()
        assert len(versions) == 1
        assert versions[0].completed is True

    def test_version_with_params(self, lock: TomlLockAdapter):
        record = VersionRecord(
            version="2026-03-09",
            published_at=_NOW,
            params={"wstatus": "EMP", "nace_r2": "TOTAL"},
        )
        lock.append_version(record)
        versions = lock.read_versions()
        assert versions[0].params == {"wstatus": "EMP", "nace_r2": "TOTAL"}

    def test_version_with_iceberg_snapshot(self, lock: TomlLockAdapter):
        record = VersionRecord(
            version="2026-03-09",
            published_at=_NOW,
            iceberg_snapshot_id=987654321,
            iceberg_snapshot_timestamp=_NOW,
        )
        lock.append_version(record)
        versions = lock.read_versions()
        assert versions[0].iceberg_snapshot_id == 987654321
        assert versions[0].iceberg_snapshot_timestamp == _NOW


# ---------------------------------------------------------------------------
# Multi-model isolation
# ---------------------------------------------------------------------------

class TestTomlLockMultiModel:
    def _make_lock(self, tmp_path: Path, model_key: str) -> TomlLockAdapter:
        return TomlLockAdapter(
            tmp_path / "dbport.lock",
            model_key=model_key,
            model_root=f"models/{model_key.split('.', 1)[-1]}",
            duckdb_path=f"models/{model_key.split('.', 1)[-1]}/data/db.duckdb",
        )

    def test_two_models_share_one_file(self, tmp_path: Path):
        lock_a = self._make_lock(tmp_path, "wifor.emp")
        lock_b = self._make_lock(tmp_path, "wifor.sector")

        lock_a.write_schema(DatasetSchema(
            ddl=SqlDdl(statement="CREATE TABLE wifor.emp (geo VARCHAR)"),
            columns=(ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),),
        ))
        lock_b.write_schema(DatasetSchema(
            ddl=SqlDdl(statement="CREATE TABLE wifor.sector (nace VARCHAR)"),
            columns=(ColumnDef(name="nace", pos=0, sql_type="VARCHAR"),),
        ))

        assert lock_a._path == lock_b._path  # same file

    def test_models_are_isolated(self, tmp_path: Path):
        lock_a = self._make_lock(tmp_path, "wifor.emp")
        lock_b = self._make_lock(tmp_path, "wifor.sector")

        lock_a.write_schema(DatasetSchema(
            ddl=SqlDdl(statement="CREATE TABLE wifor.emp (geo VARCHAR)"),
            columns=(ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),),
        ))
        lock_b.write_schema(DatasetSchema(
            ddl=SqlDdl(statement="CREATE TABLE wifor.sector (nace VARCHAR)"),
            columns=(ColumnDef(name="nace", pos=0, sql_type="VARCHAR"),),
        ))

        schema_a = lock_a.read_schema()
        schema_b = lock_b.read_schema()
        assert "geo" in schema_a.ddl.statement
        assert "nace" in schema_b.ddl.statement
        assert "nace" not in schema_a.ddl.statement
        assert "geo" not in schema_b.ddl.statement

    def test_versions_isolated_per_model(self, tmp_path: Path):
        lock_a = self._make_lock(tmp_path, "wifor.emp")
        lock_b = self._make_lock(tmp_path, "wifor.sector")

        lock_a.append_version(VersionRecord(version="2026-03-09", published_at=_NOW))
        assert lock_b.read_versions() == []

    def test_inputs_isolated_per_model(self, tmp_path: Path):
        lock_a = self._make_lock(tmp_path, "wifor.emp")
        lock_b = self._make_lock(tmp_path, "wifor.sector")

        lock_a.write_ingest_record(IngestRecord(table_address="estat.foo", last_snapshot_id=1))
        assert lock_b.read_ingest_records() == []

    def test_model_root_stored_in_file(self, tmp_path: Path):
        lock_a = self._make_lock(tmp_path, "wifor.emp")
        lock_a.write_schema(DatasetSchema(
            ddl=SqlDdl(statement="CREATE TABLE wifor.emp (geo VARCHAR)"),
            columns=(ColumnDef(name="geo", pos=0, sql_type="VARCHAR"),),
        ))
        content = lock_a._path.read_text()
        assert "models/emp" in content
        assert 'models."wifor.emp"' in content


# ---------------------------------------------------------------------------
# Round-trip: write then read via new adapter instance (file persistence)
# ---------------------------------------------------------------------------

class TestTomlLockRoundTrip:
    def test_full_roundtrip(self, tmp_path: Path):
        path = tmp_path / "dbport.lock"
        lock1 = TomlLockAdapter(path, model_key="wifor.emp", model_root="models/emp", duckdb_path="")
        lock1.write_schema(_make_schema())
        lock1.write_ingest_record(IngestRecord(table_address="estat.foo", last_snapshot_id=1))
        lock1.append_version(VersionRecord(version="2026-03-09", published_at=_NOW, completed=True))

        lock2 = TomlLockAdapter(path, model_key="wifor.emp", model_root="models/emp", duckdb_path="")
        assert lock2.read_schema() is not None
        assert len(lock2.read_ingest_records()) == 1
        assert len(lock2.read_versions()) == 1
        assert lock2.read_versions()[0].completed is True

    def test_roundtrip_preserves_other_model(self, tmp_path: Path):
        path = tmp_path / "dbport.lock"
        lock_a = TomlLockAdapter(path, model_key="wifor.emp", model_root="models/emp", duckdb_path="")
        lock_b = TomlLockAdapter(path, model_key="wifor.sector", model_root="models/sector", duckdb_path="")

        lock_a.write_schema(_make_schema())
        lock_b.write_ingest_record(IngestRecord(table_address="estat.foo", last_snapshot_id=99))

        # Reloading lock_a should not lose lock_b's data
        lock_a.append_version(VersionRecord(version="2026-03-09", published_at=_NOW))

        lock_b2 = TomlLockAdapter(path, model_key="wifor.sector", model_root="models/sector", duckdb_path="")
        assert len(lock_b2.read_ingest_records()) == 1
        assert lock_b2.read_ingest_records()[0].last_snapshot_id == 99


# ---------------------------------------------------------------------------
# TOML serialization edge cases
# ---------------------------------------------------------------------------

class TestTomlSerializationEdgeCases:
    """Cover edge cases in the TOML serializer."""

    def test_float_value_serialized(self):
        """Float values use repr() for precision."""
        from dbport.adapters.secondary.lock.toml import _toml_value
        assert _toml_value(3.14) == repr(3.14)

    def test_unknown_type_falls_back_to_string(self):
        """Unknown types are converted to string."""
        from dbport.adapters.secondary.lock.toml import _toml_value
        result = _toml_value([1, 2, 3])  # list is not handled directly
        assert '"[1, 2, 3]"' == result

    def test_none_values_skipped_in_section(self):
        """None values are omitted from TOML sections."""
        from dbport.adapters.secondary.lock.toml import _write_section
        lines: list[str] = []
        _write_section(lines, "test", {"key1": "val", "key2": None, "key3": 42})
        text = "\n".join(lines)
        assert "key1" in text
        assert "key2" not in text
        assert "key3" in text


# ---------------------------------------------------------------------------
# Legacy/flat mode (model_key="") with versions
# ---------------------------------------------------------------------------

class TestTomlLockFlatMode:
    """Cover legacy/flat mode (model_key='') with versions."""

    def test_flat_mode_versions_roundtrip(self, tmp_path: Path):
        """Versions written in flat mode can be read back."""
        lock = TomlLockAdapter(tmp_path / "dbport.lock", model_key="")
        record = VersionRecord(
            version="2026-03-14",
            published_at=datetime(2026, 3, 14, 12, 0, 0, tzinfo=UTC),
            rows=100,
            completed=True,
        )
        lock.append_version(record)
        versions = lock.read_versions()
        assert len(versions) == 1
        assert versions[0].version == "2026-03-14"


# ---------------------------------------------------------------------------
# String datetime parsing in read_versions
# ---------------------------------------------------------------------------

class TestTomlLockDatetimeParsing:
    """Cover string datetime parsing in read_versions."""

    def test_published_at_string_parsed(self, tmp_path: Path):
        """published_at stored as ISO string is parsed back to datetime."""
        lock_path = tmp_path / "dbport.lock"
        lock_path.write_text(
            '[models."wifor.emp"]\n'
            'agency = "wifor"\n'
            'dataset_id = "emp"\n'
            '\n'
            '[[models."wifor.emp".versions]]\n'
            'version = "v1"\n'
            'published_at = "2026-03-14T12:00:00Z"\n'
            'completed = true\n'
            'rows = 10\n',
            encoding="utf-8",
        )
        lock = TomlLockAdapter(lock_path, model_key="wifor.emp")
        versions = lock.read_versions()
        assert len(versions) == 1
        assert isinstance(versions[0].published_at, datetime)

    def test_snapshot_timestamp_string_parsed(self, tmp_path: Path):
        """iceberg_snapshot_timestamp stored as ISO string is parsed."""
        lock_path = tmp_path / "dbport.lock"
        lock_path.write_text(
            '[models."wifor.emp"]\n'
            'agency = "wifor"\n'
            'dataset_id = "emp"\n'
            '\n'
            '[[models."wifor.emp".versions]]\n'
            'version = "v1"\n'
            'published_at = "2026-03-14T12:00:00Z"\n'
            'iceberg_snapshot_timestamp = "2026-03-14T12:00:00Z"\n'
            'completed = true\n'
            'rows = 10\n',
            encoding="utf-8",
        )
        lock = TomlLockAdapter(lock_path, model_key="wifor.emp")
        versions = lock.read_versions()
        assert isinstance(versions[0].iceberg_snapshot_timestamp, datetime)


class TestDefaultModelRoundTrip:
    """default_model top-level key survives load/save cycle."""

    def test_save_preserves_default_model(self, tmp_path: Path):
        import tomllib

        lock_path = tmp_path / "dbport.lock"
        adapter = TomlLockAdapter(lock_path, model_key="a.x", model_root=".")
        doc = adapter._load()
        doc["default_model"] = "a.x"
        m = adapter._model_doc(doc)
        adapter._ensure_model_header(m)
        adapter._save(doc)

        raw = lock_path.read_text(encoding="utf-8")
        assert raw.startswith('default_model = "a.x"')

        doc2 = tomllib.loads(raw)
        assert doc2["default_model"] == "a.x"
        assert doc2["models"]["a.x"]["agency"] == "a"

    def test_save_without_default_model_has_no_key(self, tmp_path: Path):

        lock_path = tmp_path / "dbport.lock"
        adapter = TomlLockAdapter(lock_path, model_key="a.x", model_root=".")
        doc = adapter._load()
        m = adapter._model_doc(doc)
        adapter._ensure_model_header(m)
        adapter._save(doc)

        raw = lock_path.read_text(encoding="utf-8")
        assert "default_model" not in raw
