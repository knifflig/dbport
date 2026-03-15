"""Tests for adapters.secondary.metadata.materialize."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from dbport.adapters.secondary.metadata.materialize import MetadataAdapter
from dbport.domain.entities.codelist import CodelistEntry, ColumnCodelist
from dbport.domain.entities.dataset import DatasetKey
from dbport.domain.entities.input import IngestRecord
from dbport.domain.entities.version import DatasetVersion

_KEY = DatasetKey(agency="wifor", dataset_id="emp")
_VERSION = DatasetVersion(version="2026-03-09", params={"wstatus": "EMP"})
_NOW = datetime(2026, 3, 9, 14, 0, 0, tzinfo=UTC)


def _make_codelists(**overrides) -> ColumnCodelist:
    entries = {
        "geo": CodelistEntry(
            column_name="geo",
            column_pos=0,
            codelist_id="geo",
        ),
        "year": CodelistEntry(
            column_name="year",
            column_pos=1,
            codelist_id="year",
        ),
    }
    entries.update(overrides)
    return ColumnCodelist(entries=entries)


def _make_inputs() -> list[IngestRecord]:
    return [
        IngestRecord(
            table_address="estat.foo",
            last_snapshot_id=123,
            rows_loaded=500,
        )
    ]


class TestMetadataAdapterBuildMetadataJson:
    def test_returns_bytes(self):
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(_KEY, _VERSION, _make_inputs(), _make_codelists())
        assert isinstance(result, bytes)

    def test_valid_json(self):
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(_KEY, _VERSION, _make_inputs(), _make_codelists())
        obj = json.loads(result)
        assert isinstance(obj, dict)

    def test_contains_agency_and_dataset(self):
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(_KEY, _VERSION, _make_inputs(), _make_codelists())
        obj = json.loads(result)
        assert obj["agency_id"] == "wifor"
        assert obj["dataset_id"] == "emp"

    def test_contains_version(self):
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(_KEY, _VERSION, _make_inputs(), _make_codelists())
        obj = json.loads(result)
        assert obj["last_updated_data_at"] == "2026-03-09"

    def test_contains_params(self):
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(_KEY, _VERSION, _make_inputs(), _make_codelists())
        obj = json.loads(result)
        assert obj["params"] == {"wstatus": "EMP"}

    def test_contains_inputs(self):
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(_KEY, _VERSION, _make_inputs(), _make_codelists())
        obj = json.loads(result)
        assert len(obj["inputs"]) == 1
        assert obj["inputs"][0]["table_address"] == "estat.foo"

    def test_contains_codelists(self):
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(_KEY, _VERSION, _make_inputs(), _make_codelists())
        obj = json.loads(result)
        col_names = [c["column_name"] for c in obj["codelists"]]
        assert "geo" in col_names
        assert "year" in col_names

    def test_created_at_preserved_from_previous(self):
        adapter = MetadataAdapter()
        previous = json.dumps({"created_at": "2025-01-01T00:00:00Z", "versions": []})
        result = adapter.build_metadata_json(_KEY, _VERSION, [], _make_codelists(), previous)
        obj = json.loads(result)
        assert obj["created_at"] == "2025-01-01T00:00:00Z"

    def test_created_at_set_when_no_previous(self):
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(_KEY, _VERSION, [], _make_codelists())
        obj = json.loads(result)
        assert obj["created_at"]  # should be set to now

    def test_versions_list_appended_with_snapshot_id(self):
        """snapshot_id is stored in the versions list for downstream pinning (Fix C)."""
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(
            _KEY, _VERSION, [], _make_codelists(), snapshot_id=12345
        )
        obj = json.loads(result)
        assert len(obj["versions"]) == 1
        entry = obj["versions"][0]
        assert entry["version"] == _VERSION.version
        assert entry["iceberg_snapshot_id"] == 12345

    def test_versions_list_upserts_existing_entry(self):
        """Re-publishing the same version updates the entry rather than duplicating it."""
        adapter = MetadataAdapter()
        previous = json.dumps({
            "versions": [{"version": _VERSION.version, "iceberg_snapshot_id": 1}]
        })
        result = adapter.build_metadata_json(
            _KEY, _VERSION, [], _make_codelists(), previous, snapshot_id=2
        )
        obj = json.loads(result)
        version_entries = [v for v in obj["versions"] if v["version"] == _VERSION.version]
        assert len(version_entries) == 1
        assert version_entries[0]["iceberg_snapshot_id"] == 2

    def test_versions_list_empty_when_no_snapshot_id(self):
        """Without a snapshot_id the entry is still appended (snapshot omitted)."""
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(_KEY, _VERSION, [], _make_codelists())
        obj = json.loads(result)
        assert len(obj["versions"]) == 1
        assert "iceberg_snapshot_id" not in obj["versions"][0]

    def test_previous_versions_preserved(self):
        """Versions from previous publishes are kept alongside the new one."""
        adapter = MetadataAdapter()
        previous = json.dumps({
            "versions": [{"version": "2025-01-01", "iceberg_snapshot_id": 10}]
        })
        result = adapter.build_metadata_json(
            _KEY, _VERSION, [], _make_codelists(), previous, snapshot_id=99
        )
        obj = json.loads(result)
        assert len(obj["versions"]) == 2
        assert any(v["version"] == "2025-01-01" for v in obj["versions"])
        assert any(v["version"] == _VERSION.version for v in obj["versions"])


class TestMetadataAdapterGenerateCodelistBytes:
    def test_generates_bytes_for_column(self, tmp_path: Path):
        from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter

        ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        ad.execute("CREATE SCHEMA IF NOT EXISTS outputs")
        ad.execute("CREATE TABLE outputs.emp (geo VARCHAR, year INT)")
        ad.execute("INSERT INTO outputs.emp VALUES ('DE', 2020), ('FR', 2021), ('DE', 2022)")
        ad.close()

        ad2 = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        adapter = MetadataAdapter()
        result = adapter.generate_codelist_bytes(
            _make_codelists(),
            ad2,
            "outputs.emp",
        )
        ad2.close()

        assert "geo" in result
        assert isinstance(result["geo"], bytes)
        content = result["geo"].decode("utf-8")
        assert "DE" in content
        assert "FR" in content

    def test_generates_bytes_for_attached_table(self, tmp_path: Path):
        from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter

        ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        ad.execute("CREATE SCHEMA IF NOT EXISTS outputs")
        ad.execute("CREATE TABLE outputs.emp (geo VARCHAR, year INT)")
        ad.execute("INSERT INTO outputs.emp VALUES ('DE', 2020)")
        ad.execute("CREATE SCHEMA IF NOT EXISTS wifor")
        ad.execute("CREATE TABLE wifor.cl_geo (code VARCHAR, name VARCHAR, level INT)")
        ad.execute("INSERT INTO wifor.cl_geo VALUES ('DE', 'Germany', 1), ('FR', 'France', 1)")
        ad.close()

        ad2 = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        codelists = ColumnCodelist(entries={
            "geo": CodelistEntry(
                column_name="geo",
                column_pos=0,
                codelist_id="geo",
                attach_table="wifor.cl_geo",
            ),
            "year": CodelistEntry(
                column_name="year",
                column_pos=1,
                codelist_id="year",
            ),
        })
        adapter = MetadataAdapter()
        result = adapter.generate_codelist_bytes(codelists, ad2, "outputs.emp")
        ad2.close()

        assert "geo" in result
        content = result["geo"].decode("utf-8")
        # Full table export: should have code, name, level columns
        assert "code" in content
        assert "name" in content
        assert "Germany" in content
        assert "France" in content


class TestMetadataAdapterCodelistFields:
    def test_codelist_optional_fields_included_in_metadata(self):
        """codelist_type, codelist_kind, codelist_labels, attach_table all appear in output."""
        adapter = MetadataAdapter()
        codelists = ColumnCodelist(entries={
            "geo": CodelistEntry(
                column_name="geo",
                column_pos=0,
                codelist_id="NUTS2024",
                codelist_type="categorical",
                codelist_kind="hierarchical",
                codelist_labels={"en": "NUTS Regions"},
                attach_table="wifor.cl_nuts2024",
            ),
        })
        result = adapter.build_metadata_json(_KEY, _VERSION, [], codelists)
        obj = json.loads(result)
        cl = obj["codelists"][0]
        assert cl["codelist_type"] == "categorical"
        assert cl["codelist_kind"] == "hierarchical"
        assert cl["codelist_labels"] == {"en": "NUTS Regions"}
        assert cl["source_table"] == "wifor.cl_nuts2024"

    def test_input_filters_and_snapshot_included(self):
        """IngestRecord filters and snapshot fields appear in metadata inputs."""
        adapter = MetadataAdapter()
        inputs = [
            IngestRecord(
                table_address="estat.foo",
                last_snapshot_id=999,
                rows_loaded=100,
                filters={"wstatus": "EMP"},
            ),
        ]
        result = adapter.build_metadata_json(_KEY, _VERSION, inputs, _make_codelists())
        obj = json.loads(result)
        inp = obj["inputs"][0]
        assert inp["filters"] == {"wstatus": "EMP"}
        assert inp["last_snapshot_id"] == 999
        assert inp["rows_loaded"] == 100


class TestMetadataAdapterErrorHandling:
    def test_generate_codelist_bytes_logs_warning_on_column_failure(self, tmp_path: Path):
        """When one column's codelist generation fails, others still succeed."""
        from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter

        ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        ad.execute("CREATE SCHEMA IF NOT EXISTS outputs")
        ad.execute("CREATE TABLE outputs.emp (geo VARCHAR, year INT)")
        ad.execute("INSERT INTO outputs.emp VALUES ('DE', 2020)")
        ad.close()

        ad2 = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        # Create codelists where one points to a non-existent attached table
        codelists = ColumnCodelist(entries={
            "geo": CodelistEntry(
                column_name="geo",
                column_pos=0,
                codelist_id="geo",
                attach_table="nonexistent.table",  # This will fail
            ),
            "year": CodelistEntry(
                column_name="year",
                column_pos=1,
                codelist_id="year",
            ),
        })
        adapter = MetadataAdapter()
        result = adapter.generate_codelist_bytes(codelists, ad2, "outputs.emp")
        ad2.close()

        # "geo" should be missing (failed), but "year" should succeed
        assert "geo" not in result
        assert "year" in result

    def test_build_metadata_json_with_malformed_previous(self):
        """Invalid JSON in previous_metadata_json is treated as empty."""
        adapter = MetadataAdapter()
        result = adapter.build_metadata_json(
            _KEY, _VERSION, [], _make_codelists(),
            previous_metadata_json="not valid json{"
        )
        obj = json.loads(result)
        # created_at should be set to now (not from previous)
        assert obj["created_at"]  # should be a timestamp string
        # versions should contain only the current version
        assert len(obj["versions"]) == 1


class TestMetadataAdapterAttachToTable:
    def test_attach_to_table_calls_attach_functions(self):
        """attach_to_table delegates to attach_metadata_json and attach_codelist_csv."""

        class _TrackingCatalog:
            def __init__(self):
                self.properties = {}
                self.column_docs = {}

            def update_table_properties(self, table_address, properties):
                self.properties.update(properties)

            def update_column_docs(self, table_address, column_docs):
                self.column_docs.update(column_docs)

        catalog = _TrackingCatalog()
        adapter = MetadataAdapter()
        metadata_bytes = b'{"test":true}'
        codelist_bytes = {"geo": b"code,name\nDE,Germany\n"}
        codelist_entries = {
            "geo": CodelistEntry(
                column_name="geo",
                column_pos=0,
                codelist_id="GEO",
            )
        }
        adapter.attach_to_table("wifor.emp", metadata_bytes, codelist_bytes, codelist_entries, catalog)
        assert "dbport.metadata_json" in catalog.properties
        assert "geo" in catalog.column_docs

    def test_attach_to_table_with_codelist_metadata(self):
        """attach_to_table includes codelist entry fields in column docs."""

        class _TrackingCatalog:
            def __init__(self):
                self.properties = {}
                self.column_docs = {}

            def update_table_properties(self, table_address, properties):
                self.properties.update(properties)

            def update_column_docs(self, table_address, column_docs):
                self.column_docs.update(column_docs)

        catalog = _TrackingCatalog()
        adapter = MetadataAdapter()
        entry = CodelistEntry(
            column_name="geo",
            column_pos=0,
            codelist_id="NUTS2024",
            codelist_kind="hierarchical",
            codelist_type="categorical",
        )
        adapter.attach_to_table(
            "wifor.emp",
            b'{"test":true}',
            {"geo": b"code,name\nDE,DE\n"},
            {"geo": entry},
            catalog,
        )
        doc = json.loads(catalog.column_docs["geo"])
        assert doc["dbport"]["codelist_id"] == "NUTS2024"
        assert doc["dbport"]["codelist_kind"] == "hierarchical"
