"""Tests for adapters.secondary.metadata.attach."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json

import pytest

from dbport.adapters.secondary.metadata.attach import (
    attach_codelist_csv,
    attach_metadata_json,
)


class _FakeCatalog:
    def __init__(self):
        self.properties: dict[str, str] = {}
        self.column_docs: dict[str, str] = {}

    def update_table_properties(self, table_address, properties):
        self.properties.update(properties)

    def update_column_docs(self, table_address, column_docs):
        self.column_docs.update(column_docs)


class TestAttachMetadataJson:
    def _make_metadata_bytes(self, content: dict) -> bytes:
        return json.dumps(content).encode("utf-8")

    def test_writes_metadata_json_property(self):
        catalog = _FakeCatalog()
        md_bytes = self._make_metadata_bytes({"agency_id": "wifor"})
        attach_metadata_json(catalog, "wifor.emp", md_bytes)
        assert "dbport.metadata_json" in catalog.properties

    def test_writes_sha256_property(self):
        catalog = _FakeCatalog()
        md_bytes = self._make_metadata_bytes({"agency_id": "wifor"})
        attach_metadata_json(catalog, "wifor.emp", md_bytes)
        assert "dbport.metadata_sha256" in catalog.properties

    def test_writes_gzip_base64_property(self):
        catalog = _FakeCatalog()
        md_bytes = self._make_metadata_bytes({"x": 1})
        attach_metadata_json(catalog, "wifor.emp", md_bytes)
        assert "dbport.metadata_json_gz" in catalog.properties

    def test_sha256_matches_content(self):
        catalog = _FakeCatalog()
        md_bytes = self._make_metadata_bytes({"agency_id": "wifor", "dataset_id": "emp"})
        attach_metadata_json(catalog, "wifor.emp", md_bytes)
        expected_sha = hashlib.sha256(md_bytes).hexdigest()
        assert catalog.properties["dbport.metadata_sha256"] == expected_sha

    def test_gzip_base64_decompresses_to_original(self):
        catalog = _FakeCatalog()
        md_bytes = self._make_metadata_bytes({"agency_id": "wifor"})
        attach_metadata_json(catalog, "wifor.emp", md_bytes)
        gz_b64 = catalog.properties["dbport.metadata_json_gz"]
        decompressed = gzip.decompress(base64.b64decode(gz_b64))
        assert decompressed == md_bytes

    def test_metadata_json_property_is_valid_json(self):
        catalog = _FakeCatalog()
        md_bytes = self._make_metadata_bytes({"agency_id": "wifor", "dataset_id": "emp"})
        attach_metadata_json(catalog, "wifor.emp", md_bytes)
        parsed = json.loads(catalog.properties["dbport.metadata_json"])
        assert parsed["agency_id"] == "wifor"


class TestAttachCodelistCsv:
    def test_writes_column_doc(self):
        catalog = _FakeCatalog()
        csv_bytes = b"code,name\nDE,DE\nFR,FR\n"
        attach_codelist_csv(catalog, "wifor.emp", "geo", csv_bytes)
        assert "geo" in catalog.column_docs

    def test_column_doc_contains_gzip_base64(self):
        catalog = _FakeCatalog()
        csv_bytes = b"code,name\nDE,DE\n"
        attach_codelist_csv(catalog, "wifor.emp", "geo", csv_bytes)
        doc = json.loads(catalog.column_docs["geo"])
        gz_b64 = doc["dbport"]["csv_gzip_base64"]
        decompressed = gzip.decompress(base64.b64decode(gz_b64))
        assert decompressed == csv_bytes

    def test_column_doc_contains_sha256(self):
        catalog = _FakeCatalog()
        csv_bytes = b"code,name\nDE,DE\n"
        attach_codelist_csv(catalog, "wifor.emp", "geo", csv_bytes)
        doc = json.loads(catalog.column_docs["geo"])
        assert doc["dbport"]["csv_sha256"] == hashlib.sha256(csv_bytes).hexdigest()

    def test_codelist_entry_fields_included(self):
        catalog = _FakeCatalog()
        csv_bytes = b"code,name\nDE,DE\n"

        class _Entry:
            codelist_id = "NUTS2024"
            codelist_kind = "hierarchical"
            codelist_type = "categorical"
            codelist_labels = {"en": "NUTS"}

        attach_codelist_csv(catalog, "wifor.emp", "geo", csv_bytes, _Entry())
        doc = json.loads(catalog.column_docs["geo"])
        assert doc["dbport"]["codelist_id"] == "NUTS2024"
        assert doc["dbport"]["codelist_kind"] == "hierarchical"


class TestAttachCodelistCsvEdgeCases:
    def test_skips_when_no_update_column_docs(self):
        """When catalog has no update_column_docs, logs warning and skips."""

        class _NoCatalog:
            def update_table_properties(self, table_address, properties):
                pass
            # No update_column_docs method

        catalog = _NoCatalog()
        csv_bytes = b"code,name\nDE,DE\n"
        # Should not raise
        attach_codelist_csv(catalog, "wifor.emp", "geo", csv_bytes)
