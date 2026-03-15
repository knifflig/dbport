"""Tests for domain.entities.codelist."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbport.domain.entities.codelist import CodelistEntry, ColumnCodelist


class TestCodelistEntry:
    def test_minimal_construction(self):
        entry = CodelistEntry(
            column_name="geo",
            column_pos=0,
            codelist_id="geo",
        )
        assert entry.column_name == "geo"
        assert entry.column_pos == 0
        assert entry.codelist_id == "geo"
        assert entry.codelist_type is None
        assert entry.codelist_kind is None
        assert entry.codelist_labels is None
        assert entry.attach_table is None

    def test_full_construction(self):
        entry = CodelistEntry(
            column_name="geo",
            column_pos=1,
            codelist_id="NUTS2024",
            codelist_type="categorical",
            codelist_kind="hierarchical",
            codelist_labels={"en": "NUTS 2024 Regions", "de": "NUTS 2024 Regionen"},
            attach_table="wifor.cl_nuts2024",
        )
        assert entry.codelist_id == "NUTS2024"
        assert entry.codelist_kind == "hierarchical"
        assert entry.codelist_labels == {"en": "NUTS 2024 Regions", "de": "NUTS 2024 Regionen"}
        assert entry.attach_table == "wifor.cl_nuts2024"

    def test_frozen(self):
        entry = CodelistEntry(
            column_name="geo",
            column_pos=0,
            codelist_id="geo",
        )
        with pytest.raises((TypeError, ValidationError)):
            entry.codelist_id = "other"  # type: ignore[misc]

    def test_missing_column_name(self):
        with pytest.raises(ValidationError):
            CodelistEntry(column_pos=0, codelist_id="geo")  # type: ignore[call-arg]

    def test_missing_column_pos(self):
        with pytest.raises(ValidationError):
            CodelistEntry(column_name="geo", codelist_id="geo")  # type: ignore[call-arg]

    def test_equality(self):
        a = CodelistEntry(column_name="geo", column_pos=0, codelist_id="geo")
        b = CodelistEntry(column_name="geo", column_pos=0, codelist_id="geo")
        assert a == b

    def test_inequality(self):
        a = CodelistEntry(column_name="geo", column_pos=0, codelist_id="geo")
        b = CodelistEntry(column_name="year", column_pos=1, codelist_id="year")
        assert a != b

    def test_column_pos_zero(self):
        entry = CodelistEntry(column_name="ts", column_pos=0, codelist_id="ts")
        assert entry.column_pos == 0

    def test_optional_fields_default_none(self):
        entry = CodelistEntry(column_name="geo", column_pos=0, codelist_id="geo")
        assert entry.codelist_type is None
        assert entry.codelist_labels is None
        assert entry.attach_table is None


class TestColumnCodelist:
    def test_empty_construction(self):
        cl = ColumnCodelist()
        assert cl.entries == {}

    def test_construction_with_entries(self):
        e1 = CodelistEntry(column_name="geo", column_pos=0, codelist_id="geo")
        e2 = CodelistEntry(column_name="year", column_pos=1, codelist_id="year")
        cl = ColumnCodelist(entries={"geo": e1, "year": e2})
        assert len(cl.entries) == 2
        assert cl.entries["geo"] == e1
        assert cl.entries["year"] == e2

    def test_frozen(self):
        cl = ColumnCodelist()
        with pytest.raises((TypeError, ValidationError)):
            cl.entries = {}  # type: ignore[misc]

    def test_entries_keyed_by_column_name(self):
        e = CodelistEntry(
            column_name="geo",
            column_pos=0,
            codelist_id="NUTS2024",
        )
        cl = ColumnCodelist(entries={"geo": e})
        assert "geo" in cl.entries
        assert cl.entries["geo"].codelist_id == "NUTS2024"
