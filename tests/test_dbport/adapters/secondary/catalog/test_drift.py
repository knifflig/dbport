"""Tests for adapters.secondary.catalog.drift."""

from __future__ import annotations

import pyarrow as pa
import pytest

from dbport.adapters.secondary.catalog.drift import SchemaDriftError, check_schema_drift


def _schema(*fields) -> pa.Schema:
    return pa.schema(fields)


class TestCheckSchemaDrift:
    def test_identical_schemas_no_error(self):
        s = _schema(pa.field("geo", pa.string()), pa.field("year", pa.int16()))
        check_schema_drift(s, s)  # must not raise

    def test_compatible_schemas_no_error(self):
        local = _schema(pa.field("a", pa.int32()), pa.field("b", pa.utf8()))
        remote = _schema(pa.field("a", pa.int32()), pa.field("b", pa.utf8()))
        check_schema_drift(local, remote)

    def test_raises_on_added_column(self):
        local = _schema(pa.field("a", pa.int32()), pa.field("b", pa.utf8()))
        remote = _schema(pa.field("a", pa.int32()))
        with pytest.raises(SchemaDriftError, match=r"\+ b"):
            check_schema_drift(local, remote)

    def test_raises_on_removed_column(self):
        local = _schema(pa.field("a", pa.int32()))
        remote = _schema(pa.field("a", pa.int32()), pa.field("b", pa.utf8()))
        with pytest.raises(SchemaDriftError, match=r"- b"):
            check_schema_drift(local, remote)

    def test_raises_on_type_change(self):
        local = _schema(pa.field("a", pa.int32()))
        remote = _schema(pa.field("a", pa.int64()))
        with pytest.raises(SchemaDriftError, match=r"~ a"):
            check_schema_drift(local, remote)

    def test_error_message_includes_schema_drift_header(self):
        local = _schema(pa.field("x", pa.int32()))
        remote = _schema(pa.field("x", pa.int64()))
        with pytest.raises(SchemaDriftError, match="Schema drift"):
            check_schema_drift(local, remote)

    def test_empty_schemas_no_error(self):
        check_schema_drift(pa.schema([]), pa.schema([]))

    def test_large_string_equivalent_to_string(self):
        local = _schema(pa.field("a", pa.string()))
        remote = _schema(pa.field("a", pa.large_string()))
        check_schema_drift(local, remote)  # must not raise

    def test_large_binary_equivalent_to_binary(self):
        local = _schema(pa.field("a", pa.binary()))
        remote = _schema(pa.field("a", pa.large_binary()))
        check_schema_drift(local, remote)  # must not raise

    def test_multiple_diffs_all_reported(self):
        local = _schema(pa.field("a", pa.int32()), pa.field("c", pa.utf8()))
        remote = _schema(pa.field("a", pa.int64()), pa.field("b", pa.utf8()))
        with pytest.raises(SchemaDriftError) as exc_info:
            check_schema_drift(local, remote)
        msg = str(exc_info.value)
        assert "~ a" in msg  # type changed
        assert "+ c" in msg  # added locally
        assert "- b" in msg  # removed locally
