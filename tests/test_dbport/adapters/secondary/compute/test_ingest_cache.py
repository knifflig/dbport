"""Tests for adapters.secondary.compute.ingest_cache."""

from __future__ import annotations

from dbport.adapters.secondary.compute.ingest_cache import should_skip_ingest
from dbport.domain.entities.input import IngestRecord


def _record(
    snapshot_id: int = 100, table_address: str = "wifor.foo", filters: dict[str, str] | None = None
) -> IngestRecord:
    return IngestRecord(
        table_address=table_address,
        last_snapshot_id=snapshot_id,
        filters=filters,
    )


class TestShouldSkipIngest:
    """Tests for Should Skip Ingest."""

    def test_returns_false_when_no_record(self) -> None:
        """Returns false when no record."""
        assert should_skip_ingest(None, 100, "wifor.foo", None) is False

    def test_returns_false_when_no_snapshot_id(self) -> None:
        """Returns false when no snapshot id."""
        record = _record(snapshot_id=100)
        assert should_skip_ingest(record, None, "wifor.foo", None) is False

    def test_returns_false_when_snapshot_changed(self) -> None:
        """Returns false when snapshot changed."""
        record = _record(snapshot_id=100)
        assert should_skip_ingest(record, 999, "wifor.foo", None) is False

    def test_returns_true_when_snapshot_matches_no_filters(self) -> None:
        """Returns true when snapshot matches no filters."""
        record = _record(snapshot_id=100)
        assert should_skip_ingest(record, 100, "wifor.foo", None) is True

    def test_returns_false_when_table_address_differs(self) -> None:
        """Returns false when table address differs."""
        record = _record(snapshot_id=100, table_address="wifor.foo")
        assert should_skip_ingest(record, 100, "wifor.bar", None) is False

    def test_returns_false_when_filters_differ(self) -> None:
        """Returns false when filters differ."""
        record = _record(snapshot_id=100, filters={"wstatus": "EMP"})
        assert should_skip_ingest(record, 100, "wifor.foo", {"wstatus": "SELF"}) is False

    def test_returns_true_when_filters_match(self) -> None:
        """Returns true when filters match."""
        record = _record(snapshot_id=100, filters={"wstatus": "EMP"})
        assert should_skip_ingest(record, 100, "wifor.foo", {"wstatus": "EMP"}) is True

    def test_returns_false_when_record_has_filters_but_current_none(self) -> None:
        """Returns false when record has filters but current none."""
        record = _record(snapshot_id=100, filters={"wstatus": "EMP"})
        assert should_skip_ingest(record, 100, "wifor.foo", None) is False

    def test_returns_false_when_current_has_filters_but_record_none(self) -> None:
        """Returns false when current has filters but record none."""
        record = _record(snapshot_id=100, filters=None)
        assert should_skip_ingest(record, 100, "wifor.foo", {"wstatus": "EMP"}) is False

    def test_returns_true_with_multiple_filters_matching(self) -> None:
        """Returns true with multiple filters matching."""
        filters = {"wstatus": "EMP", "nace_r2": "TOTAL"}
        record = _record(snapshot_id=42, filters=filters)
        assert should_skip_ingest(record, 42, "wifor.foo", filters) is True
