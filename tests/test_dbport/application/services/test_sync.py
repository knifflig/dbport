"""Tests for SyncService."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dbport.application.services.sync import SyncService
from dbport.domain.entities.input import IngestRecord
from dbport.domain.entities.schema import SqlDdl
from dbport.infrastructure.progress import progress_callback


class TestSyncOutputTable:
    def _make_service(self, *, schema_ddl: str | None = None, relation_exists: bool = False):
        catalog = MagicMock()
        compute = MagicMock()
        lock = MagicMock()

        if schema_ddl is None:
            lock.read_schema.return_value = None
        else:
            schema = MagicMock()
            schema.ddl = SqlDdl(statement=schema_ddl)
            lock.read_schema.return_value = schema

        compute.relation_exists.return_value = relation_exists
        lock.read_ingest_records.return_value = []

        return SyncService(catalog, compute, lock), compute, lock

    def test_skips_when_no_schema(self):
        svc, compute, _ = self._make_service(schema_ddl=None)
        svc.execute("test.tbl")
        compute.execute.assert_not_called()

    def test_creates_table_when_not_exists(self):
        ddl = "CREATE OR REPLACE TABLE test.tbl (id INT)"
        svc, compute, _ = self._make_service(schema_ddl=ddl, relation_exists=False)
        svc.execute("test.tbl")

        # Should create schema and run DDL
        calls = [c.args[0] for c in compute.execute.call_args_list]
        assert any("CREATE SCHEMA" in c for c in calls)
        assert any("CREATE OR REPLACE TABLE" in c for c in calls)

    def test_skips_ddl_when_table_exists(self):
        ddl = "CREATE OR REPLACE TABLE test.tbl (id INT)"
        svc, compute, _ = self._make_service(schema_ddl=ddl, relation_exists=True)
        svc.execute("test.tbl")

        # DDL should NOT be executed
        for call in compute.execute.call_args_list:
            assert "CREATE OR REPLACE TABLE" not in call.args[0]

    def test_relation_exists_called_with_correct_args(self):
        ddl = "CREATE OR REPLACE TABLE ns.mytbl (id INT)"
        svc, compute, _ = self._make_service(schema_ddl=ddl, relation_exists=True)
        svc.execute("ns.mytbl")
        compute.relation_exists.assert_called_once_with("ns", "mytbl")

    def test_no_namespace_uses_main(self):
        ddl = "CREATE TABLE tbl (id INT)"
        svc, compute, _ = self._make_service(schema_ddl=ddl, relation_exists=False)
        svc.execute("tbl")
        compute.relation_exists.assert_called_once_with("main", "tbl")
        # Should NOT create schema for "main"
        for call in compute.execute.call_args_list:
            assert "CREATE SCHEMA" not in call.args[0]

    def test_progress_callback_on_create(self):
        """Progress callback fires started/finished when table is created."""
        ddl = "CREATE OR REPLACE TABLE test.tbl (id INT)"
        svc, compute, _ = self._make_service(schema_ddl=ddl, relation_exists=False)

        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            svc.execute("test.tbl")
        finally:
            progress_callback.reset(token)

        cb.started.assert_any_call("Creating output table test.tbl")
        cb.finished.assert_called()


class TestSyncInputs:
    def _make_service(self, records: list[IngestRecord] | None = None):
        catalog = MagicMock()
        compute = MagicMock()
        lock = MagicMock()

        lock.read_schema.return_value = None  # skip output table sync
        lock.read_ingest_records.return_value = records or []

        return SyncService(catalog, compute, lock), catalog, compute, lock

    def test_no_records_skips(self):
        svc, catalog, compute, lock = self._make_service(records=[])
        svc.execute("test.tbl")
        # IngestService should not be constructed
        compute.execute.assert_not_called()

    def test_syncs_inputs(self):
        record = IngestRecord(
            table_address="estat.foo",
            last_snapshot_id=123,
            last_snapshot_timestamp_ms=1000,
            rows_loaded=100,
        )
        svc, catalog, compute, lock = self._make_service(records=[record])

        with patch(
            "dbport.application.services.ingest.IngestService"
        ) as MockIngest:
            mock_ingest = MagicMock()
            MockIngest.return_value = mock_ingest
            svc.execute("test.tbl")

        MockIngest.assert_called_once_with(catalog, compute, lock)
        mock_ingest.execute.assert_called_once()
        decl = mock_ingest.execute.call_args[0][0]
        assert decl.table_address == "estat.foo"

    def test_failed_input_logs_warning(self):
        record = IngestRecord(
            table_address="estat.bad",
            last_snapshot_id=456,
            last_snapshot_timestamp_ms=2000,
            rows_loaded=0,
        )
        svc, catalog, compute, lock = self._make_service(records=[record])

        with patch(
            "dbport.application.services.ingest.IngestService"
        ) as MockIngest:
            mock_ingest = MagicMock()
            mock_ingest.execute.side_effect = RuntimeError("network error")
            MockIngest.return_value = mock_ingest
            # Should not raise — errors are caught and logged
            svc.execute("test.tbl")

    def test_failed_input_with_progress_callback(self):
        record = IngestRecord(
            table_address="estat.bad",
            last_snapshot_id=789,
            last_snapshot_timestamp_ms=3000,
            rows_loaded=0,
        )
        svc, catalog, compute, lock = self._make_service(records=[record])
        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            with patch(
                "dbport.application.services.ingest.IngestService"
            ) as MockIngest:
                mock_ingest = MagicMock()
                mock_ingest.execute.side_effect = RuntimeError("fail")
                MockIngest.return_value = mock_ingest
                svc.execute("test.tbl")
        finally:
            progress_callback.reset(token)

        cb.failed.assert_called_once_with("Failed to sync estat.bad")

    def test_syncs_input_with_filters_and_version(self):
        record = IngestRecord(
            table_address="estat.bar",
            last_snapshot_id=111,
            last_snapshot_timestamp_ms=4000,
            rows_loaded=50,
            filters={"wstatus": "EMP"},
            version="v1",
        )
        svc, catalog, compute, lock = self._make_service(records=[record])

        with patch(
            "dbport.application.services.ingest.IngestService"
        ) as MockIngest:
            mock_ingest = MagicMock()
            MockIngest.return_value = mock_ingest
            svc.execute("test.tbl")

        decl = mock_ingest.execute.call_args[0][0]
        assert decl.table_address == "estat.bar"
        assert decl.filters == {"wstatus": "EMP"}
        assert decl.version == "v1"
