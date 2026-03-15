"""Tests for SyncService."""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock

import pytest

from dbport.application.services.sync import SyncService
from dbport.domain.entities.schema import SqlDdl


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

        return SyncService(catalog, compute, lock), compute

    def test_skips_when_no_schema(self):
        svc, compute = self._make_service(schema_ddl=None)
        svc.execute("test.tbl")
        compute.execute.assert_not_called()

    def test_creates_table_when_not_exists(self):
        ddl = "CREATE OR REPLACE TABLE test.tbl (id INT)"
        svc, compute = self._make_service(schema_ddl=ddl, relation_exists=False)
        svc.execute("test.tbl")

        # Should create schema and run DDL
        calls = [c.args[0] for c in compute.execute.call_args_list]
        assert any("CREATE SCHEMA" in c for c in calls)
        assert any("CREATE OR REPLACE TABLE" in c for c in calls)

    def test_skips_ddl_when_table_exists(self):
        ddl = "CREATE OR REPLACE TABLE test.tbl (id INT)"
        svc, compute = self._make_service(schema_ddl=ddl, relation_exists=True)
        svc.execute("test.tbl")

        # DDL should NOT be executed
        for call in compute.execute.call_args_list:
            assert "CREATE OR REPLACE TABLE" not in call.args[0]

    def test_relation_exists_called_with_correct_args(self):
        ddl = "CREATE OR REPLACE TABLE ns.mytbl (id INT)"
        svc, compute = self._make_service(schema_ddl=ddl, relation_exists=True)
        svc.execute("ns.mytbl")
        compute.relation_exists.assert_called_once_with("ns", "mytbl")

    def test_no_namespace_uses_main(self):
        ddl = "CREATE TABLE tbl (id INT)"
        svc, compute = self._make_service(schema_ddl=ddl, relation_exists=False)
        svc.execute("tbl")
        compute.relation_exists.assert_called_once_with("main", "tbl")
        # Should NOT create schema for "main"
        for call in compute.execute.call_args_list:
            assert "CREATE SCHEMA" not in call.args[0]
