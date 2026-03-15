"""Tests for application.services.transform."""

from __future__ import annotations

from pathlib import Path

import pytest

from dbport.application.services.transform import TransformService


class _FakeCompute:
    def __init__(self):
        self.executed_sql: list[str] = []
        self.executed_files: list[str] = []

    def execute(self, sql, parameters=None):
        self.executed_sql.append(sql)

    def execute_file(self, path):
        self.executed_files.append(path)

    def relation_exists(self, schema, table):
        return False

    def to_arrow_batches(self, sql, batch_size=10_000):
        return iter([])

    def close(self):
        pass


class TestTransformServiceInlineSQL:
    def test_inline_sql_is_executed(self):
        compute = _FakeCompute()
        svc = TransformService(compute)
        svc.execute("SELECT 1", base_dir="/tmp")
        assert compute.executed_sql == ["SELECT 1"]

    def test_inline_sql_not_treated_as_file(self):
        compute = _FakeCompute()
        svc = TransformService(compute)
        svc.execute("CREATE TABLE foo AS SELECT 1", base_dir="/tmp")
        assert compute.executed_files == []

    def test_sql_with_whitespace_stripped(self):
        compute = _FakeCompute()
        svc = TransformService(compute)
        svc.execute("  SELECT 1  ", base_dir="/tmp")
        assert compute.executed_sql == ["SELECT 1"]


class TestTransformServiceSqlFile:
    def test_sql_file_routed_to_execute_file(self, tmp_path: Path):
        compute = _FakeCompute()
        svc = TransformService(compute)
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT 1", encoding="utf-8")
        svc.execute("query.sql", base_dir=str(tmp_path))
        assert len(compute.executed_files) == 1
        assert compute.executed_files[0].endswith("query.sql")

    def test_relative_path_resolved_against_base_dir(self, tmp_path: Path):
        compute = _FakeCompute()
        svc = TransformService(compute)
        sql_file = tmp_path / "sub" / "a.sql"
        sql_file.parent.mkdir()
        sql_file.write_text("SELECT 2", encoding="utf-8")
        svc.execute("sub/a.sql", base_dir=str(tmp_path))
        assert compute.executed_files[0] == str(sql_file)

    def test_absolute_path_used_directly(self, tmp_path: Path):
        compute = _FakeCompute()
        svc = TransformService(compute)
        sql_file = tmp_path / "abs.sql"
        sql_file.write_text("SELECT 3", encoding="utf-8")
        svc.execute(str(sql_file), base_dir="/ignored")
        assert compute.executed_files[0] == str(sql_file)

    def test_sql_file_not_also_executed_as_sql(self, tmp_path: Path):
        compute = _FakeCompute()
        svc = TransformService(compute)
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT 1", encoding="utf-8")
        svc.execute("q.sql", base_dir=str(tmp_path))
        assert compute.executed_sql == []
