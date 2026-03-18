"""Tests for application.services.transform."""

from __future__ import annotations

from pathlib import Path

import pytest

from dbport.application.services.transform import TransformService


class _FakeCompute:
    """Minimal ICompute stub for transform tests."""

    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.executed_files: list[str] = []

    def execute(self, sql: str, parameters: dict[str, str] | None = None) -> None:
        """Record executed SQL."""
        self.executed_sql.append(sql)

    def execute_file(self, path: str) -> None:
        """Record executed file."""
        self.executed_files.append(path)

    def relation_exists(self, schema: str, table: str) -> bool:
        """Check if relation exists."""
        return False

    def to_arrow_batches(self, sql: str, batch_size: int = 10_000) -> iter:
        """Return empty iterator."""
        return iter([])

    def ensure_extensions(self) -> None:
        """No-op."""

    def close(self) -> None:
        """No-op."""


class TestTransformServiceInlineSQL:
    """Tests for inline SQL execution."""

    def test_inline_sql_is_executed(self) -> None:
        """Test inline SQL is executed directly."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        svc.execute("SELECT 1", base_dir="/tmp")
        assert compute.executed_sql == ["SELECT 1"]

    def test_inline_sql_not_treated_as_file(self) -> None:
        """Test inline SQL is not treated as a file path."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        svc.execute("CREATE TABLE foo AS SELECT 1", base_dir="/tmp")
        assert compute.executed_files == []

    def test_sql_with_whitespace_stripped(self) -> None:
        """Test SQL whitespace is stripped."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        svc.execute("  SELECT 1  ", base_dir="/tmp")
        assert compute.executed_sql == ["SELECT 1"]


class TestTransformServiceSqlFile:
    """Tests for SQL file execution."""

    def test_sql_file_routed_to_execute_file(self, tmp_path: Path) -> None:
        """Test SQL file is routed to execute_file."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT 1", encoding="utf-8")
        svc.execute("query.sql", base_dir=str(tmp_path))
        assert len(compute.executed_files) == 1
        assert compute.executed_files[0].endswith("query.sql")

    def test_relative_path_resolved_against_base_dir(self, tmp_path: Path) -> None:
        """Test relative path is resolved against base_dir."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        sql_file = tmp_path / "sub" / "a.sql"
        sql_file.parent.mkdir()
        sql_file.write_text("SELECT 2", encoding="utf-8")
        svc.execute("sub/a.sql", base_dir=str(tmp_path))
        assert compute.executed_files[0] == str(sql_file)

    def test_absolute_path_used_directly(self, tmp_path: Path) -> None:
        """Test absolute path is used directly."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        sql_file = tmp_path / "abs.sql"
        sql_file.write_text("SELECT 3", encoding="utf-8")
        svc.execute(str(sql_file), base_dir="/ignored")
        assert compute.executed_files[0] == str(sql_file)

    def test_sql_file_not_also_executed_as_sql(self, tmp_path: Path) -> None:
        """Test SQL file is not also executed as inline SQL."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT 1", encoding="utf-8")
        svc.execute("q.sql", base_dir=str(tmp_path))
        assert compute.executed_sql == []


class TestTransformServicePathTraversal:
    """Tests for path traversal rejection."""

    def test_relative_traversal_rejected(self, tmp_path: Path) -> None:
        """Test relative path traversal is rejected."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        with pytest.raises(ValueError, match="escapes base directory"):
            svc.execute("../../etc/passwd.sql", base_dir=str(tmp_path))

    def test_dot_dot_in_middle_rejected(self, tmp_path: Path) -> None:
        """Test dot-dot in middle of path is rejected."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        with pytest.raises(ValueError, match="escapes base directory"):
            svc.execute("sub/../../other.sql", base_dir=str(tmp_path))

    def test_valid_relative_path_still_works(self, tmp_path: Path) -> None:
        """Test valid relative path still works."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        sub = tmp_path / "sql"
        sub.mkdir()
        (sub / "ok.sql").write_text("SELECT 1", encoding="utf-8")
        svc.execute("sql/ok.sql", base_dir=str(tmp_path))
        assert len(compute.executed_files) == 1

    def test_absolute_path_bypasses_check(self, tmp_path: Path) -> None:
        """Test absolute path bypasses traversal check."""
        compute = _FakeCompute()
        svc = TransformService(compute)
        sql_file = tmp_path / "abs.sql"
        sql_file.write_text("SELECT 1", encoding="utf-8")
        svc.execute(str(sql_file), base_dir="/ignored")
        assert len(compute.executed_files) == 1
