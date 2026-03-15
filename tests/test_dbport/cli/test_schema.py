"""Tests for dbp schema command."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()

_PATCH_TARGET = "dbport.adapters.primary.client.DBPort"


def _create_lock(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestSchemaShowCommand:
    def test_schema_show_no_model(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "schema",
        ])
        assert result.exit_code == 0
        assert "No models found" in result.output

    def test_schema_show_no_ddl(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
''')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "schema",
        ])
        assert result.exit_code == 0
        assert "No schema defined" in result.output

    def test_schema_show_with_columns(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"

[models."a.b".schema]
ddl = "CREATE TABLE a.b (id VARCHAR, val DOUBLE)"

[[models."a.b".schema.columns]]
column_name = "id"
column_pos = 0
sql_type = "VARCHAR"

[[models."a.b".schema.columns]]
column_name = "val"
column_pos = 1
sql_type = "DOUBLE"
''')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "schema",
        ])
        assert result.exit_code == 0
        assert "id" in result.output
        assert "VARCHAR" in result.output

    def test_schema_show_json(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"

[models."a.b".schema]
ddl = "CREATE TABLE a.b (id VARCHAR)"

[[models."a.b".schema.columns]]
column_name = "id"
column_pos = 0
sql_type = "VARCHAR"
''')
        result = runner.invoke(app, [
            "--json",
            "--lockfile", str(lock),
            "schema",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["ddl"] is not None
        assert len(data["data"]["columns"]) == 1


    def test_schema_show_no_columns(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"

[models."a.b".schema]
ddl = "CREATE TABLE a.b (id VARCHAR)"
''')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "schema",
        ])
        assert result.exit_code == 0
        assert "No columns defined" in result.output
        assert "DDL" in result.output

    def test_schema_show_json_no_models(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, "# empty\n")
        result = runner.invoke(app, [
            "--json",
            "--lockfile", str(lock),
            "schema",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["schema"] is None


class TestSchemaApplyCommand:
    def test_schema_apply_file_not_found(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
''')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "schema", "/nonexistent/path.sql",
        ])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_schema_apply_file_not_found_json(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
''')
        result = runner.invoke(app, [
            "--json",
            "--lockfile", str(lock),
            "--project", str(tmp_path),
            "schema", "nonexistent.sql",
        ])
        assert result.exit_code != 0
        data = json.loads(result.output)
        assert data["ok"] is False

    def test_schema_apply_success(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
''')
        # Create the SQL file so the pre-check passes
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "create_output.sql").write_text("CREATE TABLE a.b (id VARCHAR);")

        mp = MagicMock()
        mp.__enter__ = MagicMock(return_value=mp)
        mp.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "schema", "sql/create_output.sql",
            ])
        assert result.exit_code == 0
        assert "Schema applied" in result.output
        mp.schema.assert_called_once_with("sql/create_output.sql")

    def test_schema_apply_json_output(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
''')
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "out.sql").write_text("CREATE TABLE a.b (id VARCHAR);")

        mp = MagicMock()
        mp.__enter__ = MagicMock(return_value=mp)
        mp.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--json",
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "schema", "sql/out.sql",
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["applied"] == "sql/out.sql"
        assert data["data"]["model"] == "a.b"

    def test_schema_apply_non_sql_extension(self, tmp_path: Path):
        """Source without .sql extension skips file existence pre-check."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
''')
        mp = MagicMock()
        mp.__enter__ = MagicMock(return_value=mp)
        mp.__exit__ = MagicMock(return_value=False)

        # Non-.sql source like a DDL string — no file check, passes through to port.schema()
        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "schema", "CREATE TABLE a.b (id VARCHAR)",
            ])
        assert result.exit_code == 0
        mp.schema.assert_called_once_with("CREATE TABLE a.b (id VARCHAR)")
