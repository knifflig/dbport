"""Tests for dbp status command."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()


def _create_lock(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestStatusCommand:
    def test_status_no_lock(self, tmp_path: Path):
        result = runner.invoke(app, [
            "--lockfile", str(tmp_path / "dbport.lock"),
            "status",
        ])
        assert result.exit_code == 0
        assert "No dbport.lock found" in result.output

    def test_status_with_model(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."wifor.emp"]
agency = "wifor"
dataset_id = "emp"
model_root = "."
duckdb_path = "data/emp.duckdb"
''')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "status",
        ])
        assert result.exit_code == 0
        assert "wifor.emp" in result.output
        assert "wifor" in result.output

    def test_status_json(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."wifor.emp"]
agency = "wifor"
dataset_id = "emp"
model_root = "."
''')
        result = runner.invoke(app, [
            "--json",
            "--lockfile", str(lock),
            "status",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert "wifor.emp" in data["data"]["models"]

    def test_status_with_schema(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."wifor.emp"]
agency = "wifor"
dataset_id = "emp"

[models."wifor.emp".schema]
ddl = "CREATE TABLE wifor.emp (id VARCHAR)"

[[models."wifor.emp".schema.columns]]
column_name = "id"
column_pos = 0
sql_type = "VARCHAR"
''')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "status",
        ])
        assert result.exit_code == 0
        assert "1 columns" in result.output

    def test_status_empty_models(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, "# empty lock\n")
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "status",
        ])
        assert result.exit_code == 0
        assert "No models found" in result.output

    def test_status_with_inputs_and_rows(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[[models."a.b".inputs]]
table_address = "estat.table1"
rows_loaded = 1000

[[models."a.b".inputs]]
table_address = "wifor.table2"
''')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "status",
        ])
        assert result.exit_code == 0
        assert "2 loaded" in result.output
        assert "estat.table1" in result.output
        assert "1000 rows" in result.output
        # table2 has no rows, should still appear
        assert "wifor.table2" in result.output

    def test_status_with_versions(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"

[[models."a.b".versions]]
version = "2026-01-01"
completed = true

[[models."a.b".versions]]
version = "2026-03-15"
completed = true
''')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "status",
        ])
        assert result.exit_code == 0
        assert "2 version(s)" in result.output
        assert "2026-03-15" in result.output

    def test_status_show_history(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"

[[models."a.b".versions]]
version = "2026-01-01"
published_at = 2026-01-01T12:00:00Z
rows = 500
completed = true

[[models."a.b".versions]]
version = "2026-03-15"
published_at = 2026-03-15T10:00:00Z
rows = 1500
completed = false
''')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "status", "--show-history",
        ])
        assert result.exit_code == 0
        assert "2026-01-01" in result.output
        assert "2026-03-15" in result.output
        assert "yes" in result.output
        assert "no" in result.output

    def test_status_json_with_inputs_versions(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"

[[models."a.b".inputs]]
table_address = "ns.tbl"

[[models."a.b".versions]]
version = "v1"
''')
        result = runner.invoke(app, [
            "--json",
            "--lockfile", str(lock),
            "status",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        m = data["data"]["models"]["a.b"]
        assert m["inputs"] == ["ns.tbl"]
        assert m["versions"] == ["v1"]

    def test_status_no_schema(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
''')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "status",
        ])
        assert result.exit_code == 0
        assert "not defined" in result.output
