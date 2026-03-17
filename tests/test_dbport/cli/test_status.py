"""Tests for dbp status command (merged status + config info)."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()


def _create_lock(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _setup_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("[project]\n")
    return repo


# Rich lock content for detailed tests
_RICH_LOCK = (
    'default_model = "test.t1"\n\n'
    '[models."test.t1"]\n'
    'agency = "test"\n'
    'dataset_id = "t1"\n'
    'model_root = "models/t1"\n'
    'duckdb_path = "models/t1/data/t1.duckdb"\n\n'
    '[models."test.t1".schema]\n'
    'ddl = "CREATE TABLE test.t1 (geo VARCHAR, value DOUBLE);"\n'
    'source = "local"\n\n'
    '[[models."test.t1".schema.columns]]\n'
    'column_name = "geo"\n'
    'column_pos = 0\n'
    'sql_type = "VARCHAR"\n\n'
    '[[models."test.t1".schema.columns]]\n'
    'column_name = "value"\n'
    'column_pos = 1\n'
    'sql_type = "DOUBLE"\n\n'
    '[[models."test.t1".inputs]]\n'
    'table_address = "estat.table_a"\n'
    'last_snapshot_id = 1234567890\n'
    'last_snapshot_timestamp_ms = 1710000000000\n'
    'rows_loaded = 5000\n\n'
    '[[models."test.t1".inputs]]\n'
    'table_address = "wifor.cl_nuts"\n'
    'last_snapshot_id = 9876543210\n'
    'last_snapshot_timestamp_ms = 1710100000000\n'
    'rows_loaded = 200\n\n'
    '[[models."test.t1".versions]]\n'
    'version = "2026-03-01"\n'
    'published_at = 2026-03-01T10:00:00Z\n'
    'iceberg_snapshot_id = 1111111111\n'
    'rows = 4800\n'
    'completed = true\n\n'
    '[[models."test.t1".versions]]\n'
    'version = "2026-03-14"\n'
    'published_at = 2026-03-14T12:00:00Z\n'
    'iceberg_snapshot_id = 2222222222\n'
    'rows = 5000\n'
    'completed = true\n\n'
    '[models."test.t2"]\n'
    'agency = "test"\n'
    'dataset_id = "t2"\n'
    'model_root = "models/t2"\n'
)


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
        assert data["data"]["model_key"] == "wifor.emp"

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


class TestStatusHistory:
    def test_history_flag(self, tmp_path: Path):
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
            "status", "--history",
        ])
        assert result.exit_code == 0
        assert "2026-01-01" in result.output
        assert "2026-03-15" in result.output
        assert "yes" in result.output
        assert "no" in result.output


class TestStatusInputs:
    def test_inputs_flag(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "status", "--inputs",
        ])
        assert result.exit_code == 0, result.output
        assert "estat.table_a" in result.output
        assert "wifor.cl_nuts" in result.output
        assert "5000" in result.output

    def test_inputs_without_timestamp(self, tmp_path: Path):
        """Input has no last_snapshot_timestamp_ms."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", (
            'default_model = "a.x"\n\n'
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "."\n\n'
            '[[models."a.x".inputs]]\n'
            'table_address = "ns.tbl"\n'
            'rows_loaded = 100\n'
        ))
        result = runner.invoke(app, [
            "--project", str(repo),
            "status", "--inputs",
        ])
        assert result.exit_code == 0
        assert "ns.tbl" in result.output

    def test_inputs_flag_no_inputs(self, tmp_path: Path):
        """--inputs flag true but model has no inputs."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "status", "--inputs",
        ])
        assert result.exit_code == 0
        assert "a.x" in result.output


class TestStatusRaw:
    def test_raw_flag(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "status", "--raw",
        ])
        assert result.exit_code == 0, result.output
        assert 'default_model = "test.t1"' in result.output
        assert '[models."test.t1"]' in result.output

    def test_raw_no_lock(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "status", "--raw",
        ])
        assert result.exit_code == 0
        assert "No dbport.lock" in result.output

    def test_raw_json(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _RICH_LOCK)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "status", "--raw",
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["ok"] is True
        assert 'default_model' in data["data"]["raw"]


class TestStatusJson:
    def test_json_with_inputs_history(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _RICH_LOCK)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "status", "--inputs", "--history",
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["model_key"] == "test.t1"
        assert data["data"]["default_model"] == "test.t1"
        assert data["data"]["column_count"] == 2
        assert data["data"]["input_count"] == 2
        assert data["data"]["version_count"] == 2
        assert len(data["data"]["inputs"]) == 2
        assert len(data["data"]["versions"]) == 2
        assert data["data"]["inputs"][0]["table_address"] == "estat.table_a"
        assert data["data"]["versions"][1]["version"] == "2026-03-14"

    def test_json_without_inputs_history(self, tmp_path: Path):
        """JSON output without --inputs/--history should not include those keys."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _RICH_LOCK)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "status",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "inputs" not in data["data"]
        assert "versions" not in data["data"]
        assert data["data"]["input_count"] == 2
        assert data["data"]["version_count"] == 2

    def test_json_no_lock(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "status",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"].get("error") is not None

    def test_json_no_models(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", "# empty\n")
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "status",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is False

    def test_json_with_inputs_versions(self, tmp_path: Path):
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
        assert data["data"]["input_count"] == 1
        assert data["data"]["version_count"] == 1

    def test_status_respects_model_flag(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "--model", "models/t2",
            "status",
        ])
        assert result.exit_code == 0, result.output
        assert "test.t2" in result.output


class TestStatusRawJsonNoLock:
    """Cover _handle_raw with --raw --json when no lock file exists (lines 77-78)."""

    def test_raw_json_no_lock(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "status", "--raw",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is False
        assert "No dbport.lock" in data["data"]["error"]


class TestStatusJsonModelResolution:
    """Cover _handle_json model resolution fallbacks (lines 101-103, 112)."""

    def test_json_model_flag_resolves(self, tmp_path: Path):
        """--model flag used in JSON mode resolves correctly (lines 101-103)."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _RICH_LOCK)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "--model", "models/t2",
            "status",
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["model_key"] == "test.t2"

    def test_json_fallback_to_first_model(self, tmp_path: Path):
        """When no default, no CWD match, no --model, falls to first model (line 112)."""
        repo = _setup_repo(tmp_path)
        lock_no_default = (
            '[models."z.first"]\n'
            'agency = "z"\n'
            'dataset_id = "first"\n'
            'model_root = "models/first"\n\n'
            '[models."z.second"]\n'
            'agency = "z"\n'
            'dataset_id = "second"\n'
            'model_root = "models/second"\n'
        )
        _create_lock(repo / "dbport.lock", lock_no_default)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "status",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["model_key"] == "z.first"


class TestStatusCombined:
    def test_combined_inputs_history(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "status", "--inputs", "--history",
        ])
        assert result.exit_code == 0, result.output
        assert "estat.table_a" in result.output
        assert "2026-03-14" in result.output

    def test_history_no_versions(self, tmp_path: Path):
        """--history flag true but model has no versions."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "status", "--history",
        ])
        assert result.exit_code == 0
        assert "a.x" in result.output

    def test_default_summary(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "status",
        ])
        assert result.exit_code == 0, result.output
        assert "test.t1" in result.output
        assert "models/t1" in result.output
        assert "2 columns" in result.output
