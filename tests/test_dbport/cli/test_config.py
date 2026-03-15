"""Tests for dbp config command."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()


def _setup_repo(tmp_path: Path) -> Path:
    """Create a minimal repo root with pyproject.toml."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("[project]\n")
    return repo


def _write_lock(repo: Path, content: str) -> Path:
    lock = repo / "dbport.lock"
    lock.write_text(content, encoding="utf-8")
    return lock


class TestConfigDefaultShow:
    def test_show_when_no_lock_file(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "default",
        ])
        assert result.exit_code == 0
        assert "No default model" in result.output

    def test_show_when_no_default_set(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "default",
        ])
        assert result.exit_code == 0
        assert "No default model" in result.output

    def test_show_current_default(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, (
            'default_model = "a.x"\n\n'
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n'
        ))
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "default",
        ])
        assert result.exit_code == 0
        assert "a.x" in result.output

    def test_show_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, (
            'default_model = "a.x"\n\n'
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n'
        ))
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "default",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["default_model"] == "a.x"

    def test_show_json_output_no_default(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "default",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["default_model"] is None


class TestConfigDefaultSet:
    def test_set_valid_model(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, (
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n\n'
            '[models."b.y"]\nagency = "b"\ndataset_id = "y"\nmodel_root = "sub"\n'
        ))
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "default", "b.y",
        ])
        assert result.exit_code == 0
        assert "b.y" in result.output

        # Verify lock file updated
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["default_model"] == "b.y"

    def test_set_invalid_model_errors(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "default", "nonexistent.model",
        ])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_set_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n')
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "default", "a.x",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["default_model"] == "a.x"

    def test_set_preserves_models(self, tmp_path: Path):
        """Setting default_model must not lose any model data."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, (
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "."\n'
            'duckdb_path = "data/x.duckdb"\n'
        ))
        runner.invoke(app, [
            "--project", str(repo),
            "config", "default", "a.x",
        ])
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["default_model"] == "a.x"
        assert doc["models"]["a.x"]["agency"] == "a"
        assert doc["models"]["a.x"]["dataset_id"] == "x"


# -- Shared lock content for info tests --------------------------------------

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


class TestConfigInfo:
    def test_info_default_summary(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info",
        ])
        assert result.exit_code == 0, result.output
        assert "test.t1" in result.output
        assert "models/t1" in result.output
        assert "2 columns" in result.output
        assert "2" in result.output  # inputs or versions count

    def test_info_inputs_flag(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info", "--inputs",
        ])
        assert result.exit_code == 0, result.output
        assert "estat.table_a" in result.output
        assert "wifor.cl_nuts" in result.output
        assert "5000" in result.output

    def test_info_history_flag(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info", "--history",
        ])
        assert result.exit_code == 0, result.output
        assert "2026-03-01" in result.output
        assert "2026-03-14" in result.output
        assert "4800" in result.output

    def test_info_raw_flag(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info", "--raw",
        ])
        assert result.exit_code == 0, result.output
        # Raw output should contain the TOML content directly
        assert 'default_model = "test.t1"' in result.output
        assert '[models."test.t1"]' in result.output

    def test_info_combined_inputs_history(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info", "--inputs", "--history",
        ])
        assert result.exit_code == 0, result.output
        # Both tables should appear
        assert "estat.table_a" in result.output
        assert "2026-03-14" in result.output

    def test_info_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _RICH_LOCK)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "info", "--inputs", "--history",
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

    def test_info_no_lock_file(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info",
        ])
        assert result.exit_code == 0
        assert "No dbport.lock" in result.output

    def test_info_respects_model_flag(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _RICH_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "--model", "models/t2",
            "config", "info",
        ])
        assert result.exit_code == 0, result.output
        assert "test.t2" in result.output

    def test_info_raw_json(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _RICH_LOCK)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "info", "--raw",
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["ok"] is True
        assert 'default_model' in data["data"]["raw"]

    def test_info_no_models_in_lock(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, "# just a comment\n")
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info",
        ])
        assert result.exit_code == 0
        assert "No models found" in result.output

    def test_info_no_models_json(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, "# empty\n")
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "info",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is False

    def test_info_no_lock_json(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "info",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"].get("error") is not None

    def test_info_raw_no_lock(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info", "--raw",
        ])
        assert result.exit_code == 0
        assert "No dbport.lock" in result.output

    def test_info_no_schema_defined(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info",
        ])
        assert result.exit_code == 0
        assert "not defined" in result.output

    def test_info_json_without_inputs_history(self, tmp_path: Path):
        """JSON output without --inputs/--history should not include those keys."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _RICH_LOCK)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "info",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "inputs" not in data["data"]
        assert "versions" not in data["data"]
        assert data["data"]["input_count"] == 2
        assert data["data"]["version_count"] == 2

    def test_info_history_flag_no_versions(self, tmp_path: Path):
        """Cover branch: --history flag true but model has no versions."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info", "--history",
        ])
        assert result.exit_code == 0
        # Should show summary but no history table (no versions to display)
        assert "a.x" in result.output

    def test_info_inputs_flag_no_inputs(self, tmp_path: Path):
        """Cover branch: --inputs flag true but model has no inputs."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info", "--inputs",
        ])
        assert result.exit_code == 0
        assert "a.x" in result.output

    def test_info_inputs_without_timestamp(self, tmp_path: Path):
        """Cover branch: input has no last_snapshot_timestamp_ms (ts is falsy)."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, (
            'default_model = "a.x"\n\n'
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "."\n\n'
            '[[models."a.x".inputs]]\n'
            'table_address = "ns.tbl"\n'
            'rows_loaded = 100\n'
            # No last_snapshot_timestamp_ms
        ))
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "info", "--inputs",
        ])
        assert result.exit_code == 0
        assert "ns.tbl" in result.output
