"""Tests for dbp sync command."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()

_PATCH_TARGET = "dbport.adapters.primary.client.DBPort"


def _setup_repo(tmp_path: Path) -> Path:
    """Create a minimal repo root with pyproject.toml."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("[project]\n")
    return repo


def _create_lock(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


_EXISTING_LOCK = '''\
[models."test.table1"]
agency = "test"
dataset_id = "table1"
model_root = "examples/minimal"
duckdb_path = "examples/minimal/data/table1.duckdb"
run_hook = "sql/main.sql"
'''

_TWO_MODELS_LOCK = '''\
[models."test.table1"]
agency = "test"
dataset_id = "table1"
model_root = "examples/minimal"
duckdb_path = "examples/minimal/data/table1.duckdb"

[models."test.table2"]
agency = "test"
dataset_id = "table2"
model_root = "examples/other"
duckdb_path = "examples/other/data/table2.duckdb"
'''


def _mock_dbport():
    mock_port = MagicMock()
    mock_port.__enter__ = MagicMock(return_value=mock_port)
    mock_port.__exit__ = MagicMock(return_value=False)
    return mock_port


class TestSyncCommand:
    def test_sync_single_model(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        (repo / "examples" / "minimal").mkdir(parents=True, exist_ok=True)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--project", str(repo),
                "sync", "test.table1",
            ])
        assert result.exit_code == 0, result.output
        assert "Synced" in result.output

    def test_sync_unknown_model_fails(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "sync", "nonexistent.model",
        ])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_sync_all_models(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _TWO_MODELS_LOCK)
        (repo / "examples" / "minimal").mkdir(parents=True, exist_ok=True)
        (repo / "examples" / "other").mkdir(parents=True, exist_ok=True)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--project", str(repo),
                "sync",
            ])
        assert result.exit_code == 0, result.output
        assert "Synced 2/2" in result.output

    def test_sync_no_models_fails(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", "# empty lock\n")
        result = runner.invoke(app, [
            "--project", str(repo),
            "sync",
        ])
        assert result.exit_code != 0
        assert "No models found" in result.output

    def test_sync_failure_counted(self, tmp_path: Path):
        """Sync failure for one model should not crash the whole sync."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _TWO_MODELS_LOCK)
        (repo / "examples" / "minimal").mkdir(parents=True, exist_ok=True)
        (repo / "examples" / "other").mkdir(parents=True, exist_ok=True)
        mp = _mock_dbport()
        call_count = 0

        def _side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("sync failed")
            return mp

        with patch(_PATCH_TARGET, side_effect=_side_effect):
            result = runner.invoke(app, [
                "--project", str(repo),
                "sync",
            ])
        assert result.exit_code == 0, result.output
        assert "Synced 1/2" in result.output

    def test_sync_all_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _TWO_MODELS_LOCK)
        (repo / "examples" / "minimal").mkdir(parents=True, exist_ok=True)
        (repo / "examples" / "other").mkdir(parents=True, exist_ok=True)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--json",
                "--project", str(repo),
                "sync",
            ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["total"] == 2

    def test_sync_single_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        (repo / "examples" / "minimal").mkdir(parents=True, exist_ok=True)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--json",
                "--project", str(repo),
                "sync", "test.table1",
            ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["ok"] is True
        assert "test.table1" in data["data"]["synced"]
