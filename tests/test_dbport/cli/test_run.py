"""Tests for dbp run command (full workflow: sync, execute, publish)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()

_PATCH_TARGET = "dbport.adapters.primary.client.DBPort"


def _create_lock(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


_MODEL_LOCK = '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
run_hook = "sql/main.sql"
'''

_MODEL_LOCK_NO_HOOK = '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
'''


def _mock_dbport(run_hook="sql/main.sql"):
    mock_port = MagicMock()
    mock_port.__enter__ = MagicMock(return_value=mock_port)
    mock_port.__exit__ = MagicMock(return_value=False)
    mock_port._lock = MagicMock()
    mock_port._lock.read_run_hook.return_value = run_hook
    return mock_port


class TestRunCommand:
    def test_run_no_hook_fails(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK_NO_HOOK)
        mp = _mock_dbport(run_hook=None)

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run",
            ])
        assert result.exit_code != 0
        assert "No run_hook configured" in result.output

    def test_run_help(self):
        result = runner.invoke(app, ["run", "--help"])
        assert result.exit_code == 0
        assert "workflow" in result.output.lower()

    def test_run_success(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run",
            ])
        assert result.exit_code == 0
        assert "Executed" in result.output
        mp.execute.assert_called_once_with("sql/main.sql")

    def test_run_with_version_publishes(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "--version", "2026-03-15",
            ])
        assert result.exit_code == 0
        mp.execute.assert_called_once_with("sql/main.sql")
        mp.publish.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_with_timing(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "--timing",
            ])
        assert result.exit_code == 0
        assert "Duration" in result.output

    def test_run_json_output(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--json",
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run",
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["run_hook"] == "sql/main.sql"
        assert "elapsed_seconds" in data["data"]

    def test_run_no_model_fails(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "run",
        ])
        assert result.exit_code != 0
        assert "No models found" in result.output

    def test_run_dry_run_mode(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "--version", "2026-03-15", "--dry-run",
            ])
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-03-15", mode="dry")

    def test_run_refresh_mode(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "--version", "2026-03-15", "--refresh",
            ])
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-03-15", mode="refresh")
