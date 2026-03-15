"""Tests for dbp run command."""

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


_MODEL_LOCK = '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
'''


def _mock_dbport():
    mock_port = MagicMock()
    mock_port.__enter__ = MagicMock(return_value=mock_port)
    mock_port.__exit__ = MagicMock(return_value=False)
    return mock_port


class TestRunCommand:
    def test_run_no_target_fails(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text('[models."a.b"]\nagency = "a"\ndataset_id = "b"\n')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "run",
        ])
        assert result.exit_code != 0
        assert "No target specified" in result.output

    def test_run_help(self):
        result = runner.invoke(app, ["run", "--help"])
        assert result.exit_code == 0
        assert "sql" in result.output.lower()

    def test_run_success(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "sql/main.sql",
            ])
        assert result.exit_code == 0
        assert "Executed" in result.output
        mp.execute.assert_called_once_with("sql/main.sql")

    def test_run_with_timing(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "sql/main.sql", "--timing",
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
                "run", "sql/main.sql",
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["target"] == "sql/main.sql"
        assert "elapsed_seconds" in data["data"]

    def test_run_no_model_fails(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "run", "sql/main.sql",
        ])
        assert result.exit_code != 0
        assert "No models found" in result.output
