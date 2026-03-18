"""Tests for dbp model exec command."""

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


_MODEL_LOCK = """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
"""


def _mock_dbport(model_root: str = ".") -> MagicMock:
    mock_port = MagicMock()
    mock_port.__enter__ = MagicMock(return_value=mock_port)
    mock_port.__exit__ = MagicMock(return_value=False)
    mock_port._dataset = type("D", (), {"model_root": model_root})()
    mock_port.run_hook = "main.py"
    return mock_port


class TestExecuteCommand:
    """Tests for TestExecuteCommand."""

    def test_execute_uses_configured_hook_when_no_target(self, tmp_path: Path) -> None:
        """Test Execute uses configured hook when no target."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp = _mock_dbport(str(tmp_path))

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "exec",
                ],
            )
        assert result.exit_code == 0, result.output
        mp.execute.assert_called_once_with("SELECT 1")

    def test_execute_help(self) -> None:
        """Test Execute help."""
        result = runner.invoke(
            app,
            [
                "model",
                "exec",
                "--help",
            ],
        )
        assert result.exit_code == 0
        assert "sql" in result.output.lower()
        assert "py" in result.output.lower()

    def test_execute_success(self, tmp_path: Path) -> None:
        """Test Execute success."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport(str(tmp_path))

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "exec",
                    "a.b",
                    "--target",
                    "sql/main.sql",
                ],
            )
        assert result.exit_code == 0
        assert "Executed" in result.output
        mp.execute.assert_called_once_with("sql/main.sql")

    def test_execute_with_timing(self, tmp_path: Path) -> None:
        """Test Execute with timing."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "exec",
                    "a.b",
                    "--target",
                    "sql/main.sql",
                    "--timing",
                ],
            )
        assert result.exit_code == 0
        assert "Duration" in result.output

    def test_execute_json_output(self, tmp_path: Path) -> None:
        """Test Execute json output."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--json",
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "exec",
                    "a.b",
                    "--target",
                    "sql/main.sql",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["target"] == "sql/main.sql"
        assert "elapsed_seconds" in data["data"]

    def test_exec_python_success(self, tmp_path: Path) -> None:
        """Test Exec python success."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp = _mock_dbport(str(tmp_path))

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "exec",
                    "a.b",
                ],
            )
        assert result.exit_code == 0, result.output
        mp.execute.assert_called_once_with("SELECT 1")

    def test_exec_python_run_callable_uses_injected_port(self, tmp_path: Path) -> None:
        """Test Exec python run callable uses injected port."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        (tmp_path / "main.py").write_text(
            "def run(port):\n"
            "    port.execute('SELECT 2')\n\n"
            "if __name__ == '__main__':\n"
            "    raise RuntimeError('should not execute standalone block')\n",
            encoding="utf-8",
        )
        mp = _mock_dbport(str(tmp_path))

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "exec",
                    "a.b",
                ],
            )
        assert result.exit_code == 0, result.output
        mp.execute.assert_called_once_with("SELECT 2")

    def test_execute_no_model_fails(self, tmp_path: Path) -> None:
        """Test Execute no model fails."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "model",
                "exec",
                "--target",
                "sql/main.sql",
            ],
        )
        assert result.exit_code != 0
        assert "No models found" in result.output
