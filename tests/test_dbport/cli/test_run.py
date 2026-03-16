"""Tests for dbp run command (full workflow: sync, execute, publish)."""

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
run_hook = "sql/main.sql"
version = "2026-03-15"
'''

_MODEL_LOCK_NO_HOOK = '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
version = "2026-03-15"
'''

_MODEL_LOCK_NO_VERSION = '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
run_hook = "sql/main.sql"
'''

_MULTI_MODEL_LOCK = '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
run_hook = "sql/main.sql"
version = "2026-03-15"

[models."c.d"]
agency = "c"
dataset_id = "d"
model_root = "models/d"
duckdb_path = "models/d/data/d.duckdb"
run_hook = "sql/run.sql"
version = "2026-03-15"
'''

_MODEL_LOCK_WITH_VERSIONS = '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
run_hook = "sql/main.sql"

[[models."a.b".versions]]
version = "2026-03-14"
completed = true

[[models."a.b".versions]]
version = "2026-03-15"
completed = true
'''


def _mock_dbport(run_hook="sql/main.sql"):
    mock_port = MagicMock()
    mock_port.__enter__ = MagicMock(return_value=mock_port)
    mock_port.__exit__ = MagicMock(return_value=False)
    mock_port.run_hook = run_hook
    return mock_port


class TestRunCommand:
    def test_run_no_hook_defaults_to_main_py(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK_NO_HOOK)
        mp = _mock_dbport(run_hook=None)

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run",
            ])
        assert result.exit_code == 0
        mp.run.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_no_version_fails(self, tmp_path: Path):
        """When no version is configured or in history, run fails early."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK_NO_VERSION)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run",
            ])
        assert result.exit_code != 0
        assert "No version available" in result.output

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
        # Auto-resolves version from config
        mp.run.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_with_model_positional_arg(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MULTI_MODEL_LOCK)
        mp = _mock_dbport(run_hook="sql/run.sql")

        with patch(_PATCH_TARGET, return_value=mp) as mock_cls:
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "c.d",
            ])
        assert result.exit_code == 0
        # Verify DBPort was called with the correct model
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["agency"] == "c"
        assert call_kwargs["dataset_id"] == "d"

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
        mp.run.assert_called_once_with(version="2026-03-15", mode=None)

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
        assert data["data"]["model"] == "a.b"
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
        mp.run.assert_called_once_with(version="2026-03-15", mode="dry")

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
        mp.run.assert_called_once_with(version="2026-03-15", mode="refresh")

    def test_run_refresh_without_version_uses_latest(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK_WITH_VERSIONS)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "--refresh",
            ])
        assert result.exit_code == 0
        mp.run.assert_called_once_with(version="2026-03-15", mode="refresh")

    def test_run_json_output_with_version(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--json",
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "--version", "2026-03-15",
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["version"] == "2026-03-15"

    def test_run_model_key_in_json_output(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MULTI_MODEL_LOCK)
        mp = _mock_dbport(run_hook="sql/run.sql")

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--json",
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "c.d",
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["model"] == "c.d"

    def test_run_auto_resolves_config_version(self, tmp_path: Path):
        """Without --version, run uses version from lock config."""
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
        mp.run.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_falls_back_to_latest_completed_version(self, tmp_path: Path):
        """Without config version, falls back to latest completed version."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK_WITH_VERSIONS)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run",
            ])
        assert result.exit_code == 0
        mp.run.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_explicit_version_overrides_config(self, tmp_path: Path):
        """--version flag takes precedence over config version."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "run", "--version", "2026-04-01",
            ])
        assert result.exit_code == 0
        mp.run.assert_called_once_with(version="2026-04-01", mode=None)
