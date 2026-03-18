"""Tests for dbp model run command."""

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
run_hook = "main.py"
version = "2026-03-15"
"""

_MODEL_LOCK_NO_HOOK = """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
version = "2026-03-15"
"""

_MODEL_LOCK_NO_VERSION = """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
run_hook = "main.py"
"""

_MULTI_MODEL_LOCK = """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
run_hook = "main.py"
version = "2026-03-15"

[models."c.d"]
agency = "c"
dataset_id = "d"
model_root = "models/d"
duckdb_path = "models/d/data/d.duckdb"
run_hook = "sql/run.sql"
version = "2026-03-15"
"""

_MODEL_LOCK_WITH_VERSIONS = """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
run_hook = "main.py"

[[models."a.b".versions]]
version = "2026-03-14"
completed = true

[[models."a.b".versions]]
version = "2026-03-15"
completed = true
"""


def _mock_dbport(run_hook: str = "main.py") -> MagicMock:
    mock_port = MagicMock()
    mock_port.__enter__ = MagicMock(return_value=mock_port)
    mock_port.__exit__ = MagicMock(return_value=False)
    mock_port.run_hook = run_hook
    mock_port._dataset = type("D", (), {"model_root": "."})()
    return mock_port


class TestRunCommand:
    """Tests for TestRunCommand."""

    def test_run_no_hook_defaults_to_main_py(self, tmp_path: Path) -> None:
        """Test Run no hook defaults to main py."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK_NO_HOOK)
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp = _mock_dbport(run_hook="main.py")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                ],
            )
        assert result.exit_code == 0, result.output
        assert "main.py" in result.output
        mp.execute.assert_called_once_with("SELECT 1")
        mp.publish.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_no_version_fails(self, tmp_path: Path) -> None:
        """When no version is configured or in history, run fails early."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK_NO_VERSION)
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
                    "run",
                ],
            )
        assert result.exit_code != 0
        assert "No version available" in result.output
        assert "dbp config model a.b" in result.output
        assert "version" in result.output
        assert "<version>" in result.output

    def test_run_help(self) -> None:
        """Test Run help."""
        result = runner.invoke(app, ["model", "run", "--help"])
        assert result.exit_code == 0
        assert "publish" in result.output.lower()
        assert "--target" in result.output

    def test_run_success(self, tmp_path: Path) -> None:
        """Test Run success."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                ],
            )
        assert result.exit_code == 0
        assert "Executed" in result.output
        mp.execute.assert_called_once_with("SELECT 1")
        mp.publish.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_python_hook_callable_uses_injected_port(self, tmp_path: Path) -> None:
        """Test Run python hook callable uses injected port."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text(
            "def run(port):\n"
            "    port.execute('SELECT 2')\n\n"
            "if __name__ == '__main__':\n"
            "    raise RuntimeError('should not execute standalone block')\n",
            encoding="utf-8",
        )
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                ],
            )
        assert result.exit_code == 0, result.output
        mp.execute.assert_called_once_with("SELECT 2")
        mp.publish.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_with_model_positional_arg(self, tmp_path: Path) -> None:
        """Test Run with model positional arg."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MULTI_MODEL_LOCK)
        mp = _mock_dbport(run_hook="sql/run.sql")

        with patch(_PATCH_TARGET, return_value=mp) as mock_cls:
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                    "c.d",
                ],
            )
        assert result.exit_code == 0
        # Verify DBPort was called with the correct model
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["agency"] == "c"
        assert call_kwargs["dataset_id"] == "d"

    def test_run_with_version_publishes(self, tmp_path: Path) -> None:
        """Test Run with version publishes."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                    "--version",
                    "2026-03-15",
                ],
            )
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_with_timing(self, tmp_path: Path) -> None:
        """Test Run with timing."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                    "--timing",
                ],
            )
        assert result.exit_code == 0
        assert "Duration" in result.output

    def test_run_json_output(self, tmp_path: Path) -> None:
        """Test Run json output."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

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
                    "run",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["target"] == "main.py"
        assert data["data"]["model"] == "a.b"
        assert "elapsed_seconds" in data["data"]

    def test_run_no_model_fails(self, tmp_path: Path) -> None:
        """Test Run no model fails."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "model",
                "run",
            ],
        )
        assert result.exit_code != 0
        assert "No models found" in result.output

    def test_run_dry_run_mode(self, tmp_path: Path) -> None:
        """Test Run dry run mode."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                    "--version",
                    "2026-03-15",
                    "--dry-run",
                ],
            )
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-03-15", mode="dry")

    def test_run_refresh_mode(self, tmp_path: Path) -> None:
        """Test Run refresh mode."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                    "--version",
                    "2026-03-15",
                    "--refresh",
                ],
            )
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-03-15", mode="refresh")

    def test_run_refresh_without_version_uses_latest(self, tmp_path: Path) -> None:
        """Test Run refresh without version uses latest."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK_WITH_VERSIONS)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                    "--refresh",
                ],
            )
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-03-15", mode="refresh")

    def test_run_json_output_with_version(self, tmp_path: Path) -> None:
        """Test Run json output with version."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

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
                    "run",
                    "--version",
                    "2026-03-15",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["version"] == "2026-03-15"

    def test_run_model_key_in_json_output(self, tmp_path: Path) -> None:
        """Test Run model key in json output."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MULTI_MODEL_LOCK)
        mp = _mock_dbport(run_hook="sql/run.sql")

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
                    "run",
                    "c.d",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["model"] == "c.d"

    def test_run_auto_resolves_config_version(self, tmp_path: Path) -> None:
        """Without --version, run uses version from lock config."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                ],
            )
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_falls_back_to_latest_completed_version(self, tmp_path: Path) -> None:
        """Without config version, falls back to latest completed version."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK_WITH_VERSIONS)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                ],
            )
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-03-15", mode=None)

    def test_run_explicit_version_overrides_config(self, tmp_path: Path) -> None:
        """--version flag takes precedence over config version."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_dbport()
        (tmp_path / "main.py").write_text("port.execute('SELECT 1')", encoding="utf-8")
        mp._dataset = type("D", (), {"model_root": str(tmp_path)})()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "run",
                    "--version",
                    "2026-04-01",
                ],
            )
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-04-01", mode=None)


class TestRunExecuteStepCallbackFailed:
    """Cover _run_execute_step callback.failed() path (lifecycle.py lines 28-32)."""

    def test_callback_failed_called_on_exception(self) -> None:
        """When execute_hook raises, callback.failed() is called and exception re-raised."""
        import pytest

        from dbport.cli.commands.lifecycle import _run_execute_step
        from dbport.infrastructure.progress import progress_callback

        mock_port = MagicMock()
        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            with patch(
                "dbport.application.services.run.execute_hook",
                side_effect=RuntimeError("hook failed"),
            ):
                with pytest.raises(RuntimeError, match="hook failed"):
                    _run_execute_step(mock_port, "main.py")
        finally:
            progress_callback.reset(token)

        cb.started.assert_called_once_with("Executing main.py")
        cb.failed.assert_called_once_with("Failed executing main.py")

    def test_callback_finished_on_success(self) -> None:
        """When execute_hook succeeds, callback.finished() is called."""
        from dbport.cli.commands.lifecycle import _run_execute_step
        from dbport.infrastructure.progress import progress_callback

        mock_port = MagicMock()
        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            with patch("dbport.application.services.run.execute_hook"):
                _run_execute_step(mock_port, "main.py")
        finally:
            progress_callback.reset(token)

        cb.started.assert_called_once_with("Executing main.py")
        cb.finished.assert_called_once_with("Executed main.py")

    def test_no_callback_exception_still_reraises(self) -> None:
        """When no callback is set, _run_execute_step still re-raises."""
        import pytest

        from dbport.cli.commands.lifecycle import _run_execute_step
        from dbport.infrastructure.progress import progress_callback

        mock_port = MagicMock()
        token = progress_callback.set(None)
        try:
            with patch(
                "dbport.application.services.run.execute_hook",
                side_effect=RuntimeError("hook failed"),
            ):
                with pytest.raises(RuntimeError, match="hook failed"):
                    _run_execute_step(mock_port, "main.py")
        finally:
            progress_callback.reset(token)
