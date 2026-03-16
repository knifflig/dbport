"""Tests for dbp project lifecycle commands."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

import dbport.cli.commands.project as project_commands
from dbport.cli.main import app

runner = CliRunner()

_PATCH_TARGET = "dbport.adapters.primary.client.DBPort"


_MULTI_MODEL_LOCK = """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
run_hook = "main.py"
version = "2026-03-15"

[[models."a.b".versions]]
version = "2026-03-15"
completed = true

[models."c.d"]
agency = "c"
dataset_id = "d"
model_root = "models/d"
duckdb_path = "models/d/data/d.duckdb"
run_hook = "main.py"
version = "2026-03-16"

[[models."c.d".versions]]
version = "2026-03-16"
completed = true
"""


_DEPENDENT_MODEL_LOCK = (
    _MULTI_MODEL_LOCK
    + """

[[models."c.d".inputs]]
table_address = "a.b"
version = "2026-03-15"
"""
)


def _create_lock(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _mock_port() -> MagicMock:
    mock_port = MagicMock()
    mock_port.__enter__ = MagicMock(return_value=mock_port)
    mock_port.__exit__ = MagicMock(return_value=False)
    mock_port.run_hook = "main.py"
    mock_port._dataset = type("D", (), {"model_root": "."})()
    return mock_port


class TestProjectCommands:
    def test_project_help(self):
        result = runner.invoke(app, ["project", "--help"])
        assert result.exit_code == 0
        assert "sync" in result.output
        assert "load" in result.output
        assert "exec" in result.output
        assert "publish" in result.output
        assert "run" in result.output

    def test_project_sync_runs_all_models(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MULTI_MODEL_LOCK)
        mock_port = _mock_port()

        with patch(_PATCH_TARGET, return_value=mock_port) as mock_cls:
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "project",
                    "sync",
                ],
            )
        assert result.exit_code == 0, result.output
        assert "Synced 2 model(s)" in result.output
        assert mock_cls.call_count == 2

    def test_project_load_runs_all_models(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(
            lock,
            _MULTI_MODEL_LOCK
            + """

[[models."a.b".inputs]]
table_address = "ns.tbl1"
version = "2026-03-01"

[[models."c.d".inputs]]
table_address = "ns.tbl2"
filters = { geo = "DE" }
""",
        )
        mock_port = _mock_port()

        with patch(_PATCH_TARGET, return_value=mock_port) as mock_cls:
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "project",
                    "load",
                ],
            )
        assert result.exit_code == 0, result.output
        assert "Loaded inputs for 2 model(s)" in result.output
        assert mock_cls.call_count == 2
        mock_port.load.assert_any_call("ns.tbl1", filters=None, version="2026-03-01")
        mock_port.load.assert_any_call("ns.tbl2", filters={"geo": "DE"}, version=None)
        _, kwargs = mock_cls.call_args
        assert kwargs["load_inputs_on_init"] is False

    def test_project_load_update_uses_latest_snapshot(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(
            lock,
            _MULTI_MODEL_LOCK
            + """

[[models."a.b".inputs]]
table_address = "ns.tbl1"
version = "2026-03-01"

[[models."c.d".inputs]]
table_address = "ns.tbl2"
version = "2026-03-02"
""",
        )
        mock_port = _mock_port()

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "project",
                    "load",
                    "--update",
                ],
            )
        assert result.exit_code == 0, result.output
        assert "Updated inputs for 2 model(s)" in result.output
        assert mock_port.load.call_count == 2
        for call in mock_port.load.call_args_list:
            assert call.kwargs["version"] is None

    def test_project_sync_uses_thread_pool(self, tmp_path: Path):
        from concurrent.futures import Future

        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MULTI_MODEL_LOCK)
        mock_port = _mock_port()
        seen_max_workers: list[int] = []

        class FakeExecutor:
            def __init__(self, *, max_workers: int):
                seen_max_workers.append(max_workers)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def submit(self, fn, *args, **kwargs):
                future: Future = Future()
                try:
                    future.set_result(fn(*args, **kwargs))
                except Exception as exc:  # pragma: no cover - defensive
                    future.set_exception(exc)
                return future

        with (
            patch(_PATCH_TARGET, return_value=mock_port),
            patch.object(project_commands, "ThreadPoolExecutor", FakeExecutor),
        ):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "project",
                    "sync",
                ],
            )
        assert result.exit_code == 0, result.output
        assert seen_max_workers == [2]

    def test_project_exec_batches_internal_dependencies(self, tmp_path: Path):
        from concurrent.futures import Future

        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _DEPENDENT_MODEL_LOCK)
        mock_port = _mock_port()
        seen_max_workers: list[int] = []

        class FakeExecutor:
            def __init__(self, *, max_workers: int):
                seen_max_workers.append(max_workers)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def submit(self, fn, *args, **kwargs):
                future: Future = Future()
                try:
                    future.set_result(fn(*args, **kwargs))
                except Exception as exc:  # pragma: no cover - defensive
                    future.set_exception(exc)
                return future

        with (
            patch(_PATCH_TARGET, return_value=mock_port) as mock_cls,
            patch.object(project_commands, "ThreadPoolExecutor", FakeExecutor),
        ):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "project",
                    "exec",
                    "--target",
                    "sql/transform.sql",
                ],
            )
        assert result.exit_code == 0, result.output
        assert seen_max_workers == [1, 1]
        dataset_ids = [call.kwargs["dataset_id"] for call in mock_cls.call_args_list]
        assert dataset_ids == ["b", "d"]

    def test_project_exec_uses_target_override(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MULTI_MODEL_LOCK)
        mock_port = _mock_port()

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "project",
                    "exec",
                    "--target",
                    "sql/transform.sql",
                ],
            )
        assert result.exit_code == 0, result.output
        assert "Executed 2 model(s)" in result.output
        assert mock_port.execute.call_count == 2
        mock_port.execute.assert_any_call("sql/transform.sql")

    def test_project_publish_json(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MULTI_MODEL_LOCK)
        mock_port = _mock_port()

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--json",
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "project",
                    "publish",
                    "--dry-run",
                ],
            )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["total"] == 2
        assert len(data["data"]["results"]) == 2
        assert all(item["mode"] == "dry" for item in data["data"]["results"])

    def test_project_run_json(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MULTI_MODEL_LOCK)
        mock_port = _mock_port()

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--json",
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "project",
                    "run",
                    "--target",
                    "sql/main.sql",
                    "--dry-run",
                ],
            )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["total"] == 2
        assert all(item["target"] == "sql/main.sql" for item in data["data"]["results"])
        assert all(item["mode"] == "dry" for item in data["data"]["results"])
