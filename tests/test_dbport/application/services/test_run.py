"""Tests for application.services.run."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from dbport.application.services.run import RunService


class _FakeCompute:
    def __init__(self):
        self.executed_sql: list[str] = []
        self.executed_files: list[str] = []

    def execute(self, sql, parameters=None):
        self.executed_sql.append(sql)

    def execute_file(self, path):
        self.executed_files.append(path)

    def relation_exists(self, schema, table):
        return False

    def to_arrow_batches(self, sql, batch_size=10_000):
        return iter([])

    def ensure_extensions(self):
        pass

    def close(self):
        pass


class _FakeLock:
    def __init__(self, run_hook: str | None = None):
        self._run_hook = run_hook

    def read_run_hook(self) -> str | None:
        return self._run_hook

    def write_run_hook(self, hook: str) -> None:
        self._run_hook = hook


class _FakePort:
    """Minimal stand-in for DBPort used by RunService."""

    def __init__(self, model_root: str):
        self._dataset = type("D", (), {"model_root": model_root})()
        self.executed: list[str] = []
        self.published: list[dict[str, Any]] = []

    def execute(self, sql_or_path: str) -> None:
        self.executed.append(sql_or_path)

    def publish(self, *, version: str, mode: str | None = None) -> None:
        self.published.append({"version": version, "mode": mode})


class TestRunServiceNoHook:
    def test_defaults_to_main_py(self, tmp_path: Path):
        hook_file = tmp_path / "main.py"
        hook_file.write_text("port.execute('SELECT 1')", encoding="utf-8")

        svc = RunService(_FakeCompute(), _FakeLock(run_hook=None))
        port = _FakePort(str(tmp_path))
        svc.execute(port)
        assert port.executed == ["SELECT 1"]

    def test_raises_when_no_hook_file_exists(self, tmp_path: Path):
        svc = RunService(_FakeCompute(), _FakeLock(run_hook=None))
        port = _FakePort(str(tmp_path))
        with pytest.raises(FileNotFoundError, match="Run hook not found"):
            svc.execute(port)

    def test_falls_back_to_legacy_sql_main_when_main_py_missing(self, tmp_path: Path):
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "main.sql").write_text("SELECT 1", encoding="utf-8")

        svc = RunService(_FakeCompute(), _FakeLock(run_hook=None))
        port = _FakePort(str(tmp_path))
        svc.execute(port)
        assert port.executed == ["sql/main.sql"]


class TestRunServiceSqlHook:
    def test_sql_hook_delegates_to_execute(self):
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="sql/main.sql"))
        port = _FakePort("/tmp")
        svc.execute(port)
        assert port.executed == ["sql/main.sql"]

    def test_sql_hook_does_not_publish_without_version(self):
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="sql/main.sql"))
        port = _FakePort("/tmp")
        svc.execute(port)
        assert port.published == []


class TestRunServicePythonHook:
    def test_py_hook_executes_python(self, tmp_path: Path):
        hook_file = tmp_path / "run.py"
        hook_file.write_text("port.execute('SELECT 1')", encoding="utf-8")

        svc = RunService(_FakeCompute(), _FakeLock(run_hook="run.py"))
        port = _FakePort(str(tmp_path))
        svc.execute(port)
        assert port.executed == ["SELECT 1"]

    def test_py_hook_receives_port_in_namespace(self, tmp_path: Path):
        hook_file = tmp_path / "hook.py"
        hook_file.write_text(
            "assert port is not None\nport.execute('ok')",
            encoding="utf-8",
        )

        svc = RunService(_FakeCompute(), _FakeLock(run_hook="hook.py"))
        port = _FakePort(str(tmp_path))
        svc.execute(port)
        assert port.executed == ["ok"]

    def test_py_hook_receives_dunder_file(self, tmp_path: Path):
        hook_file = tmp_path / "check.py"
        hook_file.write_text(
            f"assert __file__ == {str(hook_file)!r}",
            encoding="utf-8",
        )

        svc = RunService(_FakeCompute(), _FakeLock(run_hook="check.py"))
        port = _FakePort(str(tmp_path))
        svc.execute(port)

    def test_py_hook_relative_path_resolved_to_model_root(self, tmp_path: Path):
        sub = tmp_path / "scripts"
        sub.mkdir()
        hook_file = sub / "build.py"
        hook_file.write_text("port.execute('built')", encoding="utf-8")

        svc = RunService(_FakeCompute(), _FakeLock(run_hook="scripts/build.py"))
        port = _FakePort(str(tmp_path))
        svc.execute(port)
        assert port.executed == ["built"]


class TestRunServiceUnsupportedExtension:
    def test_raises_for_unknown_extension(self):
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="model.dbt"))
        port = _FakePort("/tmp")
        with pytest.raises(ValueError, match="Unsupported run hook extension"):
            svc.execute(port)


class TestRunServicePublish:
    def test_version_forwarded_to_publish(self):
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="sql/main.sql"))
        port = _FakePort("/tmp")
        svc.execute(port, version="2026-03-09")
        assert port.published == [{"version": "2026-03-09", "mode": None}]

    def test_mode_forwarded_to_publish(self):
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="sql/main.sql"))
        port = _FakePort("/tmp")
        svc.execute(port, version="2026-03-09", mode="dry")
        assert port.published == [{"version": "2026-03-09", "mode": "dry"}]

    def test_no_publish_when_version_is_none(self):
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="sql/main.sql"))
        port = _FakePort("/tmp")
        svc.execute(port, version=None)
        assert port.published == []
