"""Tests for application.services.run."""

from __future__ import annotations

from pathlib import Path

import pytest

from dbport.application.services.run import RunService


class _FakeCompute:
    """Minimal ICompute stub for run tests."""

    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.executed_files: list[str] = []

    def execute(self, sql: str, parameters: dict[str, str] | None = None) -> None:
        """Record executed SQL."""
        self.executed_sql.append(sql)

    def execute_file(self, path: str) -> None:
        """Record executed file."""
        self.executed_files.append(path)

    def relation_exists(self, schema: str, table: str) -> bool:
        """Check if relation exists."""
        return False

    def to_arrow_batches(self, sql: str, batch_size: int = 10_000) -> iter:
        """Return empty iterator."""
        return iter([])

    def ensure_extensions(self) -> None:
        """No-op."""

    def close(self) -> None:
        """No-op."""


class _FakeLock:
    """Minimal ILockStore stub for run tests."""

    def __init__(self, run_hook: str | None = None) -> None:
        self._run_hook = run_hook

    def read_run_hook(self) -> str | None:
        """Read run hook."""
        return self._run_hook

    def write_run_hook(self, hook: str) -> None:
        """Write run hook."""
        self._run_hook = hook


class _FakePort:
    """Minimal stand-in for DBPort used by RunService."""

    def __init__(self, model_root: str) -> None:
        self._dataset = type("D", (), {"model_root": model_root})()
        self.executed: list[str] = []
        self.published: list[dict[str, str | None]] = []

    def execute(self, sql_or_path: str) -> None:
        """Record executed SQL or path."""
        self.executed.append(sql_or_path)

    def publish(self, *, version: str, mode: str | None = None) -> None:
        """Record publish call."""
        self.published.append({"version": version, "mode": mode})


class TestRunServiceNoHook:
    """Tests for RunService when no hook is configured."""

    def test_defaults_to_main_py(self, tmp_path: Path) -> None:
        """Test defaults to main.py when no hook set."""
        hook_file = tmp_path / "main.py"
        hook_file.write_text("port.execute('SELECT 1')", encoding="utf-8")

        svc = RunService(_FakeCompute(), _FakeLock(run_hook=None))
        port = _FakePort(str(tmp_path))
        svc.execute(port)
        assert port.executed == ["SELECT 1"]

    def test_raises_when_no_hook_file_exists(self, tmp_path: Path) -> None:
        """Test raises when no hook file exists."""
        svc = RunService(_FakeCompute(), _FakeLock(run_hook=None))
        port = _FakePort(str(tmp_path))
        with pytest.raises(FileNotFoundError, match="Run hook not found"):
            svc.execute(port)

    def test_falls_back_to_legacy_sql_main_when_main_py_missing(
        self,
        tmp_path: Path,
    ) -> None:
        """Test falls back to sql/main.sql when main.py missing."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "main.sql").write_text("SELECT 1", encoding="utf-8")

        svc = RunService(_FakeCompute(), _FakeLock(run_hook=None))
        port = _FakePort(str(tmp_path))
        svc.execute(port)
        assert port.executed == ["sql/main.sql"]


class TestRunServiceSqlHook:
    """Tests for RunService with SQL hook."""

    def test_sql_hook_delegates_to_execute(self) -> None:
        """Test SQL hook delegates to execute."""
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="sql/main.sql"))
        port = _FakePort("/tmp")
        svc.execute(port)
        assert port.executed == ["sql/main.sql"]

    def test_sql_hook_does_not_publish_without_version(self) -> None:
        """Test SQL hook does not publish without version."""
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="sql/main.sql"))
        port = _FakePort("/tmp")
        svc.execute(port)
        assert port.published == []


class TestRunServicePythonHook:
    """Tests for RunService with Python hook."""

    def test_py_hook_executes_python(self, tmp_path: Path) -> None:
        """Test Python hook executes Python code."""
        hook_file = tmp_path / "run.py"
        hook_file.write_text("port.execute('SELECT 1')", encoding="utf-8")

        svc = RunService(_FakeCompute(), _FakeLock(run_hook="run.py"))
        port = _FakePort(str(tmp_path))
        svc.execute(port)
        assert port.executed == ["SELECT 1"]

    def test_py_hook_receives_port_in_namespace(self, tmp_path: Path) -> None:
        """Test Python hook receives port in namespace."""
        hook_file = tmp_path / "hook.py"
        hook_file.write_text(
            "assert port is not None\nport.execute('ok')",
            encoding="utf-8",
        )

        svc = RunService(_FakeCompute(), _FakeLock(run_hook="hook.py"))
        port = _FakePort(str(tmp_path))
        svc.execute(port)
        assert port.executed == ["ok"]

    def test_py_hook_receives_dunder_file(self, tmp_path: Path) -> None:
        """Test Python hook receives __file__."""
        hook_file = tmp_path / "check.py"
        hook_file.write_text(
            f"assert __file__ == {str(hook_file)!r}",
            encoding="utf-8",
        )

        svc = RunService(_FakeCompute(), _FakeLock(run_hook="check.py"))
        port = _FakePort(str(tmp_path))
        svc.execute(port)

    def test_py_hook_relative_path_resolved_to_model_root(
        self,
        tmp_path: Path,
    ) -> None:
        """Test Python hook relative path resolved to model_root."""
        sub = tmp_path / "scripts"
        sub.mkdir()
        hook_file = sub / "build.py"
        hook_file.write_text("port.execute('built')", encoding="utf-8")

        svc = RunService(
            _FakeCompute(),
            _FakeLock(run_hook="scripts/build.py"),
        )
        port = _FakePort(str(tmp_path))
        svc.execute(port)
        assert port.executed == ["built"]


class TestRunServiceUnsupportedExtension:
    """Tests for RunService with unsupported extension."""

    def test_raises_for_unknown_extension(self) -> None:
        """Test raises for unknown extension."""
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="model.dbt"))
        port = _FakePort("/tmp")
        with pytest.raises(ValueError, match="Unsupported run hook extension"):
            svc.execute(port)


class TestRunServicePublish:
    """Tests for RunService publish behavior."""

    def test_version_forwarded_to_publish(self) -> None:
        """Test version is forwarded to publish."""
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="sql/main.sql"))
        port = _FakePort("/tmp")
        svc.execute(port, version="2026-03-09")
        assert port.published == [{"version": "2026-03-09", "mode": None}]

    def test_mode_forwarded_to_publish(self) -> None:
        """Test mode is forwarded to publish."""
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="sql/main.sql"))
        port = _FakePort("/tmp")
        svc.execute(port, version="2026-03-09", mode="dry")
        assert port.published == [{"version": "2026-03-09", "mode": "dry"}]

    def test_no_publish_when_version_is_none(self) -> None:
        """Test no publish when version is None."""
        svc = RunService(_FakeCompute(), _FakeLock(run_hook="sql/main.sql"))
        port = _FakePort("/tmp")
        svc.execute(port, version=None)
        assert port.published == []
