"""Tests for dbp config check command (project health verification)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()


class TestCheckCommand:
    def test_check_basic_passes(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text('[models."a.b"]\nagency = "a"\ndataset_id = "b"\n')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "config", "check",
        ])
        assert result.exit_code == 0
        assert "PASS" in result.output
        assert "All checks passed" in result.output

    def test_check_missing_lockfile(self, tmp_path: Path):
        result = runner.invoke(app, [
            "--lockfile", str(tmp_path / "nope.lock"),
            "config", "check",
        ])
        # Missing lockfile is a FAIL, so exit code is 1
        assert result.exit_code == 1
        assert "FAIL" in result.output
        assert "lockfile" in result.output

    def test_check_json_output(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# valid toml\n")
        result = runner.invoke(app, [
            "--json",
            "--lockfile", str(lock),
            "config", "check",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "checks" in data["data"]

    def test_check_strict_fails_on_warnings(self, tmp_path: Path, monkeypatch):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# valid toml\n")
        monkeypatch.delenv("ICEBERG_REST_URI", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "config", "check", "--strict",  # --strict passed as VALUE arg
        ])
        assert result.exit_code != 0

    def test_check_duckdb_passes(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "config", "check",
        ])
        assert "duckdb" in result.output
        assert "PASS" in result.output

    def test_check_dependencies_pass(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "config", "check",
        ])
        assert "dependencies" in result.output
        assert "PASS" in result.output

    def test_check_invalid_toml(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("not valid {{{{ toml !!!!\n")
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "config", "check",
        ])
        assert result.exit_code == 1
        assert "FAIL" in result.output

    def test_check_credentials_pass(self, tmp_path: Path):
        """When credentials are available, check should pass."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "config", "check",
        ])
        assert "credentials" in result.output
        assert "PASS" in result.output

    def test_check_credentials_warn_json(self, tmp_path: Path, monkeypatch):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        monkeypatch.delenv("ICEBERG_REST_URI", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, [
            "--json",
            "--lockfile", str(lock),
            "config", "check",
        ])
        data = json.loads(result.output)
        cred_check = [c for c in data["data"]["checks"] if c["name"] == "credentials"][0]
        assert cred_check["status"] == "warn"

    def test_check_strict_json_fails(self, tmp_path: Path, monkeypatch):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        monkeypatch.delenv("ICEBERG_REST_URI", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, [
            "--json",
            "--lockfile", str(lock),
            "config", "check", "--strict",  # --strict passed as VALUE arg
        ])
        assert result.exit_code != 0
        data = json.loads(result.output)
        assert data["ok"] is False

    def test_check_duckdb_failure(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        import duckdb as _real_duckdb

        mock_duckdb = MagicMock()
        mock_duckdb.connect.side_effect = Exception("DuckDB broken")
        mock_duckdb.__version__ = _real_duckdb.__version__

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "config", "check",
            ])
        # DuckDB failure is a FAIL
        assert result.exit_code == 1
        assert "FAIL" in result.output

    def test_check_credentials_warn_via_try_branch(self, tmp_path: Path):
        """Cover WarehouseCreds() succeeds but returns falsy fields."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")

        from types import SimpleNamespace

        fake = SimpleNamespace(catalog_uri="", catalog_token="", warehouse="")
        with patch("dbport.infrastructure.credentials.WarehouseCreds", return_value=fake):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "config", "check",
            ])
        assert "WARN" in result.output
        assert "credentials" in result.output
        assert "ICEBERG_REST_URI" in result.output

    def test_check_credentials_except_branch_with_env(self, tmp_path: Path, monkeypatch):
        """Cover except branch where env vars ARE set (detail=validation failed)."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        monkeypatch.setenv("ICEBERG_REST_URI", "https://example.com")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "tok")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "wh")

        with patch("dbport.infrastructure.credentials.WarehouseCreds", side_effect=Exception("forced")):
            result = runner.invoke(app, [
                "--json",
                "--lockfile", str(lock),
                "config", "check",
            ])
        data = json.loads(result.output)
        cred_check = [c for c in data["data"]["checks"] if c["name"] == "credentials"][0]
        assert cred_check["status"] == "warn"
        assert cred_check["detail"] == "validation failed"

    def test_check_dependency_missing(self, tmp_path: Path):
        """Cover a dependency import fails."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")

        real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__

        def selective_import(name, *args, **kwargs):
            if name == "pyarrow":
                raise ImportError("no pyarrow")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=selective_import):
            result = runner.invoke(app, [
                "--json",
                "--lockfile", str(lock),
                "config", "check",
            ])
        data = json.loads(result.output)
        dep_check = [c for c in data["data"]["checks"] if c["name"] == "dependencies"][0]
        assert dep_check["status"] == "fail"
        assert "pyarrow" in dep_check["detail"]
