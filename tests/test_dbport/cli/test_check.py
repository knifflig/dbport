"""Tests for dbp status check command."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()


class TestCheckCommand:
    """Tests for the check command."""

    def test_check_basic_passes(self, tmp_path: Path) -> None:
        """Test basic check passes."""
        lock = tmp_path / "dbport.lock"
        lock.write_text('[models."a.b"]\nagency = "a"\ndataset_id = "b"\n')
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "status",
                "check",
            ],
        )
        assert result.exit_code == 0
        assert "PASS" in result.output
        assert "All checks passed" in result.output

    def test_check_missing_lockfile(self, tmp_path: Path) -> None:
        """Test check fails when lockfile is missing."""
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(tmp_path / "nope.lock"),
                "status",
                "check",
            ],
        )
        # Missing lockfile is a FAIL, so exit code is 1
        assert result.exit_code == 1
        assert "FAIL" in result.output
        assert "lockfile" in result.output

    def test_check_json_output(self, tmp_path: Path) -> None:
        """Test check JSON output."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# valid toml\n")
        result = runner.invoke(
            app,
            [
                "--json",
                "--lockfile",
                str(lock),
                "status",
                "check",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "checks" in data["data"]

    def test_check_strict_fails_on_warnings(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test strict mode fails on warnings."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# valid toml\n")
        # Remove credential env vars AND override the pydantic .env file
        # so WarehouseCreds cannot resolve credentials from any source.
        monkeypatch.delenv("ICEBERG_REST_URI", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)
        monkeypatch.chdir(tmp_path)  # no .env file here
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "status",
                "check",
                "--strict",
            ],
        )
        assert result.exit_code != 0

    def test_check_duckdb_passes(self, tmp_path: Path) -> None:
        """Test duckdb check passes."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "status",
                "check",
            ],
        )
        assert "duckdb" in result.output
        assert "PASS" in result.output

    def test_check_dependencies_pass(self, tmp_path: Path) -> None:
        """Test dependencies check passes."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "status",
                "check",
            ],
        )
        assert "dependencies" in result.output
        assert "PASS" in result.output

    def test_check_invalid_toml(self, tmp_path: Path) -> None:
        """Test check fails on invalid TOML."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("not valid {{{{ toml !!!!\n")
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "status",
                "check",
            ],
        )
        assert result.exit_code == 1
        assert "FAIL" in result.output

    def test_check_credentials_pass(self, tmp_path: Path) -> None:
        """When credentials are available, check should pass."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "status",
                "check",
            ],
        )
        assert "credentials" in result.output
        # In this env, creds are present
        assert "PASS" in result.output

    def test_check_credentials_warn_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test credentials warning in JSON output."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        monkeypatch.delenv("ICEBERG_REST_URI", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(
            app,
            [
                "--json",
                "--lockfile",
                str(lock),
                "status",
                "check",
            ],
        )
        data = json.loads(result.output)
        cred_check = [c for c in data["data"]["checks"] if c["name"] == "credentials"][0]
        assert cred_check["status"] == "warn"

    def test_check_strict_json_fails(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test strict mode fails in JSON output."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        monkeypatch.delenv("ICEBERG_REST_URI", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(
            app,
            [
                "--json",
                "--lockfile",
                str(lock),
                "status",
                "check",
                "--strict",
            ],
        )
        assert result.exit_code != 0
        data = json.loads(result.output)
        assert data["ok"] is False

    def test_check_duckdb_failure(self, tmp_path: Path) -> None:
        """Test duckdb check failure."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        import duckdb as _real_duckdb

        mock_duckdb = MagicMock()
        mock_duckdb.connect.side_effect = Exception("DuckDB broken")
        mock_duckdb.__version__ = _real_duckdb.__version__

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "status",
                    "check",
                ],
            )
        # DuckDB failure is a FAIL
        assert result.exit_code == 1
        assert "FAIL" in result.output

    def test_check_credentials_warn_via_try_branch(self, tmp_path: Path) -> None:
        """Cover lines 59-66: WarehouseCreds() succeeds but returns falsy fields."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")

        fake_creds = MagicMock()
        fake_creds.catalog_uri = ""
        fake_creds.catalog_token = ""
        fake_creds.warehouse = ""

        with patch(
            "dbport.cli.commands.check.WarehouseCreds",
            return_value=fake_creds,
            create=True,
        ):
            # Force the import inside check to use our mock
            import dbport.cli.commands.check as check_mod

            original = check_mod.check_cmd

            def patched_check(
                ctx: object,
                strict: bool = False,  # noqa: FBT001, FBT002
            ) -> object:
                # Monkey-patch the import inside check_cmd
                return original(ctx, strict)

            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "status",
                    "check",
                ],
            )

        # With real code, the try branch uses __import__ so we need a different
        # approach. Let's directly patch the WarehouseCreds class at the source.
        from types import SimpleNamespace

        fake = SimpleNamespace(catalog_uri="", catalog_token="", warehouse="")
        with patch(
            "dbport.infrastructure.credentials.WarehouseCreds",
            return_value=fake,
        ):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "status",
                    "check",
                ],
            )
        assert "WARN" in result.output
        assert "credentials" in result.output
        assert "ICEBERG_REST_URI" in result.output

    def test_check_credentials_except_branch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cover the except branch where WarehouseCreds() raises."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        monkeypatch.setenv("ICEBERG_REST_URI", "https://example.com")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "tok")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "wh")

        # Force WarehouseCreds() to raise even though env vars exist
        with patch(
            "dbport.infrastructure.credentials.WarehouseCreds",
            side_effect=Exception("forced"),
        ):
            result = runner.invoke(
                app,
                [
                    "--json",
                    "--lockfile",
                    str(lock),
                    "status",
                    "check",
                ],
            )
        data = json.loads(result.output)
        cred_check = [c for c in data["data"]["checks"] if c["name"] == "credentials"][0]
        assert cred_check["status"] == "warn"
        assert "not configured" in cred_check["detail"]

    def test_check_dependency_missing(self, tmp_path: Path) -> None:
        """Cover lines 91-94: a dependency import fails."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")

        real_import = (
            __builtins__["__import__"]
            if isinstance(__builtins__, dict)
            else __builtins__.__import__
        )

        def selective_import(
            name: str,
            *args: object,
            **kwargs: object,
        ) -> object:
            if name == "pyarrow":
                raise ImportError("no pyarrow")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=selective_import):
            result = runner.invoke(
                app,
                [
                    "--json",
                    "--lockfile",
                    str(lock),
                    "status",
                    "check",
                ],
            )
        data = json.loads(result.output)
        dep_check = [c for c in data["data"]["checks"] if c["name"] == "dependencies"][0]
        assert dep_check["status"] == "fail"
        assert "pyarrow" in dep_check["detail"]
