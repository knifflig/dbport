"""Tests for dbp model publish command."""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)

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

[[models."a.b".versions]]
version = "2026-03-15"
completed = true
"""

_MODEL_LOCK_NO_VERSIONS = """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"
"""

_MULTI_MODEL_LOCK = """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[[models."a.b".versions]]
version = "2026-03-14"
completed = true

[models."c.d"]
agency = "c"
dataset_id = "d"
model_root = "models/d"
duckdb_path = "models/d/data/d.duckdb"

[[models."c.d".versions]]
version = "2026-03-15"
completed = true
"""


def _mock_port() -> MagicMock:
    mock_port = MagicMock()
    mock_port.__enter__ = MagicMock(return_value=mock_port)
    mock_port.__exit__ = MagicMock(return_value=False)
    return mock_port


class TestPublishCommand:
    """Tests for TestPublishCommand."""

    def test_publish_help(self) -> None:
        """Test Publish help."""
        result = runner.invoke(app, ["model", "publish", "--help"])
        assert result.exit_code == 0
        output = _strip_ansi(result.output)
        assert "--version" in output
        assert "--dry-run" in output
        assert "--refresh" in output

    def test_publish_no_version_no_completed_fails(self, tmp_path: Path) -> None:
        """When no --version and no completed versions in lock, fail."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK_NO_VERSIONS)
        mp = _mock_port()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "publish",
                ],
            )
        assert result.exit_code != 0
        assert "No completed versions" in result.output

    def test_publish_no_version_uses_latest(self, tmp_path: Path) -> None:
        """When no --version, latest completed version from lock is used."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_port()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "publish",
                ],
            )
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-03-15", mode=None)

    def test_publish_no_model_fails(self, tmp_path: Path) -> None:
        """Test Publish no model fails."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "model",
                "publish",
                "--version",
                "2026-01-01",
            ],
        )
        assert result.exit_code != 0

    def test_publish_success(self, tmp_path: Path) -> None:
        """Test Publish success."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_port()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "publish",
                    "--version",
                    "2026-03-15",
                ],
            )
        assert result.exit_code == 0
        assert "Published version 2026-03-15" in result.output
        mp.publish.assert_called_once_with(version="2026-03-15", mode=None)

    def test_publish_dry_run(self, tmp_path: Path) -> None:
        """Test Publish dry run."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_port()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "publish",
                    "--version",
                    "2026-03-15",
                    "--dry-run",
                ],
            )
        assert result.exit_code == 0
        assert "Dry run completed" in result.output
        mp.publish.assert_called_once_with(version="2026-03-15", mode="dry")

    def test_publish_refresh(self, tmp_path: Path) -> None:
        """Test Publish refresh."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_port()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "publish",
                    "--version",
                    "2026-03-15",
                    "--refresh",
                ],
            )
        assert result.exit_code == 0
        assert "Published version 2026-03-15" in result.output
        mp.publish.assert_called_once_with(version="2026-03-15", mode="refresh")

    def test_publish_refresh_no_version_uses_latest(self, tmp_path: Path) -> None:
        """Dbp publish --refresh with no --version uses latest completed."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_port()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "publish",
                    "--refresh",
                ],
            )
        assert result.exit_code == 0
        mp.publish.assert_called_once_with(version="2026-03-15", mode="refresh")

    def test_publish_with_model_positional_arg(self, tmp_path: Path) -> None:
        """Test Publish with model positional arg."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MULTI_MODEL_LOCK)
        mp = _mock_port()

        with patch(_PATCH_TARGET, return_value=mp) as mock_cls:
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "publish",
                    "c.d",
                    "--version",
                    "2026-03-15",
                ],
            )
        assert result.exit_code == 0
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["agency"] == "c"
        assert call_kwargs["dataset_id"] == "d"

    def test_publish_with_message(self, tmp_path: Path) -> None:
        """Test Publish with message."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_port()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "publish",
                    "--version",
                    "2026-03-15",
                    "--message",
                    "Quarterly update",
                ],
            )
        assert result.exit_code == 0
        assert "Quarterly update" in result.output

    def test_publish_json_output(self, tmp_path: Path) -> None:
        """Test Publish json output."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_port()

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
                    "publish",
                    "--version",
                    "2026-03-15",
                    "--dry-run",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["version"] == "2026-03-15"
        assert data["data"]["mode"] == "dry"
        assert data["data"]["model"] == "a.b"

    def test_publish_model_in_output(self, tmp_path: Path) -> None:
        """Test Publish model in output."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mp = _mock_port()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "publish",
                    "--version",
                    "2026-03-15",
                ],
            )
        assert result.exit_code == 0
        assert "a.b" in result.output
