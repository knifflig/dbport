"""Tests for dbp publish command."""

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


class TestPublishCommand:
    def test_publish_help(self):
        result = runner.invoke(app, ["publish", "--help"])
        assert result.exit_code == 0
        assert "--version" in result.output
        assert "--dry-run" in result.output
        assert "--refresh" in result.output

    def test_publish_missing_version(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text('[models."a.b"]\nagency = "a"\ndataset_id = "b"\n')
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "publish",
        ])
        # Typer should complain about missing --version
        assert result.exit_code != 0

    def test_publish_no_model_fails(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "publish", "--version", "2026-01-01",
        ])
        assert result.exit_code != 0

    def test_publish_success(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "publish", "--version", "2026-03-15",
            ])
        assert result.exit_code == 0
        assert "Published version 2026-03-15" in result.output
        mock_port.publish.assert_called_once_with(version="2026-03-15", mode=None)

    def test_publish_dry_run(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "publish", "--version", "2026-03-15", "--dry-run",
            ])
        assert result.exit_code == 0
        assert "Dry run completed" in result.output
        mock_port.publish.assert_called_once_with(version="2026-03-15", mode="dry")

    def test_publish_refresh(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "publish", "--version", "2026-03-15", "--refresh",
            ])
        assert result.exit_code == 0
        assert "Published version 2026-03-15" in result.output
        mock_port.publish.assert_called_once_with(version="2026-03-15", mode="refresh")

    def test_publish_with_message(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "publish", "--version", "2026-03-15",
                "--message", "Quarterly update",
            ])
        assert result.exit_code == 0
        assert "Quarterly update" in result.output

    def test_publish_json_output(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--json",
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "publish", "--version", "2026-03-15", "--dry-run",
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["version"] == "2026-03-15"
        assert data["data"]["mode"] == "dry"
        assert data["data"]["model"] == "a.b"

    def test_publish_model_in_output(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "publish", "--version", "2026-03-15",
            ])
        assert result.exit_code == 0
        assert "a.b" in result.output
