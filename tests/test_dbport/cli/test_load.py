"""Tests for dbp load command."""

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


class TestLoadCommand:
    def test_load_no_model_fails(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        result = runner.invoke(app, [
            "--lockfile", str(lock),
            "load", "estat.table",
        ])
        assert result.exit_code != 0
        assert "No models found" in result.output

    def test_load_no_args_no_inputs(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "load",
            ])
        assert result.exit_code == 0
        assert "No inputs" in result.output or "Warning" in result.output

    def test_load_single_dataset(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "load", "estat.table1",
            ])
        assert result.exit_code == 0
        assert "Loaded estat.table1" in result.output
        mock_port.load.assert_called_once_with("estat.table1")

    def test_load_single_dataset_json(self, tmp_path: Path):
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
                "load", "estat.table1",
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["loaded"] == ["estat.table1"]

    def test_load_all_configured_inputs(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[[models."a.b".inputs]]
table_address = "ns.tbl1"

[[models."a.b".inputs]]
table_address = "ns.tbl2"
''')

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "load",
            ])
        assert result.exit_code == 0
        assert "2 input(s)" in result.output
        assert mock_port.load.call_count == 2

    def test_load_all_inputs_with_filters(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[[models."a.b".inputs]]
table_address = "ns.tbl1"

[models."a.b".inputs.filters]
wstatus = "EMP"
''')

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "load",
            ])
        assert result.exit_code == 0
        mock_port.load.assert_called_once_with(
            "ns.tbl1", filters={"wstatus": "EMP"}, version=None
        )

    def test_load_all_json_output(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, '''
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[[models."a.b".inputs]]
table_address = "ns.tbl1"
''')

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(app, [
                "--json",
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "load",
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["loaded"] == ["ns.tbl1"]

    def test_load_no_inputs_json(self, tmp_path: Path):
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
                "load",
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["loaded"] == []

    def test_load_no_args_models_gone_after_resolve(self, tmp_path: Path):
        """Cover load.py line 47: models exist for resolve but gone on re-read."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        # After DBPort init succeeds, read_lock_models returns empty
        with patch(_PATCH_TARGET, return_value=mock_port), \
             patch("dbport.cli.commands.load.read_lock_models", return_value={}):
            result = runner.invoke(app, [
                "--lockfile", str(lock),
                "--project", str(tmp_path),
                "load",
            ])
        assert result.exit_code != 0
        assert "No models" in result.output
