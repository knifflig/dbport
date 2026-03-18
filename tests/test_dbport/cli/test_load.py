"""Tests for dbp config model input command."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from dbport.adapters.secondary.lock.toml import TomlLockAdapter
from dbport.cli.main import app
from dbport.domain.entities.input import IngestRecord

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
"""


class TestModelSyncCommand:
    """Tests for ``dbp model sync`` (model.py:model_sync_cmd)."""

    def test_model_sync_success(self, tmp_path: Path) -> None:
        """Test Model sync success."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "sync",
                ],
            )
        assert result.exit_code == 0
        assert "Synced" in result.output

    def test_model_sync_json(self, tmp_path: Path) -> None:
        """Test Model sync json."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--json",
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "sync",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["synced"] == ["a.b"]
        assert data["data"]["total"] == 1

    def test_model_sync_with_model_arg(self, tmp_path: Path) -> None:
        """Test Model sync with model arg."""
        lock = tmp_path / "dbport.lock"
        _create_lock(
            lock,
            """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[models."c.d"]
agency = "c"
dataset_id = "d"
model_root = "models/d"
duckdb_path = "models/d/data/d.duckdb"
""",
        )
        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port) as mock_cls:
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "sync",
                    "c.d",
                ],
            )
        assert result.exit_code == 0
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["agency"] == "c"
        assert call_kwargs["dataset_id"] == "d"

    def test_model_sync_no_model_fails(self, tmp_path: Path) -> None:
        """Test Model sync no model fails."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        result = runner.invoke(
            app,
            ["--lockfile", str(lock), "model", "sync"],
        )
        assert result.exit_code != 0
        assert "No models found" in result.output


class TestModelLoadEdgeCases:
    """Tests for ``dbp model load`` edge cases (model.py:model_load_cmd)."""

    def test_model_load_no_inputs_human(self, tmp_path: Path) -> None:
        """Test Model load no inputs human."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)
        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "load",
                ],
            )
        assert result.exit_code == 0
        assert "No inputs configured" in result.output

    def test_model_load_json_output(self, tmp_path: Path) -> None:
        """Test Model load json output."""
        lock = tmp_path / "dbport.lock"
        _create_lock(
            lock,
            """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[[models."a.b".inputs]]
table_address = "ns.tbl1"
""",
        )
        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--json",
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "load",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["loaded"] == ["ns.tbl1"]


class TestLoadCommand:
    """Tests for TestLoadCommand."""

    def test_input_show_no_model_fails(self, tmp_path: Path) -> None:
        """Test Input show no model fails."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "config",
                "model",
                "a.b",
                "input",
            ],
        )
        assert result.exit_code != 0
        assert "Model 'a.b' not found" in result.output

    def test_input_show_no_inputs(self, tmp_path: Path) -> None:
        """Test Input show no inputs."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "--project",
                str(tmp_path),
                "config",
                "model",
                "a.b",
                "input",
            ],
        )
        assert result.exit_code == 0
        assert "No inputs configured" in result.output

    def test_input_add_configures_only_by_default(self, tmp_path: Path) -> None:
        """Test Input add configures only by default."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)
        resolved = IngestRecord(
            table_address="estat.table1",
            last_snapshot_id=123,
            last_snapshot_timestamp_ms=456,
            rows_loaded=789,
            version="2026-03-14",
        )

        def _configure_input(*args: object, **kwargs: object) -> IngestRecord:
            TomlLockAdapter(
                lock, model_key="a.b", model_root=".", duckdb_path="data/b.duckdb"
            ).write_ingest_record(resolved)
            return resolved

        mock_port.configure_input.side_effect = _configure_input

        with patch(_PATCH_TARGET, return_value=mock_port) as mock_cls:
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "config",
                    "model",
                    "a.b",
                    "input",
                    "estat.table1",
                ],
            )
        assert result.exit_code == 0
        assert "Configured input estat.table1" in result.output
        mock_port.configure_input.assert_called_once_with(
            "estat.table1",
            filters=None,
            version=None,
        )
        _, kwargs = mock_cls.call_args
        assert kwargs["load_inputs_on_init"] is False
        doc = tomllib.loads(lock.read_text())
        item = doc["models"]["a.b"]["inputs"][0]
        assert item["table_address"] == "estat.table1"
        assert item["version"] == "2026-03-14"
        assert item["last_snapshot_id"] == 123
        assert item["rows_loaded"] == 789

    def test_input_add_and_load_with_flag(self, tmp_path: Path) -> None:
        """Test Input add and load with flag."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)
        resolved = IngestRecord(
            table_address="estat.table1",
            last_snapshot_id=123,
            last_snapshot_timestamp_ms=456,
            rows_loaded=789,
            version="2026-03-14",
        )
        mock_port.load.return_value = resolved

        with patch(_PATCH_TARGET, return_value=mock_port) as mock_cls:
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "config",
                    "model",
                    "a.b",
                    "input",
                    "estat.table1",
                    "--load",
                ],
            )
        assert result.exit_code == 0
        assert "Configured and loaded input estat.table1" in result.output
        mock_port.load.assert_called_once_with("estat.table1", filters=None, version=None)
        _, kwargs = mock_cls.call_args
        assert kwargs["load_inputs_on_init"] is False

    def test_input_add_json(self, tmp_path: Path) -> None:
        """Test Input add json."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)
        resolved = IngestRecord(
            table_address="estat.table1",
            last_snapshot_id=123,
            last_snapshot_timestamp_ms=456,
            rows_loaded=789,
            version="2026-03-14",
        )
        mock_port.configure_input.return_value = resolved

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--json",
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "config",
                    "model",
                    "a.b",
                    "input",
                    "estat.table1",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["table_address"] == "estat.table1"
        assert data["data"]["version"] == "2026-03-14"
        assert data["data"]["last_snapshot_id"] == 123
        assert data["data"]["rows_loaded"] == 789
        assert data["data"]["load"] is False

    def test_input_show_existing_inputs(self, tmp_path: Path) -> None:
        """Test Input show existing inputs."""
        lock = tmp_path / "dbport.lock"
        _create_lock(
            lock,
            """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[[models."a.b".inputs]]
table_address = "ns.tbl1"

[[models."a.b".inputs]]
table_address = "ns.tbl2"
""",
        )

        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "--project",
                str(tmp_path),
                "config",
                "model",
                "a.b",
                "input",
            ],
        )
        assert result.exit_code == 0
        assert "ns.tbl1" in result.output
        assert "ns.tbl2" in result.output

    def test_input_add_with_filters_and_version_load(self, tmp_path: Path) -> None:
        """Test Input add with filters and version load."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)
        resolved = IngestRecord(
            table_address="ns.tbl1",
            last_snapshot_id=321,
            last_snapshot_timestamp_ms=654,
            rows_loaded=987,
            filters={"wstatus": "EMP"},
            version="2026-03-01",
        )

        def _load(*args: object, **kwargs: object) -> IngestRecord:
            TomlLockAdapter(
                lock, model_key="a.b", model_root=".", duckdb_path="data/b.duckdb"
            ).write_ingest_record(resolved)
            return resolved

        mock_port.load.side_effect = _load

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "config",
                    "model",
                    "a.b",
                    "input",
                    "ns.tbl1",
                    "--filter",
                    "wstatus=EMP",
                    "--version",
                    "2026-03-01",
                    "--load",
                ],
            )
        assert result.exit_code == 0
        mock_port.load.assert_called_once_with(
            "ns.tbl1",
            filters={"wstatus": "EMP"},
            version="2026-03-01",
        )
        doc = tomllib.loads(lock.read_text())
        item = doc["models"]["a.b"]["inputs"][0]
        assert item["table_address"] == "ns.tbl1"
        assert item["filters"]["wstatus"] == "EMP"
        assert item["version"] == "2026-03-01"
        assert item["last_snapshot_id"] == 321
        assert item["rows_loaded"] == 987

    def test_input_add_without_version_persists_latest_resolved_version(
        self,
        tmp_path: Path,
    ) -> None:
        """Test Input add without version persists latest resolved version."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)
        resolved = IngestRecord(
            table_address="test.table1",
            last_snapshot_id=8351008129908343306,
            last_snapshot_timestamp_ms=1773050000000,
            rows_loaded=1485615,
            version="2026-03-14",
        )

        def _configure_input(*args: object, **kwargs: object) -> IngestRecord:
            TomlLockAdapter(
                lock, model_key="a.b", model_root=".", duckdb_path="data/b.duckdb"
            ).write_ingest_record(resolved)
            return resolved

        mock_port.configure_input.side_effect = _configure_input

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "config",
                    "model",
                    "a.b",
                    "input",
                    "test.table1",
                ],
            )

        assert result.exit_code == 0, result.output
        doc = tomllib.loads(lock.read_text())
        item = doc["models"]["a.b"]["inputs"][0]
        assert item["version"] == "2026-03-14"
        assert item["last_snapshot_id"] == 8351008129908343306
        assert item["rows_loaded"] == 1485615

    def test_input_add_invalid_version_from_warehouse_fails(self, tmp_path: Path) -> None:
        """Test Input add invalid version from warehouse fails."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)
        mock_port.configure_input.side_effect = ValueError("Version '2026-03-15' not found")

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "config",
                    "model",
                    "a.b",
                    "input",
                    "test.table1",
                    "--version",
                    "2026-03-15",
                ],
            )

        assert result.exit_code != 0
        assert "2026-03-15" in result.output

    def test_model_load_uses_configured_inputs(self, tmp_path: Path) -> None:
        """Test Model load uses configured inputs."""
        lock = tmp_path / "dbport.lock"
        _create_lock(
            lock,
            """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[[models."a.b".inputs]]
table_address = "ns.tbl1"
filters = { wstatus = "EMP" }
version = "2026-03-01"
""",
        )

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port) as mock_cls:
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "load",
                    "a.b",
                ],
            )
        assert result.exit_code == 0, result.output
        assert "Loaded 1 input(s) for a.b" in result.output
        mock_port.load.assert_called_once_with(
            "ns.tbl1",
            filters={"wstatus": "EMP"},
            version="2026-03-01",
        )
        _, kwargs = mock_cls.call_args
        assert kwargs["load_inputs_on_init"] is False

    def test_model_load_update_uses_latest_snapshot(self, tmp_path: Path) -> None:
        """Test Model load update uses latest snapshot."""
        lock = tmp_path / "dbport.lock"
        _create_lock(
            lock,
            """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[[models."a.b".inputs]]
table_address = "ns.tbl1"
version = "2026-03-01"
""",
        )

        mock_port = MagicMock()
        mock_port.__enter__ = MagicMock(return_value=mock_port)
        mock_port.__exit__ = MagicMock(return_value=False)

        with patch(_PATCH_TARGET, return_value=mock_port):
            result = runner.invoke(
                app,
                [
                    "--lockfile",
                    str(lock),
                    "--project",
                    str(tmp_path),
                    "model",
                    "load",
                    "a.b",
                    "--update",
                ],
            )
        assert result.exit_code == 0, result.output
        assert "Updated 1 input(s) for a.b" in result.output
        mock_port.load.assert_called_once_with("ns.tbl1", filters=None, version=None)

    def test_input_show_json(self, tmp_path: Path) -> None:
        """Test Input show json."""
        lock = tmp_path / "dbport.lock"
        _create_lock(
            lock,
            """
[models."a.b"]
agency = "a"
dataset_id = "b"
model_root = "."
duckdb_path = "data/b.duckdb"

[[models."a.b".inputs]]
table_address = "ns.tbl1"
""",
        )

        result = runner.invoke(
            app,
            [
                "--json",
                "--lockfile",
                str(lock),
                "--project",
                str(tmp_path),
                "config",
                "model",
                "a.b",
                "input",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["inputs"][0]["table_address"] == "ns.tbl1"

    def test_input_show_no_inputs_json(self, tmp_path: Path) -> None:
        """Test Input show no inputs json."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        result = runner.invoke(
            app,
            [
                "--json",
                "--lockfile",
                str(lock),
                "--project",
                str(tmp_path),
                "config",
                "model",
                "a.b",
                "input",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["inputs"] == []

    def test_input_invalid_filter_fails(self, tmp_path: Path) -> None:
        """Test Input invalid filter fails."""
        lock = tmp_path / "dbport.lock"
        _create_lock(lock, _MODEL_LOCK)

        result = runner.invoke(
            app,
            [
                "--lockfile",
                str(lock),
                "--project",
                str(tmp_path),
                "config",
                "model",
                "a.b",
                "input",
                "ns.tbl1",
                "--filter",
                "badfilter",
            ],
        )
        assert result.exit_code != 0
        assert "Expected key=value" in result.output
