"""Tests for dbp init command."""

from __future__ import annotations

import tomllib
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()

_PATCH_TARGET = "dbport.adapters.primary.client.DBPort"


def _setup_repo(tmp_path: Path) -> Path:
    """Create a minimal repo root with pyproject.toml."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("[project]\n")
    return repo


class TestInitCommand:
    def test_init_creates_scaffold(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "my_project",
            "--agency", "wifor",
            "--dataset", "emp_test",
            "--path", str(repo / "my_project"),
        ])
        assert result.exit_code == 0, result.output
        assert "Created model" in result.output

        project_dir = repo / "my_project"
        assert (project_dir / "sql" / "create_output.sql").exists()
        assert (project_dir / "sql" / "main.sql").exists()
        assert (project_dir / "data").is_dir()

    def test_init_writes_to_repo_root_lock(self, tmp_path: Path):
        """init must write to the repo-root dbport.lock, not model dir."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--agency", "test_agency",
            "--dataset", "test_ds",
            "--path", str(repo / "proj"),
        ])
        assert result.exit_code == 0, result.output

        # Repo-root lock should contain the model
        repo_lock = repo / "dbport.lock"
        assert repo_lock.exists(), "Lock file should be at repo root"
        lock_text = repo_lock.read_text()
        assert "test_agency" in lock_text
        assert "test_ds" in lock_text

        # Model dir should NOT have its own lock
        assert not (repo / "proj" / "dbport.lock").exists()

    def test_init_stores_relative_model_root(self, tmp_path: Path):
        """model_root in lock must be relative to repo root."""
        repo = _setup_repo(tmp_path)
        runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--agency", "a",
            "--dataset", "d",
            "--path", str(repo / "examples" / "proj"),
        ])
        lock = repo / "dbport.lock"
        doc = tomllib.loads(lock.read_text())
        model = doc["models"]["a.d"]
        assert model["model_root"] == "examples/proj"
        assert model["duckdb_path"] == "examples/proj/data/d.duckdb"

    def test_init_hybrid_template_creates_run_py(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--template", "hybrid",
            "--agency", "a",
            "--dataset", "d",
            "--path", str(repo / "proj"),
        ])
        assert result.exit_code == 0
        assert (repo / "proj" / "run.py").exists()

    def test_init_sql_template_no_run_py(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--template", "sql",
            "--path", str(repo / "proj"),
        ])
        assert not (repo / "proj" / "run.py").exists()

    def test_init_refuses_existing_dir_without_force(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        target = repo / "proj"
        target.mkdir()
        (target / "existing_file").write_text("x")
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--path", str(target),
        ])
        assert result.exit_code != 0
        assert "already" in result.output
        assert "force" in result.output.lower()

    def test_init_force_overwrites(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        target = repo / "proj"
        target.mkdir()
        (target / "existing_file").write_text("x")
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--force",
            "--path", str(target),
        ])
        assert result.exit_code == 0
        assert "Created model" in result.output

    def test_init_json_output(self, tmp_path: Path):
        import json
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--json",
            "--project", str(repo),
            "init", "proj",
            "--agency", "a",
            "--dataset", "d",
            "--path", str(repo / "proj"),
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["command"] == "init"
        assert data["data"]["agency"] == "a"
        assert data["data"]["dataset"] == "d"
        assert data["data"]["model_root"] == "proj"

    def test_init_invalid_template(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--template", "invalid",
            "--path", str(repo / "proj"),
        ])
        assert result.exit_code != 0

    def test_init_default_name(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init",
            "--path", str(repo / "default_proj"),
        ])
        assert result.exit_code == 0

    def test_init_sets_default_model(self, tmp_path: Path):
        """First init should set default_model in lock file."""
        repo = _setup_repo(tmp_path)
        runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--agency", "a",
            "--dataset", "d",
            "--path", str(repo / "proj"),
        ])
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["default_model"] == "a.d"

    def test_init_updates_default_model(self, tmp_path: Path):
        """Second init should change default_model to the new model."""
        repo = _setup_repo(tmp_path)
        runner.invoke(app, [
            "--project", str(repo),
            "init", "proj1",
            "--agency", "a",
            "--dataset", "d1",
            "--path", str(repo / "proj1"),
        ])
        runner.invoke(app, [
            "--project", str(repo),
            "init", "proj2",
            "--agency", "a",
            "--dataset", "d2",
            "--path", str(repo / "proj2"),
        ])
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["default_model"] == "a.d2"
        # Both models should still exist
        assert "a.d1" in doc["models"]
        assert "a.d2" in doc["models"]

    def test_init_python_template(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--template", "python",
            "--path", str(repo / "proj"),
        ])
        assert result.exit_code == 0
        assert (repo / "proj" / "run.py").exists()
        content = (repo / "proj" / "run.py").read_text()
        assert "DBPort" in content

    def test_init_target_outside_repo(self, tmp_path: Path):
        """When target dir is outside repo root, model_root should be absolute."""
        repo = _setup_repo(tmp_path)
        outside = tmp_path / "outside_proj"
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--agency", "a",
            "--dataset", "d",
            "--path", str(outside),
        ])
        assert result.exit_code == 0
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        model = doc["models"]["a.d"]
        # model_root should be the absolute path since it's outside repo
        assert str(outside) in model["model_root"]

    def test_init_default_path_uses_name(self, tmp_path: Path, monkeypatch):
        """Without --path, target should be CWD / <name>."""
        repo = _setup_repo(tmp_path)
        monkeypatch.chdir(repo)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "my_model",
            "--agency", "a",
            "--dataset", "d",
        ])
        assert result.exit_code == 0
        assert (repo / "my_model" / "sql" / "create_output.sql").exists()

    def test_init_existing_empty_dir_succeeds(self, tmp_path: Path):
        """Empty existing dir should not block init (no files to conflict)."""
        repo = _setup_repo(tmp_path)
        target = repo / "proj"
        target.mkdir()
        # Dir exists but is empty — should succeed without --force
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--path", str(target),
        ])
        assert result.exit_code == 0
        assert "Created model" in result.output


def _create_lock(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


_EXISTING_LOCK = '''\
[models."test.table1"]
agency = "test"
dataset_id = "table1"
model_root = "examples/minimal"
duckdb_path = "examples/minimal/data/table1.duckdb"
run_hook = "sql/main.sql"
'''

_TWO_MODELS_LOCK = '''\
[models."test.table1"]
agency = "test"
dataset_id = "table1"
model_root = "examples/minimal"
duckdb_path = "examples/minimal/data/table1.duckdb"

[models."test.table2"]
agency = "test"
dataset_id = "table2"
model_root = "examples/other"
duckdb_path = "examples/other/data/table2.duckdb"
'''


def _mock_dbport():
    mock_port = MagicMock()
    mock_port.__enter__ = MagicMock(return_value=mock_port)
    mock_port.__exit__ = MagicMock(return_value=False)
    return mock_port


class TestInitSyncExistingModel:
    """Tests for `dbp init <model_key>` syncing an existing model."""

    def test_init_with_existing_model_key_syncs(self, tmp_path: Path):
        """dbp init test.table1 should sync, not scaffold, when model exists."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        (repo / "examples" / "minimal").mkdir(parents=True, exist_ok=True)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--project", str(repo),
                "init", "test.table1",
            ])
        assert result.exit_code == 0, result.output
        assert "Synced" in result.output
        assert "Created model" not in result.output

    def test_init_with_agency_dataset_syncs_existing(self, tmp_path: Path):
        """dbp init --agency test --dataset table1 should sync when model exists."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        (repo / "examples" / "minimal").mkdir(parents=True, exist_ok=True)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--project", str(repo),
                "init", "--agency", "test", "--dataset", "table1",
            ])
        assert result.exit_code == 0, result.output
        assert "Synced" in result.output
        assert "Created model" not in result.output

    def test_init_with_unknown_name_scaffolds(self, tmp_path: Path):
        """dbp init brand_new should scaffold when name not in lock."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "brand_new",
            "--path", str(repo / "brand_new"),
        ])
        assert result.exit_code == 0, result.output
        assert "Created model" in result.output

    def test_init_with_unknown_agency_dataset_scaffolds(self, tmp_path: Path):
        """dbp init --agency new --dataset thing should scaffold."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "--agency", "new", "--dataset", "thing",
            "--path", str(repo / "new_thing"),
        ])
        assert result.exit_code == 0, result.output
        assert "Created model" in result.output

    def test_init_no_args_syncs_all(self, tmp_path: Path):
        """dbp init with no args should sync all models."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _TWO_MODELS_LOCK)
        (repo / "examples" / "minimal").mkdir(parents=True, exist_ok=True)
        (repo / "examples" / "other").mkdir(parents=True, exist_ok=True)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--project", str(repo),
                "init",
            ])
        assert result.exit_code == 0, result.output
        assert "Synced 2/2" in result.output

    def test_init_single_model_json_output(self, tmp_path: Path):
        """JSON output for single model sync."""
        import json
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        (repo / "examples" / "minimal").mkdir(parents=True, exist_ok=True)
        mp = _mock_dbport()

        with patch(_PATCH_TARGET, return_value=mp):
            result = runner.invoke(app, [
                "--json",
                "--project", str(repo),
                "init", "test.table1",
            ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["ok"] is True
        assert "test.table1" in data["data"]["synced"]
