"""Tests for dbp init command."""

from __future__ import annotations

import tomllib
from pathlib import Path
from unittest.mock import MagicMock

import pytest
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
    """Tests for TestInitCommand."""

    def test_init_creates_scaffold(self, tmp_path: Path) -> None:
        """Test Init creates scaffold."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "my_project",
                "--agency",
                "wifor",
                "--dataset",
                "emp_test",
                "--path",
                str(repo / "my_project"),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Created model" in result.output

        project_dir = repo / "my_project"
        assert (project_dir / "main.py").exists()
        assert (project_dir / "sql" / "create_output.sql").exists()
        assert (project_dir / "sql" / "main.sql").exists()
        assert (project_dir / "data").is_dir()

    def test_init_writes_to_repo_root_lock(self, tmp_path: Path) -> None:
        """Init must write to the repo-root dbport.lock, not model dir."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--agency",
                "test_agency",
                "--dataset",
                "test_ds",
                "--path",
                str(repo / "proj"),
            ],
        )
        assert result.exit_code == 0, result.output

        # Repo-root lock should contain the model
        repo_lock = repo / "dbport.lock"
        assert repo_lock.exists(), "Lock file should be at repo root"
        lock_text = repo_lock.read_text()
        assert "test_agency" in lock_text
        assert "test_ds" in lock_text

        # Model dir should NOT have its own lock
        assert not (repo / "proj" / "dbport.lock").exists()

    def test_init_stores_relative_model_root(self, tmp_path: Path) -> None:
        """model_root in lock must be relative to repo root."""
        repo = _setup_repo(tmp_path)
        runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--agency",
                "a",
                "--dataset",
                "d",
                "--path",
                str(repo / "examples" / "proj"),
            ],
        )
        lock = repo / "dbport.lock"
        doc = tomllib.loads(lock.read_text())
        model = doc["models"]["a.d"]
        assert model["model_root"] == "examples/proj"
        assert model["duckdb_path"] == "examples/proj/data/d.duckdb"

    def test_init_scaffold_ddl_no_trailing_comma(self, tmp_path: Path) -> None:
        """Generated DDL template must not have a trailing comma before closing paren."""
        repo = _setup_repo(tmp_path)
        runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--agency",
                "a",
                "--dataset",
                "d",
                "--path",
                str(repo / "proj"),
            ],
        )
        ddl = (repo / "proj" / "sql" / "create_output.sql").read_text()
        assert ",\n);" not in ddl, "Trailing comma before closing paren is invalid SQL"

    def test_init_hybrid_template_creates_main_py(self, tmp_path: Path) -> None:
        """Test Init hybrid template creates main py."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--template",
                "hybrid",
                "--agency",
                "a",
                "--dataset",
                "d",
                "--path",
                str(repo / "proj"),
            ],
        )
        assert result.exit_code == 0
        assert (repo / "proj" / "main.py").exists()

    def test_init_sql_template_creates_main_py(self, tmp_path: Path) -> None:
        """Test Init sql template creates main py."""
        repo = _setup_repo(tmp_path)
        runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--template",
                "sql",
                "--path",
                str(repo / "proj"),
            ],
        )
        assert (repo / "proj" / "main.py").exists()

    def test_init_refuses_existing_dir_without_force(self, tmp_path: Path) -> None:
        """Test Init refuses existing dir without force."""
        repo = _setup_repo(tmp_path)
        target = repo / "proj"
        target.mkdir()
        (target / "existing_file").write_text("x")
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--path",
                str(target),
            ],
        )
        assert result.exit_code != 0
        assert "already" in result.output
        assert "force" in result.output.lower()

    def test_init_force_overwrites(self, tmp_path: Path) -> None:
        """Test Init force overwrites."""
        repo = _setup_repo(tmp_path)
        target = repo / "proj"
        target.mkdir()
        (target / "existing_file").write_text("x")
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--force",
                "--path",
                str(target),
            ],
        )
        assert result.exit_code == 0
        assert "Created model" in result.output

    def test_init_json_output(self, tmp_path: Path) -> None:
        """Test Init json output."""
        import json

        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "init",
                "proj",
                "--agency",
                "a",
                "--dataset",
                "d",
                "--path",
                str(repo / "proj"),
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["command"] == "init"
        assert data["data"]["agency"] == "a"
        assert data["data"]["dataset"] == "d"
        assert data["data"]["model_root"] == "proj"

    def test_init_invalid_template(self, tmp_path: Path) -> None:
        """Test Init invalid template."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--template",
                "invalid",
                "--path",
                str(repo / "proj"),
            ],
        )
        assert result.exit_code != 0

    def test_init_default_name(self, tmp_path: Path) -> None:
        """Test Init default name."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "--path",
                str(repo / "default_proj"),
            ],
        )
        assert result.exit_code == 0

    def test_init_sets_default_model(self, tmp_path: Path) -> None:
        """First init should set default_model in lock file."""
        repo = _setup_repo(tmp_path)
        runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--agency",
                "a",
                "--dataset",
                "d",
                "--path",
                str(repo / "proj"),
            ],
        )
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["default_model"] == "a.d"

    def test_init_updates_default_model(self, tmp_path: Path) -> None:
        """Second init should change default_model to the new model."""
        repo = _setup_repo(tmp_path)
        runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj1",
                "--agency",
                "a",
                "--dataset",
                "d1",
                "--path",
                str(repo / "proj1"),
            ],
        )
        runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj2",
                "--agency",
                "a",
                "--dataset",
                "d2",
                "--path",
                str(repo / "proj2"),
            ],
        )
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["default_model"] == "a.d2"
        # Both models should still exist
        assert "a.d1" in doc["models"]
        assert "a.d2" in doc["models"]

    def test_init_python_template(self, tmp_path: Path) -> None:
        """Test Init python template."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--template",
                "python",
                "--path",
                str(repo / "proj"),
            ],
        )
        assert result.exit_code == 0
        assert (repo / "proj" / "main.py").exists()
        content = (repo / "proj" / "main.py").read_text()
        assert 'port.execute("sql/main.sql")' in content

    def test_init_target_outside_repo(self, tmp_path: Path) -> None:
        """When target dir is outside repo root, model_root should be absolute."""
        repo = _setup_repo(tmp_path)
        outside = tmp_path / "outside_proj"
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--agency",
                "a",
                "--dataset",
                "d",
                "--path",
                str(outside),
            ],
        )
        assert result.exit_code == 0
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        model = doc["models"]["a.d"]
        # model_root should be the absolute path since it's outside repo
        assert str(outside) in model["model_root"]

    def test_init_default_path_uses_models_folder(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Without --path, target should be <repo>/models/<agency>/<dataset>."""
        repo = _setup_repo(tmp_path)
        monkeypatch.chdir(repo)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "my_model",
                "--agency",
                "a",
                "--dataset",
                "d",
            ],
        )
        assert result.exit_code == 0
        assert (repo / "models" / "a" / "d" / "sql" / "create_output.sql").exists()

    def test_init_existing_empty_dir_succeeds(self, tmp_path: Path) -> None:
        """Empty existing dir should not block init (no files to conflict)."""
        repo = _setup_repo(tmp_path)
        target = repo / "proj"
        target.mkdir()
        # Dir exists but is empty — should succeed without --force
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "proj",
                "--path",
                str(target),
            ],
        )
        assert result.exit_code == 0
        assert "Created model" in result.output

    def test_init_dotted_name_parses_agency_dataset(self, tmp_path: Path) -> None:
        """'dbp init test.brand_new' should parse agency=test, dataset=brand_new."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "test.brand_new",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Created model" in result.output
        # Should be at models/test/brand_new
        assert (repo / "models" / "test" / "brand_new" / "sql" / "create_output.sql").exists()
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        model = doc["models"]["test.brand_new"]
        assert model["agency"] == "test"
        assert model["dataset_id"] == "brand_new"

    def test_init_dotted_name_with_path_override(self, tmp_path: Path) -> None:
        """'dbp init test.brand_new --path brand_new' creates at models/brand_new."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "test.brand_new",
                "--path",
                "brand_new",
            ],
        )
        assert result.exit_code == 0, result.output
        # Should be at models/brand_new (--path relative to models_folder)
        assert (repo / "models" / "brand_new" / "sql" / "create_output.sql").exists()
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        model = doc["models"]["test.brand_new"]
        assert model["model_root"] == "models/brand_new"

    def test_init_custom_models_folder(self, tmp_path: Path) -> None:
        """Models folder can be changed via config, and init respects it."""
        repo = _setup_repo(tmp_path)
        # Set models_folder to "examples"
        lock = repo / "dbport.lock"
        lock.write_text('models_folder = "examples"\n')
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "test.my_model",
            ],
        )
        assert result.exit_code == 0, result.output
        assert (repo / "examples" / "test" / "my_model" / "sql" / "create_output.sql").exists()

    def test_init_path_relative_to_models_folder(self, tmp_path: Path) -> None:
        """--path is relative to models_folder, not CWD."""
        repo = _setup_repo(tmp_path)
        lock = repo / "dbport.lock"
        lock.write_text('models_folder = "examples"\n')
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "test.thing",
                "--path",
                "custom_dir",
            ],
        )
        assert result.exit_code == 0, result.output
        assert (repo / "examples" / "custom_dir" / "sql" / "create_output.sql").exists()

    def test_init_absolute_path_bypasses_models_folder(self, tmp_path: Path) -> None:
        """Absolute --path should bypass models_folder entirely."""
        repo = _setup_repo(tmp_path)
        abs_target = tmp_path / "absolute_target"
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "test.thing",
                "--path",
                str(abs_target),
            ],
        )
        assert result.exit_code == 0, result.output
        assert (abs_target / "sql" / "create_output.sql").exists()


def _create_lock(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


_EXISTING_LOCK = """\
[models."test.table1"]
agency = "test"
dataset_id = "table1"
model_root = "examples/minimal"
duckdb_path = "examples/minimal/data/table1.duckdb"
run_hook = "sql/main.sql"
"""

_TWO_MODELS_LOCK = """\
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
"""


def _mock_dbport() -> MagicMock:
    mock_port = MagicMock()
    mock_port.__enter__ = MagicMock(return_value=mock_port)
    mock_port.__exit__ = MagicMock(return_value=False)
    return mock_port


class TestInitExistingModelValidation:
    """Tests for fast failure when init targets an existing model."""

    def test_init_with_existing_model_key_fails(self, tmp_path: Path) -> None:
        """Test Init with existing model key fails."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "test.table1",
            ],
        )
        assert result.exit_code != 0
        assert "already exists" in result.output
        assert "model sync" in result.output

    def test_init_with_agency_dataset_fails_for_existing_model(self, tmp_path: Path) -> None:
        """Test Init with agency dataset fails for existing model."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "--agency",
                "test",
                "--dataset",
                "table1",
            ],
        )
        assert result.exit_code != 0
        assert "already exists" in result.output

    def test_init_with_unknown_name_scaffolds(self, tmp_path: Path) -> None:
        """Dbp init brand_new should scaffold when name not in lock."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "brand_new",
                "--path",
                str(repo / "brand_new"),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Created model" in result.output

    def test_init_with_unknown_agency_dataset_scaffolds(self, tmp_path: Path) -> None:
        """Dbp init --agency new --dataset thing should scaffold."""
        repo = _setup_repo(tmp_path)
        _create_lock(repo / "dbport.lock", _EXISTING_LOCK)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
                "--agency",
                "new",
                "--dataset",
                "thing",
                "--path",
                str(repo / "new_thing"),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Created model" in result.output

    def test_init_no_args_fails(self, tmp_path: Path) -> None:
        """Test Init no args fails."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "init",
            ],
        )
        assert result.exit_code != 0
        assert "No model name specified" in result.output
        assert "dbp model sync" in result.output
