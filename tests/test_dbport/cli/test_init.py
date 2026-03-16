"""Tests for dbp init command (scaffold only)."""

from __future__ import annotations

import tomllib
from pathlib import Path

from typer.testing import CliRunner

from dbport.cli.main import app

runner = CliRunner()


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
        """Init must write to the repo-root dbport.lock, not model dir."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--agency", "test_agency",
            "--dataset", "test_ds",
            "--path", str(repo / "proj"),
        ])
        assert result.exit_code == 0, result.output

        repo_lock = repo / "dbport.lock"
        assert repo_lock.exists(), "Lock file should be at repo root"
        lock_text = repo_lock.read_text()
        assert "test_agency" in lock_text
        assert "test_ds" in lock_text

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

    def test_init_scaffold_ddl_no_trailing_comma(self, tmp_path: Path):
        """Generated DDL template must not have a trailing comma before closing paren."""
        repo = _setup_repo(tmp_path)
        runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--agency", "a",
            "--dataset", "d",
            "--path", str(repo / "proj"),
        ])
        ddl = (repo / "proj" / "sql" / "create_output.sql").read_text()
        assert ",\n);" not in ddl, "Trailing comma before closing paren is invalid SQL"

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

    def test_init_no_args_fails(self, tmp_path: Path):
        """Init with no arguments should error (use dbp sync instead)."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init",
        ])
        assert result.exit_code != 0
        assert "sync" in result.output.lower()

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
        assert str(outside) in model["model_root"]

    def test_init_default_path_uses_models_folder(self, tmp_path: Path, monkeypatch):
        """Without --path, target should be <repo>/models/<agency>/<dataset>."""
        repo = _setup_repo(tmp_path)
        monkeypatch.chdir(repo)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "my_model",
            "--agency", "a",
            "--dataset", "d",
        ])
        assert result.exit_code == 0
        assert (repo / "models" / "a" / "d" / "sql" / "create_output.sql").exists()

    def test_init_existing_empty_dir_succeeds(self, tmp_path: Path):
        """Empty existing dir should not block init."""
        repo = _setup_repo(tmp_path)
        target = repo / "proj"
        target.mkdir()
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "proj",
            "--path", str(target),
        ])
        assert result.exit_code == 0
        assert "Created model" in result.output

    def test_init_dotted_name_parses_agency_dataset(self, tmp_path: Path):
        """'dbp init test.brand_new' should parse agency=test, dataset=brand_new."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "test.brand_new",
        ])
        assert result.exit_code == 0, result.output
        assert "Created model" in result.output
        assert (repo / "models" / "test" / "brand_new" / "sql" / "create_output.sql").exists()
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        model = doc["models"]["test.brand_new"]
        assert model["agency"] == "test"
        assert model["dataset_id"] == "brand_new"

    def test_init_dotted_name_with_path_override(self, tmp_path: Path):
        """'dbp init test.brand_new --path brand_new' creates at models/brand_new."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "test.brand_new",
            "--path", "brand_new",
        ])
        assert result.exit_code == 0, result.output
        assert (repo / "models" / "brand_new" / "sql" / "create_output.sql").exists()
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        model = doc["models"]["test.brand_new"]
        assert model["model_root"] == "models/brand_new"

    def test_init_custom_models_folder(self, tmp_path: Path):
        """Models folder can be changed via config, and init respects it."""
        repo = _setup_repo(tmp_path)
        lock = repo / "dbport.lock"
        lock.write_text('models_folder = "examples"\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "test.my_model",
        ])
        assert result.exit_code == 0, result.output
        assert (repo / "examples" / "test" / "my_model" / "sql" / "create_output.sql").exists()

    def test_init_path_relative_to_models_folder(self, tmp_path: Path):
        """--path is relative to models_folder, not CWD."""
        repo = _setup_repo(tmp_path)
        lock = repo / "dbport.lock"
        lock.write_text('models_folder = "examples"\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "test.thing",
            "--path", "custom_dir",
        ])
        assert result.exit_code == 0, result.output
        assert (repo / "examples" / "custom_dir" / "sql" / "create_output.sql").exists()

    def test_init_absolute_path_bypasses_models_folder(self, tmp_path: Path):
        """Absolute --path should bypass models_folder entirely."""
        repo = _setup_repo(tmp_path)
        abs_target = tmp_path / "absolute_target"
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "test.thing",
            "--path", str(abs_target),
        ])
        assert result.exit_code == 0, result.output
        assert (abs_target / "sql" / "create_output.sql").exists()

    def test_init_path_only_no_name(self, tmp_path: Path):
        """dbp init --path <dir> with no name should still scaffold."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "init",
            "--path", str(repo / "default_proj"),
        ])
        assert result.exit_code == 0
        assert "Created model" in result.output

    def test_init_existing_model_in_lock_errors(self, tmp_path: Path):
        """Init should error if the model already exists in the lock file."""
        repo = _setup_repo(tmp_path)
        lock = repo / "dbport.lock"
        lock.write_text(
            '[models."test.table1"]\n'
            'agency = "test"\n'
            'dataset_id = "table1"\n'
            'model_root = "models/table1"\n'
        )
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "test.table1",
        ])
        assert result.exit_code != 0
        assert "already exists" in result.output
        assert "sync" in result.output.lower()

    def test_init_existing_model_force_rescaffolds(self, tmp_path: Path):
        """Init --force should re-scaffold even if model exists in lock."""
        repo = _setup_repo(tmp_path)
        lock = repo / "dbport.lock"
        lock.write_text(
            '[models."test.table1"]\n'
            'agency = "test"\n'
            'dataset_id = "table1"\n'
            'model_root = "models/table1"\n'
        )
        result = runner.invoke(app, [
            "--project", str(repo),
            "init", "test.table1",
            "--force",
        ])
        assert result.exit_code == 0, result.output
        assert "Created model" in result.output
