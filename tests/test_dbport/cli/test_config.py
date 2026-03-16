"""Tests for dbp config command."""

from __future__ import annotations

import json
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


def _write_lock(repo: Path, content: str) -> Path:
    lock = repo / "dbport.lock"
    lock.write_text(content, encoding="utf-8")
    return lock


class TestConfigDefaultShow:
    def test_show_when_no_lock_file(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "default",
        ])
        assert result.exit_code == 0
        assert "No default model" in result.output

    def test_show_when_no_default_set(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "default",
        ])
        assert result.exit_code == 0
        assert "No default model" in result.output

    def test_show_current_default(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, (
            'default_model = "a.x"\n\n'
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n'
        ))
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "default",
        ])
        assert result.exit_code == 0
        assert "a.x" in result.output

    def test_show_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, (
            'default_model = "a.x"\n\n'
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n'
        ))
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "default",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["default_model"] == "a.x"

    def test_show_json_output_no_default(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "default",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["default_model"] is None


class TestConfigDefaultSet:
    def test_set_valid_model(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, (
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n\n'
            '[models."b.y"]\nagency = "b"\ndataset_id = "y"\nmodel_root = "sub"\n'
        ))
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "default", "b.y",
        ])
        assert result.exit_code == 0
        assert "b.y" in result.output

        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["default_model"] == "b.y"

    def test_set_invalid_model_errors(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "default", "nonexistent.model",
        ])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_set_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n')
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "default", "a.x",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["default_model"] == "a.x"

    def test_set_preserves_models(self, tmp_path: Path):
        """Setting default_model must not lose any model data."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, (
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "."\n'
            'duckdb_path = "data/x.duckdb"\n'
        ))
        runner.invoke(app, [
            "--project", str(repo),
            "config", "default", "a.x",
        ])
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["default_model"] == "a.x"
        assert doc["models"]["a.x"]["agency"] == "a"
        assert doc["models"]["a.x"]["dataset_id"] == "x"


class TestConfigFolder:
    def test_show_default_folder(self, tmp_path: Path):
        """Without any setting, models_folder defaults to 'models'."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "folder",
        ])
        assert result.exit_code == 0
        assert "models" in result.output

    def test_show_custom_folder(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, 'models_folder = "examples"\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "folder",
        ])
        assert result.exit_code == 0
        assert "examples" in result.output

    def test_set_folder(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n')
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "folder", "examples",
        ])
        assert result.exit_code == 0
        assert "examples" in result.output
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models_folder"] == "examples"

    def test_set_folder_strips_slashes(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, "")
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "folder", "/examples/",
        ])
        assert result.exit_code == 0
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models_folder"] == "examples"

    def test_show_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, 'models_folder = "src/models"\n')
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "folder",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["models_folder"] == "src/models"

    def test_set_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, "")
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "folder", "custom",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["models_folder"] == "custom"

    def test_set_preserves_models(self, tmp_path: Path):
        """Setting models_folder must not lose any model data."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, (
            'default_model = "a.x"\n\n'
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "."\n'
            'duckdb_path = "data/x.duckdb"\n'
        ))
        runner.invoke(app, [
            "--project", str(repo),
            "config", "folder", "examples",
        ])
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models_folder"] == "examples"
        assert doc["default_model"] == "a.x"
        assert doc["models"]["a.x"]["agency"] == "a"


class TestConfigRunHook:
    _LOCK_WITH_HOOK = (
        'default_model = "a.x"\n\n'
        '[models."a.x"]\n'
        'agency = "a"\n'
        'dataset_id = "x"\n'
        'model_root = "."\n'
        'duckdb_path = "data/x.duckdb"\n'
        'run_hook = "sql/main.sql"\n'
    )

    _LOCK_NO_HOOK = (
        'default_model = "a.x"\n\n'
        '[models."a.x"]\n'
        'agency = "a"\n'
        'dataset_id = "x"\n'
        'model_root = "."\n'
        'duckdb_path = "data/x.duckdb"\n'
    )

    def test_show_current_hook(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_WITH_HOOK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "run-hook",
        ])
        assert result.exit_code == 0
        assert "sql/main.sql" in result.output

    def test_show_no_hook_set(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_NO_HOOK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "run-hook",
        ])
        assert result.exit_code == 0
        assert "No run hook" in result.output

    def test_show_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_WITH_HOOK)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "run-hook",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["run_hook"] == "sql/main.sql"
        assert data["data"]["model"] == "a.x"

    def test_set_hook(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_NO_HOOK)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "run-hook", "sql/transform.sql",
        ])
        assert result.exit_code == 0
        assert "sql/transform.sql" in result.output

        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models"]["a.x"]["run_hook"] == "sql/transform.sql"

    def test_set_hook_json_output(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_NO_HOOK)
        result = runner.invoke(app, [
            "--json", "--project", str(repo),
            "config", "run-hook", "run.py",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["run_hook"] == "run.py"

    def test_set_hook_normalizes_path(self, tmp_path: Path):
        """Hook path relative to CWD is normalized relative to model_root."""
        import os

        repo = _setup_repo(tmp_path)
        model_lock = (
            'default_model = "a.x"\n\n'
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "models/x"\n'
            'duckdb_path = "models/x/data/x.duckdb"\n'
        )
        _write_lock(repo, model_lock)
        model_dir = repo / "models" / "x"
        model_dir.mkdir(parents=True)

        old_cwd = os.getcwd()
        try:
            os.chdir(str(repo))
            result = runner.invoke(app, [
                "--project", str(repo),
                "config", "run-hook", "models/x/sql/main.sql",
            ])
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models"]["a.x"]["run_hook"] == "sql/main.sql"


class TestConfigUnknownKey:
    def test_unknown_key_errors(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config", "nonexistent",
        ])
        assert result.exit_code != 0
        assert "Unknown config key" in result.output

    def test_no_key_shows_help(self, tmp_path: Path):
        repo = _setup_repo(tmp_path)
        result = runner.invoke(app, [
            "--project", str(repo),
            "config",
        ])
        assert result.exit_code in (0, 2)
        assert "Usage" in result.output or "config" in result.output
