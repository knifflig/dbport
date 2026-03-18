"""Tests for dbp config command."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest
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
    """Tests for TestConfigDefaultShow."""

    def test_show_when_no_lock_file(self, tmp_path: Path) -> None:
        """Test Show when no lock file."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "model",
            ],
        )
        assert result.exit_code == 0
        assert "No default model" in result.output

    def test_show_when_no_default_set(self, tmp_path: Path) -> None:
        """Test Show when no default set."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "model",
            ],
        )
        assert result.exit_code == 0
        assert "No default model" in result.output

    def test_show_current_default(self, tmp_path: Path) -> None:
        """Test Show current default."""
        repo = _setup_repo(tmp_path)
        _write_lock(
            repo, ('default_model = "a.x"\n\n[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        )
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "model",
            ],
        )
        assert result.exit_code == 0
        assert "a.x" in result.output

    def test_show_json_output(self, tmp_path: Path) -> None:
        """Test Show json output."""
        repo = _setup_repo(tmp_path)
        _write_lock(
            repo, ('default_model = "a.x"\n\n[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        )
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "default",
                "model",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["default_model"] == "a.x"

    def test_show_json_output_no_default(self, tmp_path: Path) -> None:
        """Test Show json output no default."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "default",
                "model",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["default_model"] is None


class TestConfigDefaultSet:
    """Tests for TestConfigDefaultSet."""

    def test_set_valid_model(self, tmp_path: Path) -> None:
        """Test Set valid model."""
        repo = _setup_repo(tmp_path)
        _write_lock(
            repo,
            (
                '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n\n'
                '[models."b.y"]\nagency = "b"\ndataset_id = "y"\nmodel_root = "sub"\n'
            ),
        )
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "model",
                "b.y",
            ],
        )
        assert result.exit_code == 0
        assert "b.y" in result.output

        # Verify lock file updated
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["default_model"] == "b.y"

    def test_set_invalid_model_errors(self, tmp_path: Path) -> None:
        """Test Set invalid model errors."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "model",
                "nonexistent.model",
            ],
        )
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_set_json_output(self, tmp_path: Path) -> None:
        """Test Set json output."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n')
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "default",
                "model",
                "a.x",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["default_model"] == "a.x"

    def test_set_preserves_models(self, tmp_path: Path) -> None:
        """Setting default_model must not lose any model data."""
        repo = _setup_repo(tmp_path)
        _write_lock(
            repo,
            (
                '[models."a.x"]\n'
                'agency = "a"\n'
                'dataset_id = "x"\n'
                'model_root = "."\n'
                'duckdb_path = "data/x.duckdb"\n'
            ),
        )
        runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "model",
                "a.x",
            ],
        )
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["default_model"] == "a.x"
        assert doc["models"]["a.x"]["agency"] == "a"
        assert doc["models"]["a.x"]["dataset_id"] == "x"


class TestConfigFolder:
    """Tests for TestConfigFolder."""

    def test_show_default_folder(self, tmp_path: Path) -> None:
        """Without any setting, models_folder defaults to 'models'."""
        repo = _setup_repo(tmp_path)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "folder",
            ],
        )
        assert result.exit_code == 0
        assert "models" in result.output

    def test_show_custom_folder(self, tmp_path: Path) -> None:
        """Test Show custom folder."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, 'models_folder = "examples"\n')
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "folder",
            ],
        )
        assert result.exit_code == 0
        assert "examples" in result.output

    def test_set_folder(self, tmp_path: Path) -> None:
        """Test Set folder."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n')
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "folder",
                "examples",
            ],
        )
        assert result.exit_code == 0
        assert "examples" in result.output
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models_folder"] == "examples"

    def test_set_folder_strips_slashes(self, tmp_path: Path) -> None:
        """Test Set folder strips slashes."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, "")
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "folder",
                "/examples/",
            ],
        )
        assert result.exit_code == 0
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models_folder"] == "examples"

    def test_show_json_output(self, tmp_path: Path) -> None:
        """Test Show json output."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, 'models_folder = "src/models"\n')
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "default",
                "folder",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["models_folder"] == "src/models"

    def test_set_json_output(self, tmp_path: Path) -> None:
        """Test Set json output."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, "")
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "default",
                "folder",
                "custom",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["models_folder"] == "custom"

    def test_set_preserves_models(self, tmp_path: Path) -> None:
        """Setting models_folder must not lose any model data."""
        repo = _setup_repo(tmp_path)
        _write_lock(
            repo,
            (
                'default_model = "a.x"\n\n'
                '[models."a.x"]\n'
                'agency = "a"\n'
                'dataset_id = "x"\n'
                'model_root = "."\n'
                'duckdb_path = "data/x.duckdb"\n'
            ),
        )
        runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "folder",
                "examples",
            ],
        )
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models_folder"] == "examples"
        assert doc["default_model"] == "a.x"
        assert doc["models"]["a.x"]["agency"] == "a"


class TestConfigRunHook:
    """Tests for TestConfigRunHook."""

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

    def test_show_current_hook(self, tmp_path: Path) -> None:
        """Test Show current hook."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_WITH_HOOK)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "hook",
            ],
        )
        assert result.exit_code == 0
        assert "sql/main.sql" in result.output

    def test_show_no_hook_set(self, tmp_path: Path) -> None:
        """Test Show no hook set."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_NO_HOOK)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "hook",
            ],
        )
        assert result.exit_code == 0
        assert "main.py" in result.output

    def test_show_json_output(self, tmp_path: Path) -> None:
        """Test Show json output."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_WITH_HOOK)
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "default",
                "hook",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["run_hook"] == "sql/main.sql"
        assert data["data"]["model"] == "a.x"

    def test_show_json_output_uses_main_py_default(self, tmp_path: Path) -> None:
        """Test Show json output uses main py default."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_NO_HOOK)
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "default",
                "hook",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["run_hook"] == "main.py"

    def test_set_hook(self, tmp_path: Path) -> None:
        """Test Set hook."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_NO_HOOK)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "default",
                "hook",
                "sql/transform.sql",
            ],
        )
        assert result.exit_code == 0
        assert "sql/transform.sql" in result.output

        # Verify persisted
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models"]["a.x"]["run_hook"] == "sql/transform.sql"

    def test_set_hook_json_output(self, tmp_path: Path) -> None:
        """Test Set hook json output."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, self._LOCK_NO_HOOK)
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "default",
                "hook",
                "run.py",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["run_hook"] == "run.py"

    def test_set_hook_normalizes_path(self, tmp_path: Path) -> None:
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

        # Set hook from repo root as if CWD is repo root
        old_cwd = os.getcwd()
        try:
            os.chdir(str(repo))
            result = runner.invoke(
                app,
                [
                    "--project",
                    str(repo),
                    "config",
                    "default",
                    "hook",
                    "models/x/sql/main.sql",
                ],
            )
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models"]["a.x"]["run_hook"] == "sql/main.sql"


# -- Lock content with schema columns for meta/attach tests ------------------

_LOCK_WITH_SCHEMA = (
    'default_model = "a.x"\n\n'
    '[models."a.x"]\n'
    'agency = "a"\n'
    'dataset_id = "x"\n'
    'model_root = "."\n'
    'duckdb_path = "data/x.duckdb"\n\n'
    '[models."a.x".schema]\n'
    'ddl = "CREATE TABLE a.x (geo VARCHAR, year INTEGER, value DOUBLE);"\n'
    'source = "local"\n\n'
    '[[models."a.x".schema.columns]]\n'
    'column_name = "geo"\n'
    "column_pos = 0\n"
    'sql_type = "VARCHAR"\n'
    'codelist_id = "geo"\n\n'
    '[[models."a.x".schema.columns]]\n'
    'column_name = "year"\n'
    "column_pos = 1\n"
    'sql_type = "INTEGER"\n'
    'codelist_id = "year"\n\n'
    '[[models."a.x".schema.columns]]\n'
    'column_name = "value"\n'
    "column_pos = 2\n"
    'sql_type = "DOUBLE"\n'
    'codelist_id = "value"\n'
)

_LOCK_NO_SCHEMA = (
    'default_model = "a.x"\n\n'
    '[models."a.x"]\n'
    'agency = "a"\n'
    'dataset_id = "x"\n'
    'model_root = "."\n'
    'duckdb_path = "data/x.duckdb"\n'
)


class TestConfigColumns:
    """Tests for TestConfigColumns."""

    def test_show_columns_with_schema(self, tmp_path: Path) -> None:
        """Test Show columns with schema."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "geo" in result.output
        assert "year" in result.output
        assert "value" in result.output

    def test_show_no_columns(self, tmp_path: Path) -> None:
        """Test Show no columns."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_NO_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
            ],
        )
        assert result.exit_code == 0
        assert "No columns defined" in result.output

    def test_show_json_output(self, tmp_path: Path) -> None:
        """Test Show json output."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert "geo" in data["data"]["columns"]
        assert data["data"]["columns"]["geo"]["codelist_id"] == "geo"

    def test_set_codelist_id(self, tmp_path: Path) -> None:
        """Test Set codelist id."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
                "set",
                "geo",
                "--id",
                "GEO_NUTS",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Updated metadata" in result.output
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        cols = doc["models"]["a.x"]["schema"]["columns"]
        geo = next(c for c in cols if c["column_name"] == "geo")
        assert geo["codelist_id"] == "GEO_NUTS"

    def test_set_kind_and_type(self, tmp_path: Path) -> None:
        """Test Set kind and type."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
                "set",
                "geo",
                "--kind",
                "hierarchical",
                "--type",
                "reference",
            ],
        )
        assert result.exit_code == 0, result.output
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        cols = doc["models"]["a.x"]["schema"]["columns"]
        geo = next(c for c in cols if c["column_name"] == "geo")
        assert geo["codelist_kind"] == "hierarchical"
        assert geo["codelist_type"] == "reference"

    def test_set_labels(self, tmp_path: Path) -> None:
        """Test Set labels."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
                "set",
                "geo",
                "--labels",
                '{"en": "Geography", "de": "Geographie"}',
            ],
        )
        assert result.exit_code == 0, result.output
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        cols = doc["models"]["a.x"]["schema"]["columns"]
        geo = next(c for c in cols if c["column_name"] == "geo")
        assert geo["codelist_labels"]["en"] == "Geography"

    def test_set_new_column(self, tmp_path: Path) -> None:
        """Setting meta on a column not in schema creates a new entry."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
                "set",
                "new_col",
                "--id",
                "NEW",
            ],
        )
        assert result.exit_code == 0, result.output

    def test_set_json_output(self, tmp_path: Path) -> None:
        """Test Set json output."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
                "set",
                "geo",
                "--id",
                "GEO",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["column"] == "geo"
        assert data["data"]["codelist_id"] == "GEO"


class TestConfigColumnsAttach:
    """Tests for TestConfigColumnsAttach."""

    def test_attach_table(self, tmp_path: Path) -> None:
        """Test Attach table."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
                "attach",
                "geo",
                "wifor.cl_nuts",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Attached" in result.output
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        cols = doc["models"]["a.x"]["schema"]["columns"]
        geo = next(c for c in cols if c["column_name"] == "geo")
        assert geo["attach_table"] == "wifor.cl_nuts"

    def test_attach_new_column(self, tmp_path: Path) -> None:
        """Attaching to a column not in schema creates a new entry."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
                "attach",
                "unknown_col",
                "ns.tbl",
            ],
        )
        assert result.exit_code == 0, result.output

    def test_attach_json_output(self, tmp_path: Path) -> None:
        """Test Attach json output."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
                "attach",
                "geo",
                "wifor.cl_nuts",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["column"] == "geo"
        assert data["data"]["table"] == "wifor.cl_nuts"

    def test_attach_requires_table_flag(self, tmp_path: Path) -> None:
        """Test Attach requires table flag."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "columns",
                "attach",
                "geo",
            ],
        )
        assert result.exit_code != 0


class TestConfigVersion:
    """Tests for TestConfigVersion."""

    def test_version_show_none(self, tmp_path: Path) -> None:
        """Test Version show none."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "version",
            ],
        )
        assert result.exit_code == 0
        assert "No version set" in result.output

    def test_version_set(self, tmp_path: Path) -> None:
        """Test Version set."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "version",
                "2026-03-16",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "2026-03-16" in result.output
        doc = tomllib.loads((repo / "dbport.lock").read_text())
        assert doc["models"]["a.x"]["version"] == "2026-03-16"

    def test_version_show_existing(self, tmp_path: Path) -> None:
        """Test Version show existing."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        # First set
        runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "version",
                "2026-03-16",
            ],
        )
        # Then show
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "version",
            ],
        )
        assert result.exit_code == 0
        assert "2026-03-16" in result.output

    def test_version_json_output(self, tmp_path: Path) -> None:
        """Test Version json output."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "version",
                "2026-03-16",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["version"] == "2026-03-16"

    def test_version_show_json_output(self, tmp_path: Path) -> None:
        """Cover JSON output for version show (line 376)."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        # First set
        runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "version",
                "2026-03-16",
            ],
        )
        # Then show in JSON mode
        result = runner.invoke(
            app,
            [
                "--json",
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "version",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["data"]["version"] == "2026-03-16"
        assert data["data"]["model"] == "a.x"


class TestGetSelectedModelKeyParentTraversal:
    """Cover _get_selected_model_key parent context traversal (lines 203-205)."""

    def test_missing_model_key_raises(self, tmp_path: Path) -> None:
        """When no config_model_key in any context, BadParameter is raised."""
        import click
        import typer

        from dbport.cli.commands.config import _get_selected_model_key

        cmd = click.Command("test")
        ctx = click.Context(cmd)
        ctx.obj = {}
        with pytest.raises(typer.BadParameter, match="Missing model key"):
            _get_selected_model_key(ctx)

    def test_parent_traversal_finds_key(self, tmp_path: Path) -> None:
        """_get_selected_model_key walks up to parent to find config_model_key."""
        import click

        from dbport.cli.commands.config import _get_selected_model_key

        cmd = click.Command("test")
        parent_ctx = click.Context(cmd)
        parent_ctx.obj = {"config_model_key": "found.model"}

        child_ctx = click.Context(cmd, parent=parent_ctx)
        child_ctx.obj = {}

        result = _get_selected_model_key(child_ctx)
        assert result == "found.model"


class TestInputShowFilterText:
    """Cover filter_text formatting in _handle_inputs_show (line 417)."""

    def test_input_show_with_filters_human(self, tmp_path: Path) -> None:
        """Test Input show with filters human."""
        repo = _setup_repo(tmp_path)
        _write_lock(
            repo,
            (
                'default_model = "a.x"\n\n'
                '[models."a.x"]\n'
                'agency = "a"\n'
                'dataset_id = "x"\n'
                'model_root = "."\n'
                'duckdb_path = "data/x.duckdb"\n\n'
                '[[models."a.x".inputs]]\n'
                'table_address = "ns.tbl"\n'
                "rows_loaded = 100\n\n"
                '[models."a.x".inputs.filters]\n'
                'wstatus = "EMP"\n'
            ),
        )
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "input",
            ],
        )
        assert result.exit_code == 0
        assert "wstatus=EMP" in result.output


class TestParseInputFiltersEmptyKey:
    """Cover empty key filter error (line 508)."""

    def test_empty_key_filter_raises(self, tmp_path: Path) -> None:
        """Test Empty key filter raises."""
        repo = _setup_repo(tmp_path)
        _write_lock(repo, _LOCK_WITH_SCHEMA)
        result = runner.invoke(
            app,
            [
                "--project",
                str(repo),
                "config",
                "model",
                "a.x",
                "input",
                "ns.tbl1",
                "--filter",
                "=value",
            ],
        )
        assert result.exit_code != 0
        assert "Key must not be empty" in result.output
