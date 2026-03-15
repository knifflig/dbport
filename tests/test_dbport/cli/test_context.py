"""Tests for CLI context resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from dbport.cli.context import (
    CliContext,
    ModelPaths,
    _cwd_model_root,
    _find_model,
    _find_repo_root,
    read_default_model,
    read_lock_versions,
    resolve_context,
    resolve_dataset,
    resolve_model_key,
    resolve_model_paths,
    resolve_model_paths_from_data,
    write_default_model,
)


class TestFindRepoRoot:
    def test_finds_pyproject_in_parent(self, tmp_path: Path):
        (tmp_path / "pyproject.toml").write_text("[project]\n")
        child = tmp_path / "sub" / "deep"
        child.mkdir(parents=True)
        assert _find_repo_root(child) == tmp_path

    def test_falls_back_to_start_when_none(self, tmp_path: Path):
        child = tmp_path / "no_marker"
        child.mkdir()
        result = _find_repo_root(child)
        assert result.exists() or result == child.resolve()


class TestResolveContext:
    def test_lockfile_always_at_repo_root(self, tmp_path: Path, monkeypatch):
        """Lock file is always at repo root, even when CWD has its own."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text("[project]\n")

        sub = repo / "examples" / "model"
        sub.mkdir(parents=True)
        # Even if CWD has a stale dbport.lock, the repo-root one is used
        (sub / "dbport.lock").write_text("[models]\n# local\n")

        monkeypatch.chdir(sub)
        ctx = resolve_context()

        assert ctx.lockfile_path == repo / "dbport.lock"

    def test_explicit_lockfile_overrides(self, tmp_path: Path, monkeypatch):
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text("[project]\n")

        explicit = tmp_path / "custom.lock"
        explicit.write_text("[models]\n")

        monkeypatch.chdir(repo)
        ctx = resolve_context(lockfile=str(explicit))

        assert ctx.lockfile_path == explicit

    def test_model_flag_stored(self, tmp_path: Path, monkeypatch):
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text("[project]\n")
        monkeypatch.chdir(repo)

        ctx = resolve_context(model="examples/minimal_cli")
        assert ctx.model_dir == "examples/minimal_cli"


class TestCwdModelRoot:
    def test_returns_relative_path(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path / "sub" if False else tmp_path)
        # When CWD == project_path, returns "."
        assert _cwd_model_root(tmp_path) == "."

    def test_returns_subdir(self, tmp_path: Path, monkeypatch):
        sub = tmp_path / "examples" / "model"
        sub.mkdir(parents=True)
        monkeypatch.chdir(sub)
        assert _cwd_model_root(tmp_path) == "examples/model"


class TestFindModel:
    def test_matches_by_model_root(self):
        models = {
            "a.x": {"model_root": "models/x", "agency": "a", "dataset_id": "x"},
            "b.y": {"model_root": "models/y", "agency": "b", "dataset_id": "y"},
        }
        result = _find_model(models, "models/y")
        assert result is not None
        assert result[0] == "b.y"

    def test_returns_none_when_not_found(self):
        models = {"a.x": {"model_root": "models/x"}}
        assert _find_model(models, "nonexistent") is None


class TestResolveModelPaths:
    def test_resolves_relative_paths_from_repo_root(self, tmp_path: Path):
        """model_root and duckdb_path in lock are relative to repo root."""
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."test.t"]\n'
            'agency = "test"\n'
            'dataset_id = "t"\n'
            'model_root = "examples/model"\n'
            'duckdb_path = "examples/model/data/t.duckdb"\n'
        )
        ctx = CliContext(
            project_path=tmp_path,
            lockfile_path=lock,
        )
        paths = resolve_model_paths(ctx)

        assert paths.agency == "test"
        assert paths.dataset_id == "t"
        assert paths.duckdb_path == str((tmp_path / "examples/model/data/t.duckdb").resolve())
        assert paths.model_root == str((tmp_path / "examples/model").resolve())

    def test_defaults_duckdb_path_when_empty(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.b"]\n'
            'agency = "a"\n'
            'dataset_id = "b"\n'
            'model_root = "sub"\n'
        )
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        paths = resolve_model_paths(ctx)

        assert paths.duckdb_path == str((tmp_path / "sub" / "data" / "b.duckdb").resolve())

    def test_resolves_model_by_flag(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "models/x"\n'
            'duckdb_path = "models/x/data/x.duckdb"\n'
            '\n'
            '[models."b.y"]\n'
            'agency = "b"\n'
            'dataset_id = "y"\n'
            'model_root = "models/y"\n'
            'duckdb_path = "models/y/data/y.duckdb"\n'
        )
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock, model_dir="models/y")
        paths = resolve_model_paths(ctx)

        assert paths.agency == "b"
        assert paths.dataset_id == "y"

    def test_resolves_model_by_cwd(self, tmp_path: Path, monkeypatch):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "models/x"\n'
            '\n'
            '[models."b.y"]\n'
            'agency = "b"\n'
            'dataset_id = "y"\n'
            'model_root = "models/y"\n'
        )
        (tmp_path / "models" / "y").mkdir(parents=True)
        monkeypatch.chdir(tmp_path / "models" / "y")

        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        paths = resolve_model_paths(ctx)

        assert paths.agency == "b"
        assert paths.dataset_id == "y"

    def test_falls_back_to_first_model(self, tmp_path: Path, monkeypatch):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "models/x"\n'
        )
        monkeypatch.chdir(tmp_path)  # CWD is repo root, no match

        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        paths = resolve_model_paths(ctx)

        assert paths.agency == "a"

    def test_raises_when_no_models(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)

        with pytest.raises(RuntimeError, match="No models found"):
            resolve_model_paths(ctx)

    def test_raises_when_model_flag_not_found(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "models/x"\n'
        )
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock, model_dir="nonexistent")

        with pytest.raises(RuntimeError, match="No model with model_root"):
            resolve_model_paths(ctx)

    def test_handles_absolute_duckdb_path_in_lock(self, tmp_path: Path):
        """Absolute duckdb_path in lock (legacy) should still work."""
        abs_path = str(tmp_path / "data" / "t.duckdb")
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.t"]\n'
            'agency = "a"\n'
            'dataset_id = "t"\n'
            'model_root = "."\n'
            f'duckdb_path = "{abs_path}"\n'
        )
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        paths = resolve_model_paths(ctx)

        assert paths.duckdb_path == abs_path

    def test_resolves_default_model_over_first(self, tmp_path: Path, monkeypatch):
        """When no --model and no CWD match, default_model wins over first."""
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            'default_model = "b.y"\n\n'
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "models/x"\n'
            '\n'
            '[models."b.y"]\n'
            'agency = "b"\n'
            'dataset_id = "y"\n'
            'model_root = "models/y"\n'
        )
        monkeypatch.chdir(tmp_path)  # CWD is repo root, no model_root match
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        paths = resolve_model_paths(ctx)

        assert paths.agency == "b"
        assert paths.dataset_id == "y"

    def test_model_flag_overrides_default_model(self, tmp_path: Path):
        """Explicit --model flag should still override default_model."""
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            'default_model = "b.y"\n\n'
            '[models."a.x"]\n'
            'agency = "a"\n'
            'dataset_id = "x"\n'
            'model_root = "models/x"\n'
            '\n'
            '[models."b.y"]\n'
            'agency = "b"\n'
            'dataset_id = "y"\n'
            'model_root = "models/y"\n'
        )
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock, model_dir="models/x")
        paths = resolve_model_paths(ctx)

        assert paths.agency == "a"
        assert paths.dataset_id == "x"


class TestDefaultModelReadWrite:
    def test_read_returns_none_when_no_file(self, tmp_path: Path):
        assert read_default_model(tmp_path / "missing.lock") is None

    def test_read_returns_none_when_no_key(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text('[models."a.x"]\nagency = "a"\n')
        assert read_default_model(lock) is None

    def test_read_returns_value(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text('default_model = "a.x"\n')
        assert read_default_model(lock) == "a.x"

    def test_write_creates_key(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n'
        )
        write_default_model(lock, "a.x")
        assert read_default_model(lock) == "a.x"

    def test_write_preserves_models(self, tmp_path: Path):
        import tomllib
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n'
        )
        write_default_model(lock, "a.x")
        doc = tomllib.loads(lock.read_text())
        assert doc["models"]["a.x"]["agency"] == "a"


class TestResolveDataset:
    def test_returns_agency_and_dataset(self, tmp_path: Path, monkeypatch):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n'
        )
        monkeypatch.chdir(tmp_path)
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        agency, dataset_id = resolve_dataset(ctx)
        assert agency == "a"
        assert dataset_id == "x"

    def test_raises_when_no_models(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        with pytest.raises(RuntimeError, match="No models found"):
            resolve_dataset(ctx)

    def test_raises_when_no_lock_file(self, tmp_path: Path):
        ctx = CliContext(project_path=tmp_path, lockfile_path=tmp_path / "nope.lock")
        with pytest.raises(RuntimeError, match="No models found"):
            resolve_dataset(ctx)


class TestResolveModelKey:
    def test_explicit_model_key(self, tmp_path: Path, monkeypatch):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n'
            '\n[models."b.y"]\nagency = "b"\ndataset_id = "y"\nmodel_root = "m/y"\n'
        )
        monkeypatch.chdir(tmp_path)
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        key, data = resolve_model_key(ctx, "b.y")
        assert key == "b.y"
        assert data["agency"] == "b"

    def test_falls_back_to_default_resolution(self, tmp_path: Path, monkeypatch):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            'default_model = "b.y"\n'
            '\n[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "m/x"\n'
            '\n[models."b.y"]\nagency = "b"\ndataset_id = "y"\nmodel_root = "m/y"\n'
        )
        monkeypatch.chdir(tmp_path)
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        key, data = resolve_model_key(ctx)
        assert key == "b.y"

    def test_unknown_model_arg_falls_through(self, tmp_path: Path, monkeypatch):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n'
        )
        monkeypatch.chdir(tmp_path)
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        # "nonexistent" not in models, falls through to default resolution
        key, data = resolve_model_key(ctx, "nonexistent")
        assert key == "a.x"

    def test_raises_when_no_models(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        with pytest.raises(RuntimeError, match="No models found"):
            resolve_model_key(ctx)


class TestResolveModelPathsFromData:
    def test_builds_paths_from_model_data(self, tmp_path: Path):
        ctx = CliContext(project_path=tmp_path, lockfile_path=tmp_path / "dbport.lock")
        model_data = {
            "agency": "test",
            "dataset_id": "tbl",
            "model_root": "examples/m",
            "duckdb_path": "examples/m/data/tbl.duckdb",
        }
        paths = resolve_model_paths_from_data(ctx, model_data)
        assert paths.agency == "test"
        assert paths.dataset_id == "tbl"
        assert paths.model_root == str((tmp_path / "examples/m").resolve())
        assert paths.duckdb_path == str((tmp_path / "examples/m/data/tbl.duckdb").resolve())

    def test_defaults_duckdb_path(self, tmp_path: Path):
        ctx = CliContext(project_path=tmp_path, lockfile_path=tmp_path / "dbport.lock")
        model_data = {"agency": "a", "dataset_id": "b", "model_root": "sub"}
        paths = resolve_model_paths_from_data(ctx, model_data)
        assert paths.duckdb_path == str((tmp_path / "sub" / "data" / "b.duckdb").resolve())


class TestReadLockVersions:
    def test_returns_versions_list(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text(
            '[models."a.x"]\nagency = "a"\ndataset_id = "x"\nmodel_root = "."\n'
            '\n[[models."a.x".versions]]\nversion = "2026-03-15"\ncompleted = true\n'
            '\n[[models."a.x".versions]]\nversion = "2026-03-14"\ncompleted = true\n'
        )
        versions = read_lock_versions(lock, "a.x")
        assert len(versions) == 2
        assert versions[0]["version"] == "2026-03-15"

    def test_returns_empty_for_missing_model(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text('[models."a.x"]\nagency = "a"\n')
        assert read_lock_versions(lock, "b.y") == []

    def test_returns_empty_for_no_versions(self, tmp_path: Path):
        lock = tmp_path / "dbport.lock"
        lock.write_text('[models."a.x"]\nagency = "a"\ndataset_id = "x"\n')
        assert read_lock_versions(lock, "a.x") == []

    def test_returns_empty_for_missing_file(self, tmp_path: Path):
        assert read_lock_versions(tmp_path / "nope.lock", "a.x") == []
