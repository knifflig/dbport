"""CLI runtime context — resolved paths, output mode, dataset identity.

The lock file always lives at the repo root (next to ``pyproject.toml``).
Each model is identified by its ``model_root`` — a path relative to the
repo root.  The CLI resolves which model to operate on by matching CWD
(relative to repo root) against ``model_root`` entries in the lock, or
via the ``--model`` flag.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path


def _find_repo_root(start: Path) -> Path:
    """Walk up from *start* until a directory containing pyproject.toml is found."""
    current = start.resolve()
    while True:
        if (current / "pyproject.toml").exists():
            return current
        parent = current.parent
        if parent == current:
            return start.resolve()
        current = parent


@dataclass
class CliContext:
    """Resolved runtime context shared across all CLI commands."""

    project_path: Path
    lockfile_path: Path
    model_dir: str | None = None
    verbose: bool = False
    quiet: bool = False
    json_output: bool = False
    no_color: bool = False


def resolve_context(
    *,
    project: str | None = None,
    lockfile: str | None = None,
    model: str | None = None,
    verbose: bool = False,
    quiet: bool = False,
    json_output: bool = False,
    no_color: bool = False,
) -> CliContext:
    """Build a CliContext from CLI flags and filesystem discovery.

    The lock file is always ``<project_path>/dbport.lock`` — one lock file
    per repo, next to ``pyproject.toml``.
    """
    if project:
        project_path = Path(project).resolve()
    else:
        project_path = _find_repo_root(Path.cwd())

    if lockfile:
        lockfile_path = Path(lockfile).resolve()
    else:
        lockfile_path = project_path / "dbport.lock"

    return CliContext(
        project_path=project_path,
        lockfile_path=lockfile_path,
        model_dir=model,
        verbose=verbose,
        quiet=quiet,
        json_output=json_output,
        no_color=no_color,
    )


def _read_lock_doc(lockfile_path: Path) -> dict:
    """Read the full lock document. Returns {} if file missing."""
    if not lockfile_path.exists():
        return {}
    return tomllib.loads(lockfile_path.read_text(encoding="utf-8"))


def read_lock_models(lockfile_path: Path) -> dict:
    """Read the models dict from dbport.lock. Returns {} if file missing."""
    return _read_lock_doc(lockfile_path).get("models", {})


def read_default_model(lockfile_path: Path) -> str | None:
    """Read the default_model key from dbport.lock. Returns None if unset."""
    return _read_lock_doc(lockfile_path).get("default_model")


def write_default_model(lockfile_path: Path, model_key: str) -> None:
    """Set the default_model key in dbport.lock."""
    from ..adapters.secondary.lock.toml import TomlLockAdapter

    adapter = TomlLockAdapter(lockfile_path)
    doc = adapter._load()
    doc["default_model"] = model_key
    adapter._save(doc)


def _cwd_model_root(project_path: Path) -> str:
    """Compute the model_root for CWD relative to the project root.

    Returns ``"."`` if CWD is the project root itself.
    """
    cwd = Path.cwd().resolve()
    try:
        rel = cwd.relative_to(project_path)
        return str(rel) if str(rel) != "." else "."
    except ValueError:
        return "."


def _find_model(models: dict, model_root: str) -> tuple[str, dict] | None:
    """Find a model in the lock whose model_root matches *model_root*."""
    for key, data in models.items():
        if data.get("model_root", ".") == model_root:
            return key, data
    return None


def resolve_dataset(ctx: CliContext) -> tuple[str, str]:
    """Read agency and dataset_id from the matching model in dbport.lock.

    Model resolution order:
    1. ``--model`` flag (explicit model directory relative to project root)
    2. CWD relative to project root (match against model_root in lock)
    3. First model in lock (fallback for single-model repos)

    Raises RuntimeError if no model is found.
    """
    models = read_lock_models(ctx.lockfile_path)
    if not models:
        raise RuntimeError(
            f"No models found in {ctx.lockfile_path}. "
            "Run 'dbp init' to create a project first."
        )

    model_data = _resolve_model_data(ctx, models)
    return model_data["agency"], model_data["dataset_id"]


def _resolve_model_data(ctx: CliContext, models: dict) -> dict:
    """Find the matching model data from the lock file."""
    # 1. Explicit --model flag
    if ctx.model_dir is not None:
        result = _find_model(models, ctx.model_dir)
        if result is None:
            raise RuntimeError(
                f"No model with model_root='{ctx.model_dir}' in {ctx.lockfile_path}. "
                f"Available: {[d.get('model_root', '.') for d in models.values()]}"
            )
        return result[1]

    # 2. Match CWD against model_root
    cwd_root = _cwd_model_root(ctx.project_path)
    result = _find_model(models, cwd_root)
    if result is not None:
        return result[1]

    # 3. default_model from lock file
    default_key = read_default_model(ctx.lockfile_path)
    if default_key and default_key in models:
        return models[default_key]

    # 4. Fallback: first model (single-model repos or running from repo root)
    return next(iter(models.values()))


@dataclass
class ModelPaths:
    """Resolved paths for constructing a DBPort instance from the CLI."""

    agency: str
    dataset_id: str
    lock_path: str
    duckdb_path: str
    model_root: str


def resolve_model_paths(ctx: CliContext) -> ModelPaths:
    """Resolve agency, dataset_id, and absolute paths from the lock file.

    The lock file stores ``model_root`` and ``duckdb_path`` as paths relative
    to the repo root (``project_path``).  This function resolves them to
    absolute paths so the CLI can pass them to ``DBPort()``.
    """
    models = read_lock_models(ctx.lockfile_path)
    if not models:
        raise RuntimeError(
            f"No models found in {ctx.lockfile_path}. "
            "Run 'dbp init' to create a project first."
        )

    model_data = _resolve_model_data(ctx, models)
    return resolve_model_paths_from_data(ctx, model_data)


def resolve_model_paths_from_data(ctx: CliContext, model_data: dict) -> ModelPaths:
    """Build :class:`ModelPaths` from already-resolved *model_data*."""
    repo_root = ctx.project_path

    # model_root is relative to the repo root
    raw_root = model_data.get("model_root", ".")
    model_root = (repo_root / raw_root).resolve()

    # duckdb_path: resolve relative to repo root, or default
    raw_db = model_data.get("duckdb_path", "")
    if raw_db:
        db_path = Path(raw_db)
        if not db_path.is_absolute():
            db_path = repo_root / db_path
    else:
        db_path = model_root / "data" / f"{model_data['dataset_id']}.duckdb"
    duckdb_path = db_path.resolve()

    return ModelPaths(
        agency=model_data["agency"],
        dataset_id=model_data["dataset_id"],
        lock_path=str(ctx.lockfile_path),
        duckdb_path=str(duckdb_path),
        model_root=str(model_root),
    )


def resolve_model_key(ctx: CliContext, model_arg: str | None = None) -> tuple[str, dict]:
    """Determine the model key and data from an explicit arg or default resolution.

    Returns ``(model_key, model_data)`` tuple.
    """
    models = read_lock_models(ctx.lockfile_path)
    if not models:
        raise RuntimeError(
            f"No models found in {ctx.lockfile_path}. "
            "Run 'dbp init' to create a project first."
        )

    # 1. Explicit model key passed as positional arg
    if model_arg and model_arg in models:
        return model_arg, models[model_arg]

    # 2. Fall through to standard resolution
    model_data = _resolve_model_data(ctx, models)
    # Find the key that corresponds to this model_data
    for key, data in models.items():
        if data is model_data:
            return key, data
    # Shouldn't happen, but safe fallback
    return next(iter(models.items()))


def read_lock_versions(lockfile_path: Path, model_key: str) -> list[dict]:
    """Read the versions list for a specific model from the lock file."""
    models = read_lock_models(lockfile_path)
    if model_key not in models:
        return []
    return models[model_key].get("versions", [])
