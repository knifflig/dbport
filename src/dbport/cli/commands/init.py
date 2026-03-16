"""dbp init — scaffold a new model."""

from __future__ import annotations

from pathlib import Path

import typer

from ..context import read_lock_models
from ..errors import cli_error_handler
from ..render import (
    cli_progress,
    print_error,
    print_info,
    print_json,
    print_success,
)

_SQL_TEMPLATE = """\
-- Output schema for {dataset}
-- Edit this file to define your output table columns.

CREATE OR REPLACE TABLE {agency}.{dataset} (
    -- id VARCHAR NOT NULL,
    -- value DOUBLE
);
"""

_MAIN_SQL_TEMPLATE = """\
-- Main transform for {dataset}
-- This file is executed by `dbp exec`.

-- Example:
-- INSERT INTO {agency}.{dataset}
-- SELECT * FROM staging;
"""

_RUN_PY_TEMPLATE = '''\
"""Custom model logic for {dataset}."""

from dbport import DBPort


def main() -> None:
    with DBPort(agency="{agency}", dataset_id="{dataset}") as port:
        port.schema("sql/create_output.sql")
        # port.load("namespace.table_name")
        port.execute("sql/main.sql")
        # port.publish(version="YYYY-MM-DD")


if __name__ == "__main__":
    main()
'''


def init_cmd(
    ctx: typer.Context,
    name: str | None = typer.Argument(None, help="Model name (agency.dataset or project name)."),
    template: str = typer.Option("sql", "--template", help="Template type: sql, python, or hybrid."),
    dataset: str | None = typer.Option(None, "--dataset", help="Output dataset ID."),
    agency: str | None = typer.Option(None, "--agency", help="Agency identifier."),
    path: str | None = typer.Option(None, "--path", help="Target directory (default: ./<name>)."),
    force: bool = typer.Option(False, "--force", help="Overwrite existing files."),
) -> None:
    """Scaffold a new model and register it in the lock file.

    If the model already exists in the lock file, use 'dbp sync' instead.
    """
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("init", json_output=cli_ctx.json_output):
        if name is None and not agency and not dataset and not path:
            print_error(
                "No model name specified. Usage: dbp init <name>\n"
                "To sync existing models, use: dbp sync"
            )
            raise typer.Exit(1)

        # Check if model already exists in lock file
        models = read_lock_models(cli_ctx.lockfile_path)
        model_key = _resolve_model_key(name, agency, dataset)
        if model_key and model_key in models and not force:
            print_error(
                f"Model '{model_key}' already exists in {cli_ctx.lockfile_path}. "
                "Use 'dbp sync' to update it, or --force to re-scaffold."
            )
            raise typer.Exit(1)

        _scaffold_model(cli_ctx, name or "dbport_project", template, dataset, agency, path, force)


def _resolve_model_key(
    name: str | None, agency: str | None, dataset: str | None,
) -> str | None:
    """Derive a model key from the user's arguments.

    Returns a model key string (``agency.dataset_id``) if one can be
    determined, otherwise ``None``.
    """
    if agency and dataset:
        return f"{agency}.{dataset}"
    if name:
        return name
    return None


# ---------------------------------------------------------------------------
# Scaffold
# ---------------------------------------------------------------------------

def _scaffold_model(
    cli_ctx, name: str, template: str, dataset: str | None,
    agency: str | None, path: str | None, force: bool,
) -> None:
    """Create a new model scaffold and register in the lock file."""
    from ..context import read_models_folder
    from ...infrastructure.progress import progress_callback

    project_name = name

    # Parse agency.dataset from dotted name (e.g. "test.brand_new")
    if "." in name and not agency and not dataset:
        _agency, _dataset = name.split(".", 1)
    else:
        _dataset = dataset or project_name
        _agency = agency or "default"

    if template not in ("sql", "python", "hybrid"):
        print_error(f"Unknown template: {template}. Use sql, python, or hybrid.")
        raise typer.Exit(1)

    # Determine target directory using models_folder
    models_folder = read_models_folder(cli_ctx.lockfile_path)
    repo_root = cli_ctx.project_path

    if path:
        # --path is relative to models_folder (unless absolute)
        p = Path(path)
        if p.is_absolute():
            target = p
        else:
            target = repo_root / models_folder / path
    else:
        # Default: models_folder / agency / dataset
        target = repo_root / models_folder / _agency / _dataset

    if target.exists() and not force:
        existing = list(target.iterdir()) if target.is_dir() else [target]
        if existing:
            print_error(
                f"Directory '{target}' already exists and is not empty. "
                "Use --force to overwrite."
            )
            raise typer.Exit(1)

    with cli_progress(enabled=not cli_ctx.json_output):
        cb = progress_callback.get(None)

        # Create scaffold directories
        if cb:
            cb.started(f"Scaffolding model '{project_name}'")

        target.mkdir(parents=True, exist_ok=True)
        sql_dir = target / "sql"
        sql_dir.mkdir(exist_ok=True)
        data_dir = target / "data"
        data_dir.mkdir(exist_ok=True)

        fmt = {"agency": _agency, "dataset": _dataset}

        # Write starter files only if they don't exist (never overwrite model files)
        ddl_file = sql_dir / "create_output.sql"
        if not ddl_file.exists() or force:
            ddl_file.write_text(_SQL_TEMPLATE.format(**fmt), encoding="utf-8")

        main_sql = sql_dir / "main.sql"
        if not main_sql.exists() or force:
            main_sql.write_text(_MAIN_SQL_TEMPLATE.format(**fmt), encoding="utf-8")

        if template in ("python", "hybrid"):
            run_py = target / "run.py"
            if not run_py.exists() or force:
                run_py.write_text(_RUN_PY_TEMPLATE.format(**fmt), encoding="utf-8")

        if cb:
            cb.finished()

        # Register model in the repo-root lock file
        if cb:
            cb.started("Registering model in lock file")

        repo_root = cli_ctx.project_path
        lock_path = cli_ctx.lockfile_path

        try:
            model_root_rel = str(target.relative_to(repo_root))
        except ValueError:
            model_root_rel = str(target)

        duckdb_rel = f"{model_root_rel}/data/{_dataset}.duckdb"

        _register_model(lock_path, _agency, _dataset, model_root_rel, duckdb_rel)

        # Set run_hook based on template
        if template in ("python", "hybrid"):
            run_hook = "run.py"
        else:
            run_hook = "sql/main.sql"
        _set_run_hook(lock_path, _agency, _dataset, model_root_rel, duckdb_rel, run_hook)

        # Always set the newly initialized model as the default
        from ..context import write_default_model
        model_key = f"{_agency}.{_dataset}"
        write_default_model(lock_path, model_key)

        if cb:
            cb.finished()

    if cli_ctx.json_output:
        print_json("init", {
            "name": project_name,
            "path": str(target),
            "agency": _agency,
            "dataset": _dataset,
            "template": template,
            "model_root": model_root_rel,
            "run_hook": run_hook,
        })
    else:
        print_success(f"Created model '{project_name}' at {target}")
        print_info(f"  Agency:     {_agency}")
        print_info(f"  Dataset:    {_dataset}")
        print_info(f"  Model root: {model_root_rel}")
        print_info(f"  Template:   {template}")
        print_info(f"  Run hook:   {run_hook}")
        print_info(f"  Lock file:  {lock_path}")
        print_info("")
        print_info("Next steps:")
        print_info(f"  cd {model_root_rel}")
        print_info("  dbp schema sql/create_output.sql")
        print_info("  dbp load <namespace.table>")
        print_info("  dbp run --version YYYY-MM-DD")


# ---------------------------------------------------------------------------
# Lock helpers
# ---------------------------------------------------------------------------

def _register_model(
    lock_path: Path, agency: str, dataset_id: str,
    model_root: str, duckdb_path: str,
) -> None:
    """Add a model entry to the repo-root dbport.lock."""
    from ...adapters.secondary.lock.toml import TomlLockAdapter

    model_key = f"{agency}.{dataset_id}"
    adapter = TomlLockAdapter(
        lock_path,
        model_key=model_key,
        model_root=model_root,
        duckdb_path=duckdb_path,
    )
    doc = adapter._load()
    m = adapter._model_doc(doc)
    adapter._ensure_model_header(m)
    adapter._save(doc)


def _set_run_hook(
    lock_path: Path, agency: str, dataset_id: str,
    model_root: str, duckdb_path: str, hook: str,
) -> None:
    """Set the run_hook for a model in the lock file."""
    from ...adapters.secondary.lock.toml import TomlLockAdapter

    model_key = f"{agency}.{dataset_id}"
    adapter = TomlLockAdapter(
        lock_path,
        model_key=model_key,
        model_root=model_root,
        duckdb_path=duckdb_path,
    )
    adapter.write_run_hook(hook)
