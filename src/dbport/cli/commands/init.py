"""dbp init — create a new model scaffold or sync existing models."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from ..errors import cli_error_handler
from ..render import cli_progress, print_error, print_info, print_json, print_success


_SQL_TEMPLATE = """\
-- Output schema for {dataset}
-- Edit this file to define your output table columns.

CREATE OR REPLACE TABLE {agency}.{dataset} (
    -- id VARCHAR NOT NULL,
    -- value DOUBLE,
);
"""

_MAIN_SQL_TEMPLATE = """\
-- Main transform for {dataset}
-- This file is executed by `dbp execute`.

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
    name: Optional[str] = typer.Argument(None, help="Project/model name. Omit to sync existing models."),
    template: str = typer.Option("sql", "--template", help="Template type: sql, python, or hybrid."),
    dataset: Optional[str] = typer.Option(None, "--dataset", help="Output dataset ID."),
    agency: Optional[str] = typer.Option(None, "--agency", help="Agency identifier."),
    path: Optional[str] = typer.Option(None, "--path", help="Target directory (default: ./<name>)."),
    force: bool = typer.Option(False, "--force", help="Overwrite existing files."),
) -> None:
    """Create a new model scaffold or sync existing models with the warehouse.

    With NAME: creates a new model scaffold and registers it in the lock file.
    Without NAME: syncs all models in the lock file against the warehouse,
    preparing DuckDB files for execution.
    """
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("init", json_output=cli_ctx.json_output):
        # Scaffold mode: name provided, or scaffold-specific options used
        is_scaffold = name or path or dataset or agency
        if is_scaffold:
            _scaffold_model(cli_ctx, name or "dbport_project", template, dataset, agency, path, force)
        else:
            _sync_models(cli_ctx)


def _scaffold_model(
    cli_ctx, name: str, template: str, dataset: str | None,
    agency: str | None, path: str | None, force: bool,
) -> None:
    """Create a new model scaffold and register in the lock file."""
    from ...infrastructure.progress import progress_callback

    project_name = name
    _dataset = dataset or project_name
    _agency = agency or "default"

    if template not in ("sql", "python", "hybrid"):
        print_error(f"Unknown template: {template}. Use sql, python, or hybrid.")
        raise typer.Exit(1)

    # Determine target directory
    if path:
        target = Path(path).resolve()
    else:
        target = Path.cwd() / project_name

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


def _sync_models(cli_ctx) -> None:
    """Sync all models in the lock file with the warehouse."""
    from ..context import read_lock_models

    models = read_lock_models(cli_ctx.lockfile_path)
    if not models:
        print_error(
            f"No models found in {cli_ctx.lockfile_path}. "
            "Run 'dbp init <name>' to create a model first."
        )
        raise typer.Exit(1)

    synced = []
    with cli_progress(enabled=not cli_ctx.json_output):
        for model_key, model_data in models.items():
            _agency = model_data.get("agency", "default")
            _dataset_id = model_data.get("dataset_id", model_key)
            raw_root = model_data.get("model_root", ".")
            model_root = str((cli_ctx.project_path / raw_root).resolve())

            raw_db = model_data.get("duckdb_path", "")
            if raw_db:
                db_path = Path(raw_db)
                if not db_path.is_absolute():
                    db_path = cli_ctx.project_path / db_path
            else:
                db_path = Path(model_root) / "data" / f"{_dataset_id}.duckdb"
            duckdb_path = str(db_path.resolve())

            try:
                from ...adapters.primary.client import DBPort

                with DBPort(
                    agency=_agency,
                    dataset_id=_dataset_id,
                    lock_path=str(cli_ctx.lockfile_path),
                    duckdb_path=duckdb_path,
                    model_root=model_root,
                ):
                    pass  # init + sync happens in __init__
                synced.append(model_key)
            except Exception as exc:
                if not cli_ctx.json_output:
                    print_error(f"Failed to sync {model_key}: {exc}")

    if cli_ctx.json_output:
        print_json("init", {"synced": synced, "total": len(models)})
    else:
        print_success(f"Synced {len(synced)}/{len(models)} model(s)")
        for key in synced:
            print_info(f"  {key}")


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
