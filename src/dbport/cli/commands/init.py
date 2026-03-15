"""dbp init — create a new DBPort model scaffold."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from ..errors import cli_error_handler
from ..render import print_error, print_info, print_json, print_success


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
-- This file is executed by `dbp run`.

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
    name: Optional[str] = typer.Argument(None, help="Project/model name."),
    template: str = typer.Option("sql", "--template", help="Template type: sql, python, or hybrid."),
    dataset: Optional[str] = typer.Option(None, "--dataset", help="Output dataset ID."),
    agency: Optional[str] = typer.Option(None, "--agency", help="Agency identifier."),
    path: Optional[str] = typer.Option(None, "--path", help="Target directory (default: ./<name>)."),
    force: bool = typer.Option(False, "--force", help="Overwrite existing files."),
) -> None:
    """Create a new DBPort model scaffold and register it in the repo-root lock file."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("init", json_output=cli_ctx.json_output):
        project_name = name or "dbport_project"
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

        # Create scaffold
        target.mkdir(parents=True, exist_ok=True)
        sql_dir = target / "sql"
        sql_dir.mkdir(exist_ok=True)
        data_dir = target / "data"
        data_dir.mkdir(exist_ok=True)

        fmt = {"agency": _agency, "dataset": _dataset}

        # Write starter SQL files
        (sql_dir / "create_output.sql").write_text(
            _SQL_TEMPLATE.format(**fmt), encoding="utf-8"
        )
        (sql_dir / "main.sql").write_text(
            _MAIN_SQL_TEMPLATE.format(**fmt), encoding="utf-8"
        )

        # Write run.py for hybrid/python templates
        if template in ("python", "hybrid"):
            (target / "run.py").write_text(
                _RUN_PY_TEMPLATE.format(**fmt), encoding="utf-8"
            )

        # Register model in the repo-root lock file (not in the model dir)
        repo_root = cli_ctx.project_path
        lock_path = cli_ctx.lockfile_path

        # model_root and duckdb_path are relative to the repo root
        try:
            model_root_rel = str(target.relative_to(repo_root))
        except ValueError:
            model_root_rel = str(target)

        duckdb_rel = f"{model_root_rel}/data/{_dataset}.duckdb"

        _register_model(lock_path, _agency, _dataset, model_root_rel, duckdb_rel)

        # Always set the newly initialized model as the default
        from ..context import write_default_model
        model_key = f"{_agency}.{_dataset}"
        write_default_model(lock_path, model_key)

        if cli_ctx.json_output:
            print_json("init", {
                "name": project_name,
                "path": str(target),
                "agency": _agency,
                "dataset": _dataset,
                "template": template,
                "model_root": model_root_rel,
            })
        else:
            print_success(f"Created model '{project_name}' at {target}")
            print_info(f"  Agency:     {_agency}")
            print_info(f"  Dataset:    {_dataset}")
            print_info(f"  Model root: {model_root_rel}")
            print_info(f"  Template:   {template}")
            print_info(f"  Lock file:  {lock_path}")
            print_info("")
            print_info("Next steps:")
            print_info(f"  cd {model_root_rel}")
            print_info("  dbp schema sql/create_output.sql")
            print_info("  dbp load <namespace.table>")
            print_info("  dbp run sql/main.sql")


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
    # Trigger a read+write cycle to create/update the model entry
    doc = adapter._load()
    m = adapter._model_doc(doc)
    adapter._ensure_model_header(m)
    adapter._save(doc)
