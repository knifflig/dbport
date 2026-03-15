"""dbp schema — show or apply the output schema."""

from __future__ import annotations

from typing import Optional

import typer

from ..context import resolve_dataset, resolve_model_paths
from ..errors import cli_error_handler
from ..render import print_info, print_json, print_success, print_table, print_warning


def schema_cmd(
    ctx: typer.Context,
    source: Optional[str] = typer.Argument(None, help="Path to .sql DDL file to apply."),
    diff: bool = typer.Option(False, "--diff", help="Show schema diff between lock and DuckDB."),
) -> None:
    """Show or apply the output schema contract."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("schema", json_output=cli_ctx.json_output):
        if source is not None:
            _apply_schema(cli_ctx, source)
        else:
            _show_schema(cli_ctx)


def _show_schema(cli_ctx) -> None:
    """Display the current schema from the lock file."""
    from ..context import _resolve_model_data, read_lock_models

    models = read_lock_models(cli_ctx.lockfile_path)
    if not models:
        if cli_ctx.json_output:
            print_json("schema", {"schema": None})
        else:
            print_warning("No models found in dbport.lock.")
        return

    model_data = _resolve_model_data(cli_ctx, models)
    model_key = f"{model_data.get('agency', '?')}.{model_data.get('dataset_id', '?')}"
    schema = model_data.get("schema", {})

    if cli_ctx.json_output:
        print_json("schema", {
            "model": model_key,
            "ddl": schema.get("ddl"),
            "columns": schema.get("columns", []),
        })
        return

    if not schema.get("ddl"):
        print_warning(f"No schema defined for {model_key}.")
        print_info("Apply a schema with: dbp schema sql/create_output.sql")
        return

    print_info(f"[bold]Schema for {model_key}[/]")
    print_info("")

    columns = schema.get("columns", [])
    if columns:
        rows = []
        for col in columns:
            rows.append([
                str(col.get("column_pos", "")),
                col.get("column_name", "?"),
                col.get("sql_type", "?"),
                col.get("codelist_id", ""),
            ])
        print_table("Columns", ["Pos", "Name", "Type", "Codelist"], rows)
    else:
        print_info("[dim]No columns defined.[/]")

    print_info("")
    print_info("[bold]DDL:[/]")
    print_info(schema["ddl"])


def _apply_schema(cli_ctx, source: str) -> None:
    """Apply a schema from a SQL file via DBPort client."""
    from pathlib import Path

    from ...adapters.primary.client import DBPort

    paths = resolve_model_paths(cli_ctx)

    # Pre-check: resolve the source path against model_root
    if source.strip().lower().endswith(".sql"):
        source_path = Path(source)
        if not source_path.is_absolute():
            source_path = Path(paths.model_root) / source_path
        if not source_path.exists():
            raise FileNotFoundError(f"Schema file not found: {source_path}")

    with DBPort(
        agency=paths.agency,
        dataset_id=paths.dataset_id,
        lock_path=paths.lock_path,
        duckdb_path=paths.duckdb_path,
        model_root=paths.model_root,
    ) as port:
        port.schema(source)

    if cli_ctx.json_output:
        print_json("schema", {"applied": source, "model": f"{paths.agency}.{paths.dataset_id}"})
    else:
        print_success(f"Schema applied from {source}")
