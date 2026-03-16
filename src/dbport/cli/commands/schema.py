"""dbp config model <model> schema — show or apply the output schema."""

from __future__ import annotations

import typer

from ..context import read_lock_models, resolve_model_paths, resolve_model_paths_from_data
from ..errors import cli_error_handler
from ..render import print_info, print_json, print_success, print_table, print_warning


def schema_cmd(
    ctx: typer.Context,
    source: str | None = typer.Argument(None, help="Path to .sql DDL file to apply."),
    diff: bool = typer.Option(False, "--diff", help="Show schema diff between lock and DuckDB."),
) -> None:
    """Show or apply the output schema contract."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    command_name = "config model schema"

    with cli_error_handler(command_name, json_output=cli_ctx.json_output):
        if source is not None:
            _apply_schema(ctx, cli_ctx, source)
        else:
            _show_schema(ctx, cli_ctx)


def _selected_model_key(ctx: typer.Context) -> str | None:
    current = ctx
    while current is not None:
        if current.obj and "config_model_key" in current.obj:
            return current.obj["config_model_key"]
        current = current.parent
    return None


def _resolve_schema_target(ctx: typer.Context, cli_ctx) -> tuple[str, dict]:
    """Resolve the model key and data for schema commands.

    When invoked via ``dbp config model <model_key> schema``, prefer the explicit
    model key from the Typer context. Otherwise, fall back to standard CLI model
    resolution.
    """
    from ..context import _resolve_model_data

    models = read_lock_models(cli_ctx.lockfile_path)
    if not models:
        raise RuntimeError(
            f"No models found in {cli_ctx.lockfile_path}. Run 'dbp init' to create a project first."
        )

    explicit_model_key = _selected_model_key(ctx)
    if explicit_model_key is not None:
        if explicit_model_key not in models:
            raise RuntimeError(
                f"Model '{explicit_model_key}' not found in {cli_ctx.lockfile_path}. "
                f"Available: {list(models.keys())}"
            )
        return explicit_model_key, models[explicit_model_key]

    model_data = _resolve_model_data(cli_ctx, models)
    model_key = f"{model_data.get('agency', '?')}.{model_data.get('dataset_id', '?')}"
    return model_key, model_data


def _show_schema(ctx: typer.Context, cli_ctx) -> None:
    """Display the current schema from the lock file."""
    try:
        model_key, model_data = _resolve_schema_target(ctx, cli_ctx)
    except RuntimeError as exc:
        if cli_ctx.json_output:
            print_json("config model schema", {"schema": None, "error": str(exc)}, ok=False)
        else:
            print_warning(str(exc))
        return

    schema = model_data.get("schema", {})

    if cli_ctx.json_output:
        print_json(
            "config model schema",
            {
                "model": model_key,
                "ddl": schema.get("ddl"),
                "columns": schema.get("columns", []),
            },
        )
        return

    if not schema.get("ddl"):
        print_warning(f"No schema defined for {model_key}.")
        print_info(
            f"Apply a schema with: dbp config model {model_key} schema sql/create_output.sql"
        )
        return

    print_info(f"[bold]Schema for {model_key}[/]")
    print_info("")

    columns = schema.get("columns", [])
    if columns:
        rows = []
        for col in columns:
            rows.append(
                [
                    str(col.get("column_pos", "")),
                    col.get("column_name", "?"),
                    col.get("sql_type", "?"),
                    col.get("codelist_id", ""),
                ]
            )
        print_table("Columns", ["Pos", "Name", "Type", "Codelist"], rows)
    else:
        print_info("[dim]No columns defined.[/]")

    print_info("")
    print_info("[bold]DDL:[/]")
    print_info(schema["ddl"])


def _apply_schema(ctx: typer.Context, cli_ctx, source: str) -> None:
    """Apply a schema from a SQL file via DBPort client."""
    from pathlib import Path

    from ...adapters.primary.client import DBPort

    _, model_data = _resolve_schema_target(ctx, cli_ctx)
    paths = resolve_model_paths_from_data(cli_ctx, model_data)

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
        print_json(
            "config model schema",
            {"applied": source, "model": f"{paths.agency}.{paths.dataset_id}"},
        )
    else:
        print_success(f"Schema applied from {source}")
