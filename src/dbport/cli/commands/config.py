"""dbp config — repo-level control plane.

Subcommands for inspecting and managing project configuration persisted
in ``dbport.lock``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import typer

from ..errors import cli_error_handler
from ..render import print_error, print_info, print_json, print_success, print_table, print_warning

config_app = typer.Typer(
    name="config",
    help="Manage project configuration.",
    no_args_is_help=True,
)


@config_app.command(name="default")
def default_cmd(
    ctx: typer.Context,
    model_key: Optional[str] = typer.Argument(None, help="Model key (agency.dataset_id) to set as default."),
) -> None:
    """Show or set the default model for this project."""
    from ..context import read_default_model, read_lock_models, write_default_model
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("config default", json_output=cli_ctx.json_output):
        if model_key is None:
            # Show current default
            current = read_default_model(cli_ctx.lockfile_path)
            if cli_ctx.json_output:
                print_json("config default", {"default_model": current})
            elif current:
                print_info(f"Default model: {current}")
            else:
                print_info("No default model set.")
        else:
            # Set default — validate model exists
            models = read_lock_models(cli_ctx.lockfile_path)
            if model_key not in models:
                available = list(models.keys()) if models else []
                print_error(
                    f"Model '{model_key}' not found in {cli_ctx.lockfile_path}. "
                    f"Available: {available}"
                )
                raise typer.Exit(1)

            write_default_model(cli_ctx.lockfile_path, model_key)

            if cli_ctx.json_output:
                print_json("config default", {"default_model": model_key})
            else:
                print_success(f"Default model set to: {model_key}")


@config_app.command(name="info")
def info_cmd(
    ctx: typer.Context,
    inputs: bool = typer.Option(False, "--inputs", help="Show input details."),
    history: bool = typer.Option(False, "--history", help="Show publish history."),
    raw: bool = typer.Option(False, "--raw", help="Show raw lock file TOML."),
) -> None:
    """Inspect persisted lock file state for the resolved model."""
    from ..context import _read_lock_doc, read_default_model, read_lock_models, _resolve_model_data
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("config info", json_output=cli_ctx.json_output):
        # --raw: dump the entire lock file as-is
        if raw:
            if not cli_ctx.lockfile_path.exists():
                print_warning("No dbport.lock found.")
                return
            content = cli_ctx.lockfile_path.read_text(encoding="utf-8")
            if cli_ctx.json_output:
                print_json("config info", {"raw": content})
            else:
                import sys
                sys.stdout.write(content)
            return

        if not cli_ctx.lockfile_path.exists():
            if cli_ctx.json_output:
                print_json("config info", {"error": "No dbport.lock found."}, ok=False)
            else:
                print_warning("No dbport.lock found. Run 'dbp init' to create a project.")
            return

        models = read_lock_models(cli_ctx.lockfile_path)
        if not models:
            if cli_ctx.json_output:
                print_json("config info", {"error": "No models in lock file."}, ok=False)
            else:
                print_warning("No models found in dbport.lock.")
            return

        # Resolve which model to inspect
        model_data = _resolve_model_data(cli_ctx, models)
        model_key = f"{model_data.get('agency', '?')}.{model_data.get('dataset_id', '?')}"
        default_model = read_default_model(cli_ctx.lockfile_path)

        model_inputs = model_data.get("inputs", [])
        model_versions = model_data.get("versions", [])
        schema = model_data.get("schema", {})

        # JSON output
        if cli_ctx.json_output:
            data: dict = {
                "default_model": default_model,
                "model_key": model_key,
                "agency": model_data.get("agency"),
                "dataset_id": model_data.get("dataset_id"),
                "model_root": model_data.get("model_root"),
                "duckdb_path": model_data.get("duckdb_path"),
                "schema_defined": bool(schema.get("ddl")),
                "schema_source": schema.get("source"),
                "column_count": len(schema.get("columns", [])),
                "input_count": len(model_inputs),
                "version_count": len(model_versions),
            }
            if inputs:
                data["inputs"] = [
                    {
                        "table_address": inp.get("table_address"),
                        "last_snapshot_id": inp.get("last_snapshot_id"),
                        "last_snapshot_timestamp_ms": inp.get("last_snapshot_timestamp_ms"),
                        "rows_loaded": inp.get("rows_loaded"),
                        "filters": inp.get("filters"),
                        "version": inp.get("version"),
                    }
                    for inp in model_inputs
                ]
            if history:
                data["versions"] = [
                    {
                        "version": v.get("version"),
                        "published_at": str(v.get("published_at", "")),
                        "iceberg_snapshot_id": v.get("iceberg_snapshot_id"),
                        "rows": v.get("rows"),
                        "completed": v.get("completed"),
                    }
                    for v in model_versions
                ]
            print_json("config info", data)
            return

        # Human-readable output — summary always shown
        print_info(f"[bold]Lock file:[/]     {cli_ctx.lockfile_path}")
        print_info(f"[bold]Default model:[/] {default_model or '[dim]not set[/]'}")
        print_info("")
        print_info(f"[bold cyan]Model:[/] {model_key}")
        print_info(f"  Agency:     {model_data.get('agency', '?')}")
        print_info(f"  Dataset:    {model_data.get('dataset_id', '?')}")
        print_info(f"  Model root: {model_data.get('model_root', '?')}")
        print_info(f"  DuckDB:     {model_data.get('duckdb_path', '?')}")

        if schema.get("ddl"):
            col_count = len(schema.get("columns", []))
            print_info(f"  Schema:     defined ({col_count} columns, source: {schema.get('source', '?')})")
        else:
            print_info("  Schema:     [dim]not defined[/]")

        print_info(f"  Inputs:     {len(model_inputs)}")
        latest = model_versions[-1].get("version", "?") if model_versions else "none"
        print_info(f"  Versions:   {len(model_versions)} (latest: {latest})")

        # --inputs: detailed input table
        if inputs and model_inputs:
            print_info("")
            rows = []
            for inp in model_inputs:
                ts = inp.get("last_snapshot_timestamp_ms")
                ts_str = ""
                if ts:
                    ts_str = datetime.fromtimestamp(ts / 1000).isoformat(timespec="seconds") if isinstance(ts, (int, float)) else str(ts)
                filters = inp.get("filters")
                filter_str = ", ".join(f"{k}={v}" for k, v in filters.items()) if filters else ""
                rows.append([
                    inp.get("table_address", "?"),
                    str(inp.get("rows_loaded", "?")),
                    str(inp.get("last_snapshot_id", ""))[:12],
                    ts_str,
                    filter_str,
                ])
            print_table(
                f"Inputs — {model_key}",
                ["Table", "Rows", "Snapshot ID", "Snapshot Time", "Filters"],
                rows,
            )

        # --history: detailed version history table
        if history and model_versions:
            print_info("")
            rows = []
            for v in model_versions:
                rows.append([
                    v.get("version", "?"),
                    str(v.get("published_at", "?")),
                    str(v.get("iceberg_snapshot_id", ""))[:12],
                    str(v.get("rows", "?")),
                    "yes" if v.get("completed") else "no",
                ])
            print_table(
                f"Version History — {model_key}",
                ["Version", "Published At", "Snapshot ID", "Rows", "Completed"],
                rows,
            )
