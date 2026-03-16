"""dbp status — show resolved project and runtime state."""

from __future__ import annotations

from datetime import datetime

import typer

from ..context import _resolve_model_data, read_default_model, read_lock_models
from ..errors import cli_error_handler
from ..render import print_info, print_json, print_table, print_warning


def status_cmd(
    ctx: typer.Context,
    inputs: bool = typer.Option(False, "--inputs", help="Show detailed input table."),
    history: bool = typer.Option(False, "--history", help="Show version publish history."),
    raw: bool = typer.Option(False, "--raw", help="Show raw lock file TOML."),
) -> None:
    """Show resolved project and runtime state."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("status", json_output=cli_ctx.json_output):
        # --raw: dump the entire lock file as-is
        if raw:
            if not cli_ctx.lockfile_path.exists():
                print_warning("No dbport.lock found.")
                return
            content = cli_ctx.lockfile_path.read_text(encoding="utf-8")
            if cli_ctx.json_output:
                print_json("status", {"raw": content})
            else:
                import sys
                sys.stdout.write(content)
            return

        if not cli_ctx.lockfile_path.exists():
            if cli_ctx.json_output:
                print_json("status", {"error": "No dbport.lock found."}, ok=False)
            else:
                print_warning("No dbport.lock found. Run 'dbp init' to create a project.")
            return

        models = read_lock_models(cli_ctx.lockfile_path)

        if not models:
            if cli_ctx.json_output:
                print_json("status", {"error": "No models in lock file."}, ok=False)
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
                "project_path": str(cli_ctx.project_path),
                "lockfile_path": str(cli_ctx.lockfile_path),
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
            print_json("status", data)
            return

        # Human-readable output
        print_info(f"[bold]Project:[/]       {cli_ctx.project_path}")
        print_info(f"[bold]Lockfile:[/]      {cli_ctx.lockfile_path}")
        print_info(f"[bold]Default model:[/] {default_model or '[dim]not set[/]'}")
        print_info("")
        print_info(f"[bold cyan]Model:[/] {model_key}")
        print_info(f"  Agency:     {model_data.get('agency', '?')}")
        print_info(f"  Dataset:    {model_data.get('dataset_id', '?')}")
        print_info(f"  Model root: {model_data.get('model_root', '?')}")
        print_info(f"  DuckDB:     {model_data.get('duckdb_path', '?')}")

        if schema.get("ddl"):
            col_count = len(schema.get("columns", []))
            source = schema.get("source")
            source_str = f", source: {source}" if source else ""
            print_info(f"  Schema:     defined ({col_count} columns{source_str})")
        else:
            print_info("  Schema:     [dim]not defined[/]")

        # Inputs summary
        if model_inputs:
            print_info(f"  Inputs:     {len(model_inputs)} loaded")
            if not inputs:
                for inp in model_inputs:
                    addr = inp.get("table_address", "?")
                    rows = inp.get("rows_loaded")
                    suffix = f" ({rows} rows)" if rows else ""
                    print_info(f"    - {addr}{suffix}")
        else:
            print_info("  Inputs:     [dim]none[/]")

        # Versions summary
        latest = model_versions[-1].get("version", "?") if model_versions else "none"
        print_info(f"  Published:  {len(model_versions)} version(s), latest: {latest}")

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
