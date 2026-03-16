"""dbp status — show resolved project state and related diagnostics."""

from __future__ import annotations

import typer

from ..context import read_lock_models
from ..errors import cli_error_handler
from ..render import print_info, print_json, print_table, print_warning

status_app = typer.Typer(
    name="status",
    help="Show resolved project and runtime state.",
    no_args_is_help=False,
)


@status_app.callback(invoke_without_command=True)
def status_cmd(
    ctx: typer.Context,
    show_history: bool = typer.Option(
        False, "--show-history", help="Show version publish history."
    ),
) -> None:
    """Show resolved project and runtime state."""
    if ctx.invoked_subcommand is not None:
        return

    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("status", json_output=cli_ctx.json_output):
        models = read_lock_models(cli_ctx.lockfile_path)

        if cli_ctx.json_output:
            data = {
                "project_path": str(cli_ctx.project_path),
                "lockfile_path": str(cli_ctx.lockfile_path),
                "lockfile_exists": cli_ctx.lockfile_path.exists(),
                "models": {},
            }
            for key, m in models.items():
                model_data = {
                    "agency": m.get("agency"),
                    "dataset_id": m.get("dataset_id"),
                    "model_root": m.get("model_root"),
                    "schema_defined": bool(m.get("schema", {}).get("ddl")),
                    "inputs": [inp.get("table_address") for inp in m.get("inputs", [])],
                    "versions": [v.get("version") for v in m.get("versions", [])],
                }
                data["models"][key] = model_data
            print_json("status", data)
            return

        # Human-readable output
        print_info(f"[bold]Project:[/]  {cli_ctx.project_path}")
        print_info(f"[bold]Lockfile:[/] {cli_ctx.lockfile_path}")

        if not cli_ctx.lockfile_path.exists():
            print_warning("No dbport.lock found. Run 'dbp init' to create a project.")
            return

        if not models:
            print_warning("No models found in dbport.lock.")
            return

        for key, m in models.items():
            print_info("")
            print_info(f"[bold cyan]Model:[/] {key}")
            print_info(f"  Agency:     {m.get('agency', '?')}")
            print_info(f"  Dataset:    {m.get('dataset_id', '?')}")
            print_info(f"  Model root: {m.get('model_root', '?')}")
            print_info(f"  DuckDB:     {m.get('duckdb_path', '?')}")

            # Schema
            schema = m.get("schema", {})
            if schema.get("ddl"):
                col_count = len(schema.get("columns", []))
                print_info(f"  Schema:     defined ({col_count} columns)")
            else:
                print_info("  Schema:     [dim]not defined[/]")

            # Inputs
            inputs = m.get("inputs", [])
            if inputs:
                print_info(f"  Inputs:     {len(inputs)} loaded")
                for inp in inputs:
                    addr = inp.get("table_address", "?")
                    rows = inp.get("rows_loaded")
                    suffix = f" ({rows} rows)" if rows else ""
                    print_info(f"    - {addr}{suffix}")
            else:
                print_info("  Inputs:     [dim]none[/]")

            # Versions
            versions = m.get("versions", [])
            if versions:
                latest = versions[-1]
                print_info(
                    f"  Published:  {len(versions)} version(s), latest: {latest.get('version', '?')}"
                )
            else:
                print_info("  Published:  [dim]none[/]")

            # Version history
            if show_history and versions:
                print_info("")
                rows = []
                for v in versions:
                    rows.append(
                        [
                            v.get("version", "?"),
                            str(v.get("published_at", "?")),
                            str(v.get("rows", "?")),
                            "yes" if v.get("completed") else "no",
                        ]
                    )
                print_table(
                    f"Version History — {key}",
                    ["Version", "Published At", "Rows", "Completed"],
                    rows,
                )


from .check import check_cmd  # noqa: E402

status_app.command("check")(check_cmd)
