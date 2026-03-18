"""dbp status — show resolved project state and related diagnostics."""

from __future__ import annotations

from typing import TYPE_CHECKING

import typer

from ..context import read_default_model, read_lock_models

if TYPE_CHECKING:
    from ..context import CliContext
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
    show_inputs: bool = typer.Option(False, "--inputs", help="Show detailed input information."),
    show_history: bool = typer.Option(False, "--history", help="Show version publish history."),
    show_raw: bool = typer.Option(False, "--raw", help="Show raw lock file content."),
) -> None:
    """Show resolved project and runtime state."""
    if ctx.invoked_subcommand is not None:
        return

    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("status", json_output=cli_ctx.json_output):
        # --raw: dump raw lock file content
        if show_raw:
            _handle_raw(cli_ctx)
            return

        if not cli_ctx.lockfile_path.exists():
            if cli_ctx.json_output:
                print_json("status", {"error": "No dbport.lock found."}, ok=False)
                return
            print_warning("No dbport.lock found. Run 'dbp init' to create a project.")
            return

        models = read_lock_models(cli_ctx.lockfile_path)

        if not models:
            if cli_ctx.json_output:
                print_json("status", {"error": "No models found in dbport.lock."}, ok=False)
                return
            print_warning("No models found in dbport.lock.")
            return

        if cli_ctx.json_output:
            _handle_json(cli_ctx, models, show_inputs=show_inputs, show_history=show_history)
            return

        # Human-readable output
        print_info(f"[bold]Project:[/]  {cli_ctx.project_path}")
        print_info(f"[bold]Lockfile:[/] {cli_ctx.lockfile_path}")

        for key, m in models.items():
            _print_model_summary(key, m, show_inputs=show_inputs, show_history=show_history)


def _handle_raw(cli_ctx: CliContext) -> None:
    """Show raw lock file content."""
    if not cli_ctx.lockfile_path.exists():
        if cli_ctx.json_output:
            print_json("status", {"error": "No dbport.lock found."}, ok=False)
            return
        print_warning("No dbport.lock found.")
        return
    raw = cli_ctx.lockfile_path.read_text(encoding="utf-8")
    if cli_ctx.json_output:
        print_json("status", {"raw": raw})
    else:
        from rich import print as rprint
        from rich.text import Text

        rprint(Text(raw))


def _handle_json(
    cli_ctx: CliContext, models: dict, *, show_inputs: bool, show_history: bool
) -> None:
    """Produce JSON output for the resolved model."""
    from ..context import _cwd_model_root, _find_model

    # Resolve which model to display
    default_key = read_default_model(cli_ctx.lockfile_path)

    # Model resolution: --model flag, CWD match, default_model, first model
    model_key: str | None = None
    if cli_ctx.model_dir is not None:
        result = _find_model(models, cli_ctx.model_dir)
        if result:
            model_key = result[0]
    if model_key is None:
        cwd_root = _cwd_model_root(cli_ctx.project_path)
        result = _find_model(models, cwd_root)
        if result:
            model_key = result[0]
    if model_key is None and default_key and default_key in models:
        model_key = default_key
    if model_key is None:
        model_key = next(iter(models))

    m = models[model_key]
    schema = m.get("schema", {})
    inputs = m.get("inputs", [])
    versions = m.get("versions", [])

    data: dict = {
        "model_key": model_key,
        "default_model": default_key,
        "agency": m.get("agency"),
        "dataset_id": m.get("dataset_id"),
        "model_root": m.get("model_root"),
        "schema_defined": bool(schema.get("ddl")),
        "column_count": len(schema.get("columns", [])),
        "input_count": len(inputs),
        "version_count": len(versions),
    }

    if show_inputs:
        data["inputs"] = [
            {
                "table_address": inp.get("table_address"),
                "last_snapshot_id": inp.get("last_snapshot_id"),
                "last_snapshot_timestamp_ms": inp.get("last_snapshot_timestamp_ms"),
                "rows_loaded": inp.get("rows_loaded"),
            }
            for inp in inputs
        ]
    if show_history:
        data["versions"] = [
            {
                "version": v.get("version"),
                "published_at": v.get("published_at"),
                "rows": v.get("rows"),
                "completed": v.get("completed", False),
            }
            for v in versions
        ]

    print_json("status", data)


def _print_model_summary(key: str, m: dict, *, show_inputs: bool, show_history: bool) -> None:
    """Print human-readable summary for one model."""
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
