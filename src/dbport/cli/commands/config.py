"""dbp config — repo-level control plane.

Subcommands for inspecting and managing project configuration persisted
in ``dbport.lock``.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

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
    model_key: str | None = typer.Argument(None, help="Model key (agency.dataset_id) to set as default."),
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


@config_app.command(name="folder")
def folder_cmd(
    ctx: typer.Context,
    folder: str | None = typer.Argument(None, help="Models folder relative to project root (e.g. 'models' or 'examples')."),
) -> None:
    """Show or set the default models folder for new models.

    New models created with `dbp init` are scaffolded inside this folder.
    Default: 'models'.
    """
    from ..context import read_models_folder, write_models_folder
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("config folder", json_output=cli_ctx.json_output):
        if folder is None:
            # Show current models folder
            current = read_models_folder(cli_ctx.lockfile_path)
            if cli_ctx.json_output:
                print_json("config folder", {"models_folder": current})
            else:
                print_info(f"Models folder: {current}")
        else:
            # Normalize: strip leading/trailing slashes
            folder = folder.strip("/")
            write_models_folder(cli_ctx.lockfile_path, folder)

            if cli_ctx.json_output:
                print_json("config folder", {"models_folder": folder})
            else:
                print_success(f"Models folder set to: {folder}")


@config_app.command(name="run-hook")
def run_hook_cmd(
    ctx: typer.Context,
    hook_path: str | None = typer.Argument(None, help="Path to run hook file (e.g. sql/main.sql or run.py)."),
) -> None:
    """Show or set the run hook for the resolved model."""
    from ..context import resolve_model_paths
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("config run-hook", json_output=cli_ctx.json_output):
        if hook_path is None:
            # Show current run_hook
            paths = resolve_model_paths(cli_ctx)
            from ...adapters.secondary.lock.toml import TomlLockAdapter

            model_key = f"{paths.agency}.{paths.dataset_id}"
            raw_root = str(Path(paths.model_root).relative_to(cli_ctx.project_path)) if Path(paths.model_root).is_absolute() else paths.model_root
            raw_db = str(Path(paths.duckdb_path).relative_to(cli_ctx.project_path)) if Path(paths.duckdb_path).is_absolute() else paths.duckdb_path
            adapter = TomlLockAdapter(
                cli_ctx.lockfile_path,
                model_key=model_key,
                model_root=raw_root,
                duckdb_path=raw_db,
            )
            current = adapter.read_run_hook()
            if cli_ctx.json_output:
                print_json("config run-hook", {"run_hook": current, "model": model_key})
            elif current:
                print_info(f"Run hook for {model_key}: {current}")
            else:
                print_info(f"No run hook set for {model_key}.")
        else:
            # Set run_hook
            paths = resolve_model_paths(cli_ctx)
            from ...adapters.secondary.lock.toml import TomlLockAdapter

            model_key = f"{paths.agency}.{paths.dataset_id}"
            raw_root = str(Path(paths.model_root).relative_to(cli_ctx.project_path)) if Path(paths.model_root).is_absolute() else paths.model_root
            raw_db = str(Path(paths.duckdb_path).relative_to(cli_ctx.project_path)) if Path(paths.duckdb_path).is_absolute() else paths.duckdb_path
            adapter = TomlLockAdapter(
                cli_ctx.lockfile_path,
                model_key=model_key,
                model_root=raw_root,
                duckdb_path=raw_db,
            )
            # Normalize hook_path: resolve relative to CWD, store relative to model_root
            abs_model_root = (cli_ctx.project_path / raw_root).resolve()
            hook = Path(hook_path)
            if not hook.is_absolute():
                hook = (Path.cwd() / hook).resolve()
            try:
                normalized = str(hook.relative_to(abs_model_root))
            except ValueError:
                normalized = hook_path  # Outside model_root — store as-is
            adapter.write_run_hook(normalized)

            if cli_ctx.json_output:
                print_json("config run-hook", {"run_hook": normalized, "model": model_key})
            else:
                print_success(f"Run hook set to: {normalized}")


def _make_lock_adapter(cli_ctx):
    """Create a TomlLockAdapter for the resolved model."""
    from ..context import resolve_model_paths
    from ...adapters.secondary.lock.toml import TomlLockAdapter

    paths = resolve_model_paths(cli_ctx)
    model_key = f"{paths.agency}.{paths.dataset_id}"
    raw_root = str(Path(paths.model_root).relative_to(cli_ctx.project_path)) if Path(paths.model_root).is_absolute() else paths.model_root
    raw_db = str(Path(paths.duckdb_path).relative_to(cli_ctx.project_path)) if Path(paths.duckdb_path).is_absolute() else paths.duckdb_path
    adapter = TomlLockAdapter(
        cli_ctx.lockfile_path,
        model_key=model_key,
        model_root=raw_root,
        duckdb_path=raw_db,
    )
    return adapter, model_key


@config_app.command(name="meta")
def meta_cmd(
    ctx: typer.Context,
    column: str | None = typer.Argument(None, help="Column name to configure."),
    codelist_id: str | None = typer.Option(None, "--id", help="Codelist identifier."),
    codelist_type: str | None = typer.Option(None, "--type", help="Codelist type (e.g. categorical, hierarchical)."),
    codelist_kind: str | None = typer.Option(None, "--kind", help="Codelist kind (e.g. reference, derived)."),
    codelist_labels: str | None = typer.Option(None, "--labels", help="JSON labels (e.g. '{\"en\": \"Geography\"}')."),
) -> None:
    """Show or set codelist metadata for output columns."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("config meta", json_output=cli_ctx.json_output):
        adapter, model_key = _make_lock_adapter(cli_ctx)

        if column is None:
            # Show all column metadata
            entries = adapter.read_codelist_entries()
            if cli_ctx.json_output:
                data = {
                    name: {
                        "codelist_id": e.codelist_id,
                        "codelist_type": e.codelist_type,
                        "codelist_kind": e.codelist_kind,
                        "codelist_labels": e.codelist_labels,
                        "attach_table": e.attach_table,
                    }
                    for name, e in entries.items()
                }
                print_json("config meta", {"model": model_key, "columns": data})
            elif entries:
                rows = []
                for name, e in entries.items():
                    rows.append([
                        name,
                        e.codelist_id or "",
                        e.codelist_type or "",
                        e.codelist_kind or "",
                        e.attach_table or "",
                    ])
                print_table(
                    f"Column metadata — {model_key}",
                    ["Column", "ID", "Type", "Kind", "Attach"],
                    rows,
                )
            else:
                print_info("No columns defined. Apply a schema first: dbp schema sql/create_output.sql")
            return

        # Set metadata for a specific column
        from ....domain.entities.codelist import CodelistEntry

        entries = adapter.read_codelist_entries()
        existing = entries.get(column)
        if existing is None:
            existing = CodelistEntry(
                column_name=column, column_pos=0, codelist_id=column,
            )

        overrides: dict = {}
        if codelist_id is not None:
            overrides["codelist_id"] = codelist_id
        if codelist_type is not None:
            overrides["codelist_type"] = codelist_type
        if codelist_kind is not None:
            overrides["codelist_kind"] = codelist_kind
        if codelist_labels is not None:
            import json
            overrides["codelist_labels"] = json.loads(codelist_labels)

        updated = existing.model_copy(update=overrides)
        adapter.write_codelist_entry(updated)

        if cli_ctx.json_output:
            print_json("config meta", {
                "column": column,
                "model": model_key,
                "codelist_id": updated.codelist_id,
                "codelist_type": updated.codelist_type,
                "codelist_kind": updated.codelist_kind,
            })
        else:
            print_success(f"Updated metadata for column '{column}'")


@config_app.command(name="attach")
def attach_cmd(
    ctx: typer.Context,
    column: str = typer.Argument(help="Column name to attach codelist to."),
    table: str = typer.Option(..., "--table", help="DuckDB table address to use as codelist source."),
) -> None:
    """Attach a DuckDB table as codelist source for a column."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("config attach", json_output=cli_ctx.json_output):
        from ....domain.entities.codelist import CodelistEntry

        adapter, model_key = _make_lock_adapter(cli_ctx)

        entries = adapter.read_codelist_entries()
        existing = entries.get(column)
        if existing is None:
            existing = CodelistEntry(
                column_name=column, column_pos=0, codelist_id=column,
            )

        updated = existing.model_copy(update={"attach_table": table})
        adapter.write_codelist_entry(updated)

        if cli_ctx.json_output:
            print_json("config attach", {
                "column": column,
                "table": table,
                "model": model_key,
            })
        else:
            print_success(f"Attached '{table}' as codelist for column '{column}'")


@config_app.command(name="info")
def info_cmd(
    ctx: typer.Context,
    inputs: bool = typer.Option(False, "--inputs", help="Show input details."),
    history: bool = typer.Option(False, "--history", help="Show publish history."),
    raw: bool = typer.Option(False, "--raw", help="Show raw lock file TOML."),
) -> None:
    """Inspect persisted lock file state for the resolved model."""
    from ..context import _resolve_model_data, read_default_model, read_lock_models
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
