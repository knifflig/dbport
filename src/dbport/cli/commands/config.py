"""dbp config — repo-level control plane.

Subcommands for inspecting and managing project configuration persisted
in ``dbport.lock``.
"""

from __future__ import annotations

from pathlib import Path

import typer

from ..errors import cli_error_handler
from ..render import print_error, print_info, print_json, print_success, print_table, print_warning

CONFIG_HELP = "Manage repo defaults and model-specific configuration."

config_app = typer.Typer(
    name="config",
    help=CONFIG_HELP,
    no_args_is_help=True,
)

default_app = typer.Typer(
    name="default",
    help="Manage repo default settings.",
    no_args_is_help=True,
)

model_app = typer.Typer(
    name="model",
    help="Manage configuration for a specific model.",
    no_args_is_help=True,
)

columns_app = typer.Typer(
    name="columns",
    help="Manage column metadata for a specific model.",
    no_args_is_help=False,
)


@default_app.command("model")
def default_model_cmd(
    ctx: typer.Context,
    model_key: str | None = typer.Argument(None, help="Model key (agency.dataset_id)."),
) -> None:
    """Show or set the default model for this project."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    _handle_default_model(cli_ctx, model_key)


@default_app.command("folder")
def default_folder_cmd(
    ctx: typer.Context,
    folder: str | None = typer.Argument(None, help="Models folder relative to project root."),
) -> None:
    """Show or set the default models folder for new models."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    _handle_default_folder(cli_ctx, folder)


@default_app.command("hook")
def default_hook_cmd(
    ctx: typer.Context,
    hook_path: str | None = typer.Argument(None, help="Path to run hook file."),
) -> None:
    """Show or set the run hook for the resolved model."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    _handle_default_hook(cli_ctx, hook_path)


@model_app.callback()
def model_callback(
    ctx: typer.Context,
    model_key: str = typer.Argument(..., help="Model key (agency.dataset_id)."),
) -> None:
    """Select the model to configure."""
    ctx.ensure_object(dict)
    ctx.obj["config_model_key"] = model_key


@model_app.command("version")
def model_version_cmd(
    ctx: typer.Context,
    version: str | None = typer.Argument(None, help="Version string to set."),
) -> None:
    """Show or set the configured version for a model."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    model_key = _get_selected_model_key(ctx)
    _handle_version_for_model(cli_ctx, model_key, version)


@model_app.command("input")
def model_input_cmd(
    ctx: typer.Context,
    dataset: str | None = typer.Argument(None, help="Table address to configure as input."),
    filters: list[str] = typer.Option(
        None,
        "--filter",
        help="Equality filter as key=value. Repeatable.",
    ),
    version: str | None = typer.Option(None, "--version", help="Pinned dataset version to load."),
    load: bool = typer.Option(
        False,
        "--load",
        help="Load the configured input immediately after persisting it.",
    ),
) -> None:
    """Show configured inputs or add one, optionally loading it."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    model_key = _get_selected_model_key(ctx)
    if dataset is None:
        _handle_inputs_show(cli_ctx, model_key)
        return

    _handle_input_add(
        cli_ctx,
        model_key,
        dataset=dataset,
        filters=filters,
        version=version,
        load=load,
    )


@columns_app.callback(invoke_without_command=True)
def model_columns_cmd(
    ctx: typer.Context,
) -> None:
    """Show current column metadata for a model."""
    if ctx.invoked_subcommand is not None:
        return

    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    model_key = _get_selected_model_key(ctx)
    _handle_columns_show(cli_ctx, model_key)


@columns_app.command("set")
def model_columns_set_cmd(
    ctx: typer.Context,
    column: str = typer.Argument(..., help="Column name to inspect or configure."),
    codelist_id: str | None = typer.Option(None, "--id", help="Codelist identifier."),
    codelist_type: str | None = typer.Option(None, "--type", help="Codelist type."),
    codelist_kind: str | None = typer.Option(None, "--kind", help="Codelist kind."),
    codelist_labels: str | None = typer.Option(None, "--labels", help="JSON labels."),
) -> None:
    """Set column metadata for a model."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    model_key = _get_selected_model_key(ctx)
    _handle_columns_set(
        cli_ctx,
        model_key,
        column=column,
        codelist_id=codelist_id,
        codelist_type=codelist_type,
        codelist_kind=codelist_kind,
        codelist_labels=codelist_labels,
    )


@columns_app.command("attach")
def model_attach_cmd(
    ctx: typer.Context,
    column: str = typer.Argument(..., help="Column name to attach a codelist to."),
    table: str = typer.Argument(..., help="DuckDB table address to use as codelist source."),
) -> None:
    """Attach a table as the codelist source for a column."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    model_key = _get_selected_model_key(ctx)
    _handle_attach_for_model(cli_ctx, model_key, column, table)


config_app.add_typer(default_app, name="default")
config_app.add_typer(model_app, name="model")
model_app.add_typer(columns_app, name="columns")

from . import schema as schema_commands  # noqa: E402

model_app.command("schema")(schema_commands.schema_cmd)


def _get_selected_model_key(ctx: typer.Context) -> str:
    if ctx.obj and "config_model_key" in ctx.obj:
        return ctx.obj["config_model_key"]
    if ctx.parent is not None:
        return _get_selected_model_key(ctx.parent)
    raise typer.BadParameter("Missing model key. Use: dbp config model <model_key> ...")


def _handle_default_model(cli_ctx, model_key: str | None) -> None:
    """Show or set the default model for this project."""
    from ..context import read_default_model, read_lock_models, write_default_model

    with cli_error_handler("config default model", json_output=cli_ctx.json_output):
        if model_key is None:
            # Show current default
            current = read_default_model(cli_ctx.lockfile_path)
            if cli_ctx.json_output:
                print_json("config default model", {"default_model": current})
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
                print_json("config default model", {"default_model": model_key})
            else:
                print_success(f"Default model set to: {model_key}")


def _handle_default_folder(cli_ctx, folder: str | None) -> None:
    """Show or set the default models folder for new models.

    New models created with `dbp init` are scaffolded inside this folder.
    Default: 'models'.
    """
    from ..context import read_models_folder, write_models_folder

    with cli_error_handler("config default folder", json_output=cli_ctx.json_output):
        if folder is None:
            # Show current models folder
            current = read_models_folder(cli_ctx.lockfile_path)
            if cli_ctx.json_output:
                print_json("config default folder", {"models_folder": current})
            else:
                print_info(f"Models folder: {current}")
        else:
            # Normalize: strip leading/trailing slashes
            folder = folder.strip("/")
            write_models_folder(cli_ctx.lockfile_path, folder)

            if cli_ctx.json_output:
                print_json("config default folder", {"models_folder": folder})
            else:
                print_success(f"Models folder set to: {folder}")


def _handle_default_hook(cli_ctx, hook_path: str | None) -> None:
    """Show or set the run hook for the resolved model."""
    with cli_error_handler("config default hook", json_output=cli_ctx.json_output):
        adapter, model_key = _make_lock_adapter(cli_ctx)

        if hook_path is None:
            from ...application.services.run import resolve_run_hook
            from ..context import resolve_model_paths

            paths = resolve_model_paths(cli_ctx)
            current = resolve_run_hook(adapter, str(paths.model_root))
            if cli_ctx.json_output:
                print_json("config default hook", {"run_hook": current, "model": model_key})
            else:
                print_info(f"Run hook for {model_key}: {current}")
        else:
            from ..context import resolve_model_paths

            paths = resolve_model_paths(cli_ctx)
            # Normalize hook_path: resolve relative to CWD, store relative to model_root
            raw_root = (
                str(Path(paths.model_root).relative_to(cli_ctx.project_path))
                if Path(paths.model_root).is_absolute()
                else paths.model_root
            )
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
                print_json("config default hook", {"run_hook": normalized, "model": model_key})
            else:
                print_success(f"Run hook set to: {normalized}")


def _make_lock_adapter(cli_ctx):
    """Create a TomlLockAdapter for the resolved model."""
    from ...adapters.secondary.lock.toml import TomlLockAdapter
    from ..context import resolve_model_paths

    paths = resolve_model_paths(cli_ctx)
    model_key = f"{paths.agency}.{paths.dataset_id}"
    raw_root = (
        str(Path(paths.model_root).relative_to(cli_ctx.project_path))
        if Path(paths.model_root).is_absolute()
        else paths.model_root
    )
    raw_db = (
        str(Path(paths.duckdb_path).relative_to(cli_ctx.project_path))
        if Path(paths.duckdb_path).is_absolute()
        else paths.duckdb_path
    )
    adapter = TomlLockAdapter(
        cli_ctx.lockfile_path,
        model_key=model_key,
        model_root=raw_root,
        duckdb_path=raw_db,
    )
    return adapter, model_key


def _make_lock_adapter_for_model(cli_ctx, explicit_model_key: str):
    """Create a TomlLockAdapter for an explicitly selected model key."""
    from ...adapters.secondary.lock.toml import TomlLockAdapter
    from ..context import read_lock_models, resolve_model_paths_from_data

    models = read_lock_models(cli_ctx.lockfile_path)
    if explicit_model_key not in models:
        available = list(models.keys()) if models else []
        print_error(
            f"Model '{explicit_model_key}' not found in {cli_ctx.lockfile_path}. "
            f"Available: {available}"
        )
        raise typer.Exit(1)

    paths = resolve_model_paths_from_data(cli_ctx, models[explicit_model_key])
    raw_root = (
        str(Path(paths.model_root).relative_to(cli_ctx.project_path))
        if Path(paths.model_root).is_absolute()
        else paths.model_root
    )
    raw_db = (
        str(Path(paths.duckdb_path).relative_to(cli_ctx.project_path))
        if Path(paths.duckdb_path).is_absolute()
        else paths.duckdb_path
    )
    adapter = TomlLockAdapter(
        cli_ctx.lockfile_path,
        model_key=explicit_model_key,
        model_root=raw_root,
        duckdb_path=raw_db,
    )
    return adapter, explicit_model_key


def _handle_version_for_model(cli_ctx, model_key: str, version: str | None) -> None:
    with cli_error_handler("config version", json_output=cli_ctx.json_output):
        adapter, resolved_model_key = _make_lock_adapter_for_model(cli_ctx, model_key)

        if version is None:
            current = adapter.read_version()
            if cli_ctx.json_output:
                print_json("config version", {"version": current, "model": resolved_model_key})
            elif current:
                print_info(f"Version for {resolved_model_key}: {current}")
            else:
                print_info(f"No version set for {resolved_model_key}.")
            return

        adapter.write_version(version)
        if cli_ctx.json_output:
            print_json("config version", {"version": version, "model": resolved_model_key})
        else:
            print_success(f"Version set to: {version}")


def _handle_inputs_show(cli_ctx, model_key: str) -> None:
    with cli_error_handler("config input", json_output=cli_ctx.json_output):
        adapter, resolved_model_key = _make_lock_adapter_for_model(cli_ctx, model_key)
        records = adapter.read_ingest_records()

        if cli_ctx.json_output:
            data = [
                {
                    "table_address": record.table_address,
                    "filters": record.filters,
                    "version": record.version,
                    "rows_loaded": record.rows_loaded,
                    "last_snapshot_id": record.last_snapshot_id,
                }
                for record in records
            ]
            print_json("config input", {"model": resolved_model_key, "inputs": data})
            return

        if not records:
            print_info(f"No inputs configured for {resolved_model_key}.")
            return

        rows = []
        for record in records:
            filter_text = ""
            if record.filters:
                filter_text = ", ".join(f"{key}={value}" for key, value in record.filters.items())
            rows.append(
                [
                    record.table_address,
                    record.version or "",
                    filter_text,
                    str(record.rows_loaded or ""),
                ]
            )

        print_table(
            f"Inputs — {resolved_model_key}",
            ["Table", "Version", "Filters", "Rows"],
            rows,
        )


def _handle_input_add(
    cli_ctx,
    model_key: str,
    *,
    dataset: str,
    filters: list[str] | None,
    version: str | None,
    load: bool,
) -> None:
    from ...adapters.primary.client import DBPort
    from ..context import read_lock_models, resolve_model_paths_from_data

    with cli_error_handler("config input", json_output=cli_ctx.json_output):
        _, resolved_model_key = _make_lock_adapter_for_model(cli_ctx, model_key)
        parsed_filters = _parse_input_filters(filters)
        model_data = read_lock_models(cli_ctx.lockfile_path)[resolved_model_key]
        paths = resolve_model_paths_from_data(cli_ctx, model_data)
        with DBPort(
            agency=paths.agency,
            dataset_id=paths.dataset_id,
            lock_path=paths.lock_path,
            duckdb_path=paths.duckdb_path,
            model_root=paths.model_root,
            load_inputs_on_init=False,
        ) as port:
            if load:
                resolved_record = port.load(
                    dataset,
                    filters=parsed_filters,
                    version=version,
                )
            else:
                resolved_record = port.configure_input(
                    dataset,
                    filters=parsed_filters,
                    version=version,
                )

        if cli_ctx.json_output:
            print_json(
                "config input",
                {
                    "model": resolved_model_key,
                    "table_address": dataset,
                    "filters": parsed_filters,
                    "version": resolved_record.version,
                    "last_snapshot_id": resolved_record.last_snapshot_id,
                    "last_snapshot_timestamp_ms": resolved_record.last_snapshot_timestamp_ms,
                    "rows_loaded": resolved_record.rows_loaded,
                    "load": load,
                },
            )
        else:
            if load:
                print_success(f"Configured and loaded input {dataset} for {resolved_model_key}")
            else:
                print_success(f"Configured input {dataset} for {resolved_model_key}")


def _parse_input_filters(filters: list[str] | None) -> dict[str, str] | None:
    if not filters:
        return None

    parsed: dict[str, str] = {}
    for item in filters:
        if "=" not in item:
            raise typer.BadParameter(
                f"Invalid --filter '{item}'. Expected key=value.",
                param_hint="--filter",
            )
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise typer.BadParameter(
                f"Invalid --filter '{item}'. Key must not be empty.",
                param_hint="--filter",
            )
        parsed[key] = value
    return parsed


def _handle_columns_show(cli_ctx, model_key: str) -> None:
    with cli_error_handler("config columns", json_output=cli_ctx.json_output):
        adapter, resolved_model_key = _make_lock_adapter_for_model(cli_ctx, model_key)
        entries = adapter.read_codelist_entries()
        if cli_ctx.json_output:
            data = {
                name: {
                    "codelist_id": entry.codelist_id,
                    "codelist_type": entry.codelist_type,
                    "codelist_kind": entry.codelist_kind,
                    "codelist_labels": entry.codelist_labels,
                    "attach_table": entry.attach_table,
                }
                for name, entry in entries.items()
            }
            print_json("config columns", {"model": resolved_model_key, "columns": data})
        elif entries:
            rows = [
                [
                    name,
                    entry.codelist_id or "",
                    entry.codelist_type or "",
                    entry.codelist_kind or "",
                    entry.attach_table or "",
                ]
                for name, entry in entries.items()
            ]
            print_table(
                f"Column metadata — {resolved_model_key}",
                ["Column", "ID", "Type", "Kind", "Attach"],
                rows,
            )
        else:
            print_info(
                "No columns defined. Apply a schema first: "
                f"dbp config model {resolved_model_key} schema sql/create_output.sql"
            )


def _handle_columns_set(
    cli_ctx,
    model_key: str,
    *,
    column: str,
    codelist_id: str | None,
    codelist_type: str | None,
    codelist_kind: str | None,
    codelist_labels: str | None,
) -> None:
    with cli_error_handler("config columns", json_output=cli_ctx.json_output):
        _, resolved_model_key = _make_lock_adapter_for_model(cli_ctx, model_key)
        overrides = _parse_column_override_args(
            codelist_id=codelist_id,
            codelist_type=codelist_type,
            codelist_kind=codelist_kind,
            codelist_labels=codelist_labels,
        )
        _update_column_metadata(cli_ctx, resolved_model_key, column, overrides)


def _parse_column_override_args(
    *,
    codelist_id: str | None,
    codelist_type: str | None,
    codelist_kind: str | None,
    codelist_labels: str | None,
) -> dict:
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

    return overrides


def _handle_attach_for_model(cli_ctx, model_key: str, column: str, table: str) -> None:
    with cli_error_handler("config columns attach", json_output=cli_ctx.json_output):
        _, resolved_model_key = _make_lock_adapter_for_model(cli_ctx, model_key)
        _attach_column_table(cli_ctx, resolved_model_key, column, table)


def _update_column_metadata(cli_ctx, model_key: str, column: str, overrides: dict) -> None:
    from ...adapters.primary.client import DBPort
    from ..context import resolve_model_paths_from_data, read_lock_models

    models = read_lock_models(cli_ctx.lockfile_path)
    model_data = models[model_key]
    paths = resolve_model_paths_from_data(cli_ctx, model_data)

    with DBPort(
        agency=paths.agency,
        dataset_id=paths.dataset_id,
        lock_path=paths.lock_path,
        duckdb_path=paths.duckdb_path,
        model_root=paths.model_root,
        config_only=True,
    ) as port:
        col = getattr(port.columns, column)
        col.meta(**overrides)

    if cli_ctx.json_output:
        print_json(
            "config columns",
            {
                "column": column,
                "model": model_key,
                **{k: v for k, v in overrides.items() if v is not None},
            },
        )
    else:
        print_success(f"Updated metadata for column '{column}'")


def _attach_column_table(cli_ctx, model_key: str, column: str, table: str) -> None:
    from ...adapters.primary.client import DBPort
    from ..context import resolve_model_paths_from_data, read_lock_models

    models = read_lock_models(cli_ctx.lockfile_path)
    model_data = models[model_key]
    paths = resolve_model_paths_from_data(cli_ctx, model_data)

    with DBPort(
        agency=paths.agency,
        dataset_id=paths.dataset_id,
        lock_path=paths.lock_path,
        duckdb_path=paths.duckdb_path,
        model_root=paths.model_root,
        config_only=True,
    ) as port:
        col = getattr(port.columns, column)
        col.attach(table=table)

    if cli_ctx.json_output:
        print_json(
            "config columns attach",
            {
                "column": column,
                "table": table,
                "model": model_key,
            },
        )
    else:
        print_success(f"Attached '{table}' as codelist for column '{column}'")
