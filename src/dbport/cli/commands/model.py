"""dbp model — run lifecycle commands for one model."""

from __future__ import annotations

import typer

from ..context import resolve_model_key
from ..errors import cli_error_handler
from ..render import cli_progress, cli_tree_progress, print_info, print_json, print_success
from . import lifecycle

model_app = typer.Typer(
    name="model",
    help="Run lifecycle operations for one model.",
    no_args_is_help=True,
)


@model_app.command("sync")
def model_sync_cmd(
    ctx: typer.Context,
    model: str | None = typer.Argument(None, help="Model key (agency.dataset_id)."),
) -> None:
    """Sync one model, defaulting to the resolved default model."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    with cli_error_handler("model sync", json_output=cli_ctx.json_output):
        model_key, model_data = resolve_model_key(cli_ctx, model)
        with cli_tree_progress(
            enabled=not cli_ctx.json_output and not cli_ctx.quiet,
            title=f"Syncing {model_key}",
        ) as model_ctx:
            with model_ctx(model_key):
                lifecycle.sync_model(cli_ctx, model_data)

        if cli_ctx.json_output:
            print_json("model sync", {"synced": [model_key], "total": 1, "model": model_key})
        else:
            print_success(f"Synced {model_key}")


@model_app.command("load")
def model_load_cmd(
    ctx: typer.Context,
    model: str | None = typer.Argument(None, help="Model key (agency.dataset_id)."),
    update: bool = typer.Option(
        False,
        "--update",
        help="Resolve the newest available snapshot for each configured input.",
    ),
) -> None:
    """Load configured inputs for one model."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    with cli_error_handler("model load", json_output=cli_ctx.json_output):
        model_key, model_data = resolve_model_key(cli_ctx, model)
        with cli_tree_progress(
            enabled=not cli_ctx.json_output and not cli_ctx.quiet,
            title=f"Loading {model_key}",
        ) as model_ctx:
            with model_ctx(model_key):
                result = lifecycle.load_model(
                    cli_ctx,
                    model_key,
                    model_data,
                    update=update,
                )

        if cli_ctx.json_output:
            print_json("model load", result)
        else:
            if result["loaded"]:
                action = "Updated" if update else "Loaded"
                print_success(f"{action} {len(result['loaded'])} input(s) for {model_key}")
            else:
                print_info(f"No inputs configured for {model_key}")


@model_app.command("exec")
def model_exec_cmd(
    ctx: typer.Context,
    model: str | None = typer.Argument(None, help="Model key (agency.dataset_id)."),
    target: str | None = typer.Option(
        None,
        "--target",
        help="Execute this .sql or .py file instead of the configured model hook.",
    ),
    timing: bool = typer.Option(False, "--timing", help="Print execution duration."),
) -> None:
    """Execute the configured model hook or an explicit override target."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    with cli_error_handler("model exec", json_output=cli_ctx.json_output):
        model_key, model_data = resolve_model_key(cli_ctx, model)
        with cli_progress(enabled=not cli_ctx.json_output and not cli_ctx.quiet):
            result = lifecycle.exec_model(cli_ctx, model_key, model_data, target=target)

        if cli_ctx.json_output:
            print_json("model exec", result)
        else:
            print_success(f"Executed {result['target']}")
            print_info(f"  Model: {model_key}")
            if timing:
                print_info(f"  Duration: {result['elapsed_seconds']:.2f}s")


@model_app.command("publish")
def model_publish_cmd(
    ctx: typer.Context,
    model: str | None = typer.Argument(None, help="Model key (agency.dataset_id)."),
    version: str | None = typer.Option(
        None, "--version", help="Version identifier for this publish."
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate only; do not write data."),
    refresh: bool = typer.Option(False, "--refresh", help="Overwrite existing version."),
    message: str | None = typer.Option(None, "--message", "-m", help="Publish note for history."),
) -> None:
    """Publish one model, defaulting to the resolved default model."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    with cli_error_handler("model publish", json_output=cli_ctx.json_output):
        model_key, model_data = resolve_model_key(cli_ctx, model)
        with cli_tree_progress(
            enabled=not cli_ctx.json_output and not cli_ctx.quiet,
            title=f"Publishing {model_key}",
        ) as model_ctx:
            with model_ctx(model_key):
                result = lifecycle.publish_model(
                    cli_ctx,
                    model_key,
                    model_data,
                    version=version,
                    dry_run=dry_run,
                    refresh=refresh,
                )

        if cli_ctx.json_output:
            print_json("model publish", result)
        else:
            if result["mode"] == "dry":
                print_success(f"Dry run completed for version {result['version']}")
            else:
                print_success(f"Published version {result['version']}")
            print_info(f"  Model: {model_key}")
            if message:
                print_info(f"  Note:  {message}")


@model_app.command("run")
def model_run_cmd(
    ctx: typer.Context,
    model: str | None = typer.Argument(None, help="Model key (agency.dataset_id)."),
    version: str | None = typer.Option(
        None, "--version", help="Version to publish after execution."
    ),
    target: str | None = typer.Option(
        None,
        "--target",
        help="Execute this .sql or .py file instead of the configured model hook.",
    ),
    timing: bool = typer.Option(False, "--timing", help="Print execution duration."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate only; do not publish."),
    refresh: bool = typer.Option(False, "--refresh", help="Overwrite existing version."),
) -> None:
    """Run sync, hook execution, and publish for one model."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    with cli_error_handler("model run", json_output=cli_ctx.json_output):
        model_key, model_data = resolve_model_key(cli_ctx, model)
        with cli_tree_progress(
            enabled=not cli_ctx.json_output and not cli_ctx.quiet,
            title=f"Running {model_key}",
        ) as model_ctx:
            with model_ctx(model_key):
                result = lifecycle.run_model(
                    cli_ctx,
                    model_key,
                    model_data,
                    version=version,
                    target=target,
                    dry_run=dry_run,
                    refresh=refresh,
                )

        if cli_ctx.json_output:
            print_json("model run", result)
        else:
            print_success(f"Executed model {model_key} via {result['target']}")
            print_info(f"  Published version: {result['version']}")
            if timing:
                print_info(f"  Duration: {result['elapsed_seconds']:.2f}s")
