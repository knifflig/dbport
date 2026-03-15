"""dbp publish — publish the output dataset."""

from __future__ import annotations

from typing import Optional

import typer

from ..context import resolve_model_paths
from ..errors import cli_error_handler
from ..render import cli_progress, print_info, print_json, print_success


def publish_cmd(
    ctx: typer.Context,
    version: str = typer.Option(..., "--version", help="Version identifier for this publish."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate only; do not write data."),
    refresh: bool = typer.Option(False, "--refresh", help="Overwrite existing version."),
    message: Optional[str] = typer.Option(None, "--message", "-m", help="Publish note for history."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt."),
    strict: bool = typer.Option(False, "--strict", help="Fail on warnings."),
) -> None:
    """Publish the current output dataset to the warehouse."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("publish", json_output=cli_ctx.json_output):
        from ...adapters.primary.client import DBPort

        paths = resolve_model_paths(cli_ctx)

        # Determine mode
        mode: str | None = None
        if dry_run:
            mode = "dry"
        elif refresh:
            mode = "refresh"

        with DBPort(
            agency=paths.agency,
            dataset_id=paths.dataset_id,
            lock_path=paths.lock_path,
            duckdb_path=paths.duckdb_path,
            model_root=paths.model_root,
        ) as port:
            with cli_progress(enabled=not cli_ctx.json_output and not cli_ctx.quiet):
                port.publish(version=version, mode=mode)

        if cli_ctx.json_output:
            print_json("publish", {
                "version": version,
                "mode": mode,
                "model": f"{paths.agency}.{paths.dataset_id}",
            })
        else:
            if mode == "dry":
                print_success(f"Dry run completed for version {version}")
            else:
                print_success(f"Published version {version}")
            print_info(f"  Model: {paths.agency}.{paths.dataset_id}")
            if message:
                print_info(f"  Note:  {message}")
