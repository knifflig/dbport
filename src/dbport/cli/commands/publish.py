"""dbp publish — publish the output dataset."""

from __future__ import annotations

import typer

from ..context import (
    read_lock_versions,
    resolve_model_key,
    resolve_model_paths_from_data,
)
from ..errors import cli_error_handler
from ..render import cli_tree_progress, print_info, print_json, print_success


def publish_cmd(
    ctx: typer.Context,
    model: str | None = typer.Argument(None, help="Model key (agency.dataset_id)."),
    version: str | None = typer.Option(None, "--version", help="Version identifier for this publish."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate only; do not write data."),
    refresh: bool = typer.Option(False, "--refresh", help="Overwrite existing version."),
) -> None:
    """Publish the current output dataset to the warehouse."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("publish", json_output=cli_ctx.json_output):
        from ...adapters.primary.client import DBPort

        model_key, model_data = resolve_model_key(cli_ctx, model)
        paths = resolve_model_paths_from_data(cli_ctx, model_data)

        # Resolve version: explicit > latest completed from lock
        pub_version = version
        if pub_version is None:
            lock_versions = read_lock_versions(cli_ctx.lockfile_path, model_key)
            completed = [v for v in lock_versions if v.get("completed")]
            if not completed:
                raise RuntimeError(
                    "No completed versions found in lock file. "
                    "Specify --version explicitly."
                )
            pub_version = completed[-1]["version"]

        # Determine mode
        mode: str | None = None
        if dry_run:
            mode = "dry"
        elif refresh:
            mode = "refresh"

        with cli_tree_progress(
            enabled=not cli_ctx.json_output and not cli_ctx.quiet,
            title=f"Publishing {model_key}",
        ) as model_ctx:
            with model_ctx(model_key):
                with DBPort(
                    agency=paths.agency,
                    dataset_id=paths.dataset_id,
                    lock_path=paths.lock_path,
                    duckdb_path=paths.duckdb_path,
                    model_root=paths.model_root,
                ) as port:
                    port.publish(version=pub_version, mode=mode)

        if cli_ctx.json_output:
            print_json("publish", {
                "version": pub_version,
                "mode": mode,
                "model": model_key,
            })
        else:
            if mode == "dry":
                print_success(f"Dry run completed for version {pub_version}")
            else:
                print_success(f"Published version {pub_version}")
            print_info(f"  Model: {model_key}")
