"""dbp run — run the complete model workflow: sync, execute, publish."""

from __future__ import annotations

import time

import typer

from ..context import (
    read_lock_version_config,
    read_lock_versions,
    resolve_model_key,
    resolve_model_paths_from_data,
)
from ..errors import cli_error_handler
from ..render import cli_tree_progress, print_info, print_json, print_success


def run_cmd(
    ctx: typer.Context,
    model: str | None = typer.Argument(None, help="Model key (agency.dataset_id)."),
    version: str | None = typer.Option(None, "--version", help="Version to publish after execution."),
    timing: bool = typer.Option(False, "--timing", help="Print execution duration."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate only; do not publish."),
    refresh: bool = typer.Option(False, "--refresh", help="Overwrite existing version."),
) -> None:
    """Run the complete model workflow: sync, execute, publish."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("run", json_output=cli_ctx.json_output):
        from ...adapters.primary.client import DBPort
        from ...infrastructure.progress import progress_callback

        model_key, model_data = resolve_model_key(cli_ctx, model)
        paths = resolve_model_paths_from_data(cli_ctx, model_data)

        t0 = time.monotonic()

        with cli_tree_progress(
            enabled=not cli_ctx.json_output and not cli_ctx.quiet,
            title=f"Running {model_key}",
        ) as model_ctx:
            with model_ctx(model_key):
                with DBPort(
                    agency=paths.agency,
                    dataset_id=paths.dataset_id,
                    lock_path=paths.lock_path,
                    duckdb_path=paths.duckdb_path,
                    model_root=paths.model_root,
                ) as port:
                    # Read hook path for display / JSON output
                    run_hook = port.run_hook

                    # Resolve version: CLI flag → config → latest completed → fail
                    pub_version = version
                    if pub_version is None:
                        pub_version = read_lock_version_config(
                            cli_ctx.lockfile_path, model_key
                        )
                    if pub_version is None:
                        lock_versions = read_lock_versions(
                            cli_ctx.lockfile_path, model_key
                        )
                        completed = [v for v in lock_versions if v.get("completed")]
                        if completed:
                            pub_version = completed[-1]["version"]
                    if pub_version is None:
                        raise typer.BadParameter(
                            "No version available. Set one with: "
                            "dbp config version <version>"
                        )

                    mode = None
                    if dry_run:
                        mode = "dry"
                    elif refresh:
                        mode = "refresh"

                    cb = progress_callback.get(None)
                    if cb:
                        cb.started(f"Executing {run_hook or 'run hook'}")
                    port.run(version=pub_version, mode=mode)
                    if cb:
                        cb.finished(f"Executed {run_hook or 'run hook'}")

        elapsed = time.monotonic() - t0

        if cli_ctx.json_output:
            data: dict = {
                "model": model_key,
                "run_hook": run_hook,
                "elapsed_seconds": round(elapsed, 3),
            }
            if pub_version:
                data["version"] = pub_version
            print_json("run", data)
        else:
            print_success(f"Executed model {model_key} via {run_hook}")
            if pub_version:
                print_info(f"  Published version: {pub_version}")
            if timing:
                print_info(f"  Duration: {elapsed:.2f}s")
