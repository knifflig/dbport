"""dbp run — run the complete model workflow: sync, execute, publish."""

from __future__ import annotations

import time
from typing import Optional

import typer

from ..context import resolve_model_paths
from ..errors import cli_error_handler
from ..render import print_info, print_json, print_success


def run_cmd(
    ctx: typer.Context,
    version: Optional[str] = typer.Option(None, "--version", help="Version to publish after execution."),
    timing: bool = typer.Option(False, "--timing", help="Print execution duration."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate only; do not publish."),
    refresh: bool = typer.Option(False, "--refresh", help="Overwrite existing version."),
) -> None:
    """Run the complete model workflow: sync, execute, publish."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("run", json_output=cli_ctx.json_output):
        from ...adapters.primary.client import DBPort

        paths = resolve_model_paths(cli_ctx)

        t0 = time.monotonic()

        with DBPort(
            agency=paths.agency,
            dataset_id=paths.dataset_id,
            lock_path=paths.lock_path,
            duckdb_path=paths.duckdb_path,
            model_root=paths.model_root,
        ) as port:
            # Read run_hook from lock
            run_hook = port._lock.read_run_hook()
            if not run_hook:
                raise RuntimeError(
                    "No run_hook configured for this model. "
                    "Set it with: dbp config run-hook <path>"
                )

            # Execute the model
            port.execute(run_hook)

            # Publish if version provided
            if version:
                mode = None
                if dry_run:
                    mode = "dry"
                elif refresh:
                    mode = "refresh"
                port.publish(version=version, mode=mode)

        elapsed = time.monotonic() - t0

        if cli_ctx.json_output:
            data: dict = {
                "run_hook": run_hook,
                "elapsed_seconds": round(elapsed, 3),
            }
            if version:
                data["version"] = version
            print_json("run", data)
        else:
            print_success(f"Executed model via {run_hook}")
            if version:
                print_info(f"  Published version: {version}")
            if timing:
                print_info(f"  Duration: {elapsed:.2f}s")
