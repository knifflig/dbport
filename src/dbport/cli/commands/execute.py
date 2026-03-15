"""dbp execute — execute SQL transforms or script files."""

from __future__ import annotations

import time
from typing import Optional

import typer

from ..context import resolve_model_paths
from ..errors import cli_error_handler
from ..render import cli_progress, print_info, print_json, print_success


def execute_cmd(
    ctx: typer.Context,
    target: Optional[str] = typer.Argument(None, help="Path to .sql file to execute."),
    timing: bool = typer.Option(False, "--timing", help="Print execution duration."),
) -> None:
    """Execute SQL transforms in DuckDB."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("execute", json_output=cli_ctx.json_output):
        if not target:
            raise RuntimeError("No target specified. Usage: dbp execute <path/to/file.sql>")

        from ...adapters.primary.client import DBPort
        from ...infrastructure.progress import progress_callback

        paths = resolve_model_paths(cli_ctx)

        t0 = time.monotonic()

        with cli_progress(enabled=not cli_ctx.json_output and not cli_ctx.quiet):
            with DBPort(
                agency=paths.agency,
                dataset_id=paths.dataset_id,
                lock_path=paths.lock_path,
                duckdb_path=paths.duckdb_path,
                model_root=paths.model_root,
            ) as port:
                cb = progress_callback.get(None)
                if cb:
                    cb.started(f"Executing {target}")
                port.execute(target)
                if cb:
                    cb.finished(f"Executed {target}")

        elapsed = time.monotonic() - t0

        if cli_ctx.json_output:
            data = {"target": target, "elapsed_seconds": round(elapsed, 3)}
            print_json("execute", data)
        else:
            print_success(f"Executed {target}")
            if timing:
                print_info(f"  Duration: {elapsed:.2f}s")
