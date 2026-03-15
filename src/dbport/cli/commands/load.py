"""dbp load — load inputs into DuckDB."""

from __future__ import annotations

from typing import Optional

import typer

from ..context import _resolve_model_data, read_lock_models, resolve_model_paths
from ..errors import cli_error_handler
from ..render import cli_progress, print_info, print_json, print_success, print_warning


def load_cmd(
    ctx: typer.Context,
    dataset: Optional[str] = typer.Argument(None, help="Table address to load (e.g. estat.table_name)."),
    refresh: bool = typer.Option(False, "--refresh", help="Force re-load even if snapshot unchanged."),
) -> None:
    """Load configured or explicit inputs into DuckDB."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("load", json_output=cli_ctx.json_output):
        from ...adapters.primary.client import DBPort

        paths = resolve_model_paths(cli_ctx)

        with DBPort(
            agency=paths.agency,
            dataset_id=paths.dataset_id,
            lock_path=paths.lock_path,
            duckdb_path=paths.duckdb_path,
            model_root=paths.model_root,
        ) as port:
            if dataset:
                # Load a single explicit input
                with cli_progress(enabled=not cli_ctx.json_output and not cli_ctx.quiet):
                    port.load(dataset)
                if cli_ctx.json_output:
                    print_json("load", {"loaded": [dataset]})
                else:
                    print_success(f"Loaded {dataset}")
            else:
                # Load all configured inputs from lock
                models = read_lock_models(cli_ctx.lockfile_path)
                if not models:
                    raise RuntimeError("No models in dbport.lock.")

                model_data = _resolve_model_data(cli_ctx, models)
                inputs = model_data.get("inputs", [])

                if not inputs:
                    if cli_ctx.json_output:
                        print_json("load", {"loaded": []})
                    else:
                        print_warning("No inputs configured in dbport.lock. Specify a dataset to load.")
                    return

                loaded = []
                with cli_progress(enabled=not cli_ctx.json_output and not cli_ctx.quiet):
                    for inp in inputs:
                        addr = inp["table_address"]
                        filters = inp.get("filters")
                        version = inp.get("version")
                        port.load(addr, filters=filters, version=version)
                        loaded.append(addr)

                if cli_ctx.json_output:
                    print_json("load", {"loaded": loaded})
                else:
                    print_success(f"Loaded {len(loaded)} input(s): {', '.join(loaded)}")
