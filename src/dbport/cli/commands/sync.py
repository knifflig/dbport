"""dbp sync — sync existing models from the lock file / catalog."""

from __future__ import annotations

import typer

from ..context import read_lock_models, resolve_model_paths_from_data
from ..errors import cli_error_handler
from ..render import (
    cli_tree_progress,
    print_error,
    print_json,
    print_success,
)


def sync_cmd(
    ctx: typer.Context,
    model: str | None = typer.Argument(None, help="Model key (agency.dataset_id) to sync."),
) -> None:
    """Sync one or all models from the lock file.

    Without arguments: syncs all models in the lock file.
    With a model key: syncs that specific model.
    """
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("sync", json_output=cli_ctx.json_output):
        models = read_lock_models(cli_ctx.lockfile_path)

        if model is not None:
            # Sync a single model
            if model not in models:
                print_error(
                    f"Model '{model}' not found in {cli_ctx.lockfile_path}. "
                    f"Available: {list(models.keys())}"
                )
                raise typer.Exit(1)
            _sync_single_model(cli_ctx, model, models[model])
        else:
            # Sync all models
            _sync_all_models(cli_ctx, models)


def _do_sync(cli_ctx, model_key: str, model_data: dict) -> None:
    """Create a DBPort instance for the model, triggering sync in __init__."""
    from ...adapters.primary.client import DBPort

    paths = resolve_model_paths_from_data(cli_ctx, model_data)
    with DBPort(
        agency=paths.agency,
        dataset_id=paths.dataset_id,
        lock_path=paths.lock_path,
        duckdb_path=paths.duckdb_path,
        model_root=paths.model_root,
    ):
        pass  # sync happens in __init__


def _sync_single_model(cli_ctx, model_key: str, model_data: dict) -> None:
    """Sync a single existing model with tree progress display."""
    with cli_tree_progress(
        enabled=not cli_ctx.json_output,
        title="Syncing",
    ) as model_ctx:
        with model_ctx(model_key):
            _do_sync(cli_ctx, model_key, model_data)

    if cli_ctx.json_output:
        print_json("sync", {"synced": [model_key], "total": 1})
    else:
        print_success(f"Synced {model_key}")


def _sync_all_models(cli_ctx, models: dict) -> None:
    """Sync all models in the lock file, running in parallel with tree progress."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not models:
        print_error(
            f"No models found in {cli_ctx.lockfile_path}. "
            "Run 'dbp init <name>' to create a model first."
        )
        raise typer.Exit(1)

    synced: list[str] = []
    failed: list[str] = []

    with cli_tree_progress(
        enabled=not cli_ctx.json_output,
        title="Syncing models",
    ) as model_ctx:
        def _sync_worker(model_key: str, model_data: dict) -> str:
            with model_ctx(model_key):
                _do_sync(cli_ctx, model_key, model_data)
            return model_key

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {
                pool.submit(_sync_worker, key, data): key
                for key, data in models.items()
            }
            for fut in as_completed(futures):
                key = futures[fut]
                try:
                    fut.result()
                    synced.append(key)
                except Exception:
                    failed.append(key)

    if cli_ctx.json_output:
        print_json("sync", {"synced": synced, "total": len(models)})
    else:
        print_success(f"Synced {len(synced)}/{len(models)} model(s)")
