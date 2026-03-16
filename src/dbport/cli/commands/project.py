"""dbp project — run lifecycle commands across all models."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import typer

from ..context import read_lock_models
from ..errors import cli_error_handler
from ..render import cli_tree_progress, print_error, print_info, print_json, print_success
from . import lifecycle

project_app = typer.Typer(
    name="project",
    help="Run lifecycle operations across all models in the project.",
    no_args_is_help=True,
)


def _project_models(cli_ctx) -> dict:
    models = read_lock_models(cli_ctx.lockfile_path)
    if not models:
        raise RuntimeError(
            f"No models found in {cli_ctx.lockfile_path}. Run 'dbp init <name>' to create a model first."
        )
    return models


def _project_batches(models: dict) -> list[list[tuple[int, str, dict]]]:
    indexed_models = list(enumerate(models.items()))
    model_keys = set(models)
    dependencies: dict[str, set[str]] = {}
    dependents: dict[str, set[str]] = defaultdict(set)

    for model_key, model_data in models.items():
        internal_inputs = {
            item.get("table_address")
            for item in model_data.get("inputs", [])
            if item.get("table_address") in model_keys and item.get("table_address") != model_key
        }
        dependencies[model_key] = internal_inputs
        for dependency in internal_inputs:
            dependents[dependency].add(model_key)

    available = [model_key for _, (model_key, _) in indexed_models if not dependencies[model_key]]
    processed: set[str] = set()
    batches: list[list[tuple[int, str, dict]]] = []

    while available:
        batch = [
            (index, model_key, model_data)
            for index, (model_key, model_data) in indexed_models
            if model_key in available
        ]
        batches.append(batch)

        next_available: list[str] = []
        for _, model_key, _ in batch:
            processed.add(model_key)
            for dependent in dependents.get(model_key, set()):
                dependencies[dependent].discard(model_key)
                if not dependencies[dependent]:
                    next_available.append(dependent)
        available = next_available

    if len(processed) != len(models):
        unresolved = [key for key in models if key not in processed]
        raise RuntimeError(
            "Cyclic model input dependencies found in dbport.lock: " + ", ".join(unresolved)
        )

    return batches


def _run_for_all(cli_ctx, *, title: str, action, command_name: str) -> list[dict]:
    models = _project_models(cli_ctx)
    results_by_index: dict[int, dict] = {}
    failures_by_index: dict[int, tuple[str, str]] = {}
    indexed_models = list(enumerate(models.items()))
    batches = _project_batches(models)

    with cli_tree_progress(
        enabled=not cli_ctx.json_output and not cli_ctx.quiet,
        title=title,
    ) as model_ctx:

        def _worker(index: int, model_key: str, model_data: dict) -> tuple[int, dict]:
            with model_ctx(model_key):
                return index, action(model_key, model_data)

        for batch in batches:
            max_workers = min(4, len(batch)) or 1
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(_worker, index, model_key, model_data): (index, model_key)
                    for index, model_key, model_data in batch
                }
                for future in as_completed(futures):
                    index, model_key = futures[future]
                    try:
                        result_index, result = future.result()
                        results_by_index[result_index] = result
                    except Exception as exc:
                        failures_by_index[index] = (model_key, str(exc))

            if failures_by_index:
                break

    results = [results_by_index[index] for index, _ in indexed_models if index in results_by_index]
    failures = [
        failures_by_index[index] for index, _ in indexed_models if index in failures_by_index
    ]

    if cli_ctx.json_output:
        print_json(
            command_name,
            {"results": results, "failed": failures, "total": len(models)},
            ok=not failures,
        )
    else:
        if failures:
            for model_key, detail in failures:
                print_error(f"{model_key}: {detail}")
            raise typer.Exit(1)

    return results


@project_app.command("sync")
def project_sync_cmd(ctx: typer.Context) -> None:
    """Sync all models in the project."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    with cli_error_handler("project sync", json_output=cli_ctx.json_output):
        results = _run_for_all(
            cli_ctx,
            title="Syncing models",
            action=lambda _model_key, model_data: lifecycle.sync_model(cli_ctx, model_data),
            command_name="project sync",
        )
        if not cli_ctx.json_output:
            print_success(f"Synced {len(results)} model(s)")


@project_app.command("load")
def project_load_cmd(
    ctx: typer.Context,
    update: bool = typer.Option(
        False,
        "--update",
        help="Resolve the newest available snapshot for each configured input.",
    ),
) -> None:
    """Load configured inputs for all models in the project."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    with cli_error_handler("project load", json_output=cli_ctx.json_output):
        results = _run_for_all(
            cli_ctx,
            title="Loading models",
            action=lambda model_key, model_data: lifecycle.load_model(
                cli_ctx,
                model_key,
                model_data,
                update=update,
            ),
            command_name="project load",
        )
        if not cli_ctx.json_output:
            loaded_models = sum(1 for item in results if item["loaded"])
            action = "Updated" if update else "Loaded"
            print_success(f"{action} inputs for {loaded_models} model(s)")


@project_app.command("exec")
def project_exec_cmd(
    ctx: typer.Context,
    target: str | None = typer.Option(
        None,
        "--target",
        help="Execute this .sql or .py file instead of each model's configured hook.",
    ),
) -> None:
    """Execute the configured hook, or an override target, for all models."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    with cli_error_handler("project exec", json_output=cli_ctx.json_output):
        results = _run_for_all(
            cli_ctx,
            title="Executing models",
            action=lambda model_key, model_data: lifecycle.exec_model(
                cli_ctx,
                model_key,
                model_data,
                target=target,
            ),
            command_name="project exec",
        )
        if not cli_ctx.json_output:
            print_success(f"Executed {len(results)} model(s)")


@project_app.command("publish")
def project_publish_cmd(
    ctx: typer.Context,
    version: str | None = typer.Option(
        None, "--version", help="Version identifier for this publish."
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate only; do not write data."),
    refresh: bool = typer.Option(False, "--refresh", help="Overwrite existing version."),
    message: str | None = typer.Option(None, "--message", "-m", help="Publish note for history."),
) -> None:
    """Publish all models in the project."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    with cli_error_handler("project publish", json_output=cli_ctx.json_output):
        results = _run_for_all(
            cli_ctx,
            title="Publishing models",
            action=lambda model_key, model_data: lifecycle.publish_model(
                cli_ctx,
                model_key,
                model_data,
                version=version,
                dry_run=dry_run,
                refresh=refresh,
            ),
            command_name="project publish",
        )
        if not cli_ctx.json_output:
            if results and results[0]["mode"] == "dry":
                print_success(f"Dry run completed for {len(results)} model(s)")
            else:
                print_success(f"Published {len(results)} model(s)")
            if message:
                print_info(f"  Note:  {message}")


@project_app.command("run")
def project_run_cmd(
    ctx: typer.Context,
    version: str | None = typer.Option(
        None, "--version", help="Version to publish after execution."
    ),
    target: str | None = typer.Option(
        None,
        "--target",
        help="Execute this .sql or .py file instead of each model's configured hook.",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate only; do not publish."),
    refresh: bool = typer.Option(False, "--refresh", help="Overwrite existing version."),
) -> None:
    """Run sync, hook execution, and publish for all models."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)
    with cli_error_handler("project run", json_output=cli_ctx.json_output):
        results = _run_for_all(
            cli_ctx,
            title="Running models",
            action=lambda model_key, model_data: lifecycle.run_model(
                cli_ctx,
                model_key,
                model_data,
                version=version,
                target=target,
                dry_run=dry_run,
                refresh=refresh,
            ),
            command_name="project run",
        )
        if not cli_ctx.json_output:
            print_success(f"Ran {len(results)} model(s)")
