"""Shared lifecycle helpers for model and project CLI commands."""

from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...adapters.primary.client import DBPort
    from ..context import CliContext

from ..context import read_lock_versions, resolve_model_paths_from_data


@contextmanager
def _phase(key: str, *, title: str, icon: str) -> Generator[None]:
    from ...infrastructure.progress import progress_phase

    with progress_phase(key, title=title, icon=icon):
        yield


def _run_execute_step(port: DBPort, target: str) -> None:
    from ...application.services.run import execute_hook
    from ...infrastructure.progress import progress_callback

    callback = progress_callback.get(None)
    if callback is not None:
        callback.started(f"Executing {target}")
    try:
        execute_hook(port, target)
    except Exception:
        failed = getattr(callback, "failed", None)
        if callable(failed):
            failed(f"Failed executing {target}")
        raise
    if callback is not None:
        callback.finished(f"Executed {target}")


def resolve_publish_version(
    cli_ctx: CliContext, model_key: str, explicit_version: str | None
) -> str:
    """Resolve the publish version for ``dbp model run`` (full lifecycle).

    Resolution order:
    1. Explicit ``--version`` CLI flag
    2. Configured ``version`` field in the lock file (set via ``dbp config model … version``)
    3. Latest completed version from the ``versions`` array in the lock file

    This includes the configured version because ``run`` creates a new publish
    from scratch and the configured version represents the intended next publish.
    """
    from ..context import read_lock_version_config

    if explicit_version is not None:
        return explicit_version

    configured = read_lock_version_config(cli_ctx.lockfile_path, model_key)
    if configured is not None:
        return configured

    lock_versions = read_lock_versions(cli_ctx.lockfile_path, model_key)
    completed = [item for item in lock_versions if item.get("completed")]
    if completed:
        return completed[-1]["version"]

    raise RuntimeError(
        f"No version available. Set one with: dbp config model {model_key} version <version>"
    )


def resolve_publish_version_for_publish(
    cli_ctx: CliContext,
    model_key: str,
    explicit_version: str | None,
) -> str:
    """Resolve the publish version for ``dbp model publish`` (standalone).

    Resolution order:
    1. Explicit ``--version`` CLI flag
    2. Latest completed version from the ``versions`` array in the lock file

    Intentionally does NOT fall back to the configured ``version`` field,
    because standalone ``publish`` re-publishes existing output data and
    should default to the most recent completed version rather than a
    configured future version.
    """
    if explicit_version is not None:
        return explicit_version

    lock_versions = read_lock_versions(cli_ctx.lockfile_path, model_key)
    completed = [item for item in lock_versions if item.get("completed")]
    if completed:
        return completed[-1]["version"]

    raise RuntimeError("No completed versions found in lock file. Specify --version explicitly.")


def resolve_publish_mode(*, dry_run: bool, refresh: bool) -> str | None:
    """Map CLI publish flags to the DBPort publish mode."""
    if dry_run:
        return "dry"
    if refresh:
        return "refresh"
    return None


def sync_model(cli_ctx: CliContext, model_data: dict) -> dict:
    """Sync one model by opening DBPort, which runs sync in __init__."""
    from ...adapters.primary.client import DBPort

    paths = resolve_model_paths_from_data(cli_ctx, model_data)
    with DBPort(
        agency=paths.agency,
        dataset_id=paths.dataset_id,
        lock_path=paths.lock_path,
        duckdb_path=paths.duckdb_path,
        model_root=paths.model_root,
        load_inputs_on_init=False,
    ):
        pass

    return {
        "agency": paths.agency,
        "dataset_id": paths.dataset_id,
    }


def load_model(
    cli_ctx: CliContext,
    model_key: str,
    model_data: dict,
    *,
    update: bool = False,
) -> dict:
    """Load configured inputs for one model."""
    from ...adapters.primary.client import DBPort

    paths = resolve_model_paths_from_data(cli_ctx, model_data)
    inputs = model_data.get("inputs", [])
    loaded: list[str] = []

    if not inputs:
        return {
            "model": model_key,
            "loaded": loaded,
            "updated": update,
        }

    with DBPort(
        agency=paths.agency,
        dataset_id=paths.dataset_id,
        lock_path=paths.lock_path,
        duckdb_path=paths.duckdb_path,
        model_root=paths.model_root,
        load_inputs_on_init=False,
    ) as port:
        with _phase("load", title="Load", icon="📥"):
            for item in inputs:
                port.load(
                    item["table_address"],
                    filters=item.get("filters"),
                    version=None if update else item.get("version"),
                )
                loaded.append(item["table_address"])

    return {
        "model": model_key,
        "loaded": loaded,
        "updated": update,
    }


def exec_model(
    cli_ctx: CliContext, model_key: str, model_data: dict, *, target: str | None = None
) -> dict:
    """Execute a model hook or an explicit override target."""
    from ...adapters.primary.client import DBPort

    paths = resolve_model_paths_from_data(cli_ctx, model_data)
    started = time.monotonic()

    with DBPort(
        agency=paths.agency,
        dataset_id=paths.dataset_id,
        lock_path=paths.lock_path,
        duckdb_path=paths.duckdb_path,
        model_root=paths.model_root,
    ) as port:
        effective_target = target or port.run_hook
        _run_execute_step(port, effective_target)

    return {
        "model": model_key,
        "target": effective_target,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def publish_model(
    cli_ctx: CliContext,
    model_key: str,
    model_data: dict,
    *,
    version: str | None = None,
    dry_run: bool = False,
    refresh: bool = False,
) -> dict:
    """Publish one model."""
    from ...adapters.primary.client import DBPort

    paths = resolve_model_paths_from_data(cli_ctx, model_data)
    pub_version = resolve_publish_version_for_publish(cli_ctx, model_key, version)
    mode = resolve_publish_mode(dry_run=dry_run, refresh=refresh)

    with DBPort(
        agency=paths.agency,
        dataset_id=paths.dataset_id,
        lock_path=paths.lock_path,
        duckdb_path=paths.duckdb_path,
        model_root=paths.model_root,
    ) as port:
        with _phase("publish", title="Publish", icon="🚀"):
            port.publish(version=pub_version, mode=mode)

    return {
        "model": model_key,
        "version": pub_version,
        "mode": mode,
    }


def run_model(
    cli_ctx: CliContext,
    model_key: str,
    model_data: dict,
    *,
    version: str | None = None,
    target: str | None = None,
    dry_run: bool = False,
    refresh: bool = False,
) -> dict:
    """Run sync, execute hook, and publish for one model."""
    from ...adapters.primary.client import DBPort

    paths = resolve_model_paths_from_data(cli_ctx, model_data)
    pub_version = resolve_publish_version(cli_ctx, model_key, version)
    mode = resolve_publish_mode(dry_run=dry_run, refresh=refresh)
    started = time.monotonic()

    with DBPort(
        agency=paths.agency,
        dataset_id=paths.dataset_id,
        lock_path=paths.lock_path,
        duckdb_path=paths.duckdb_path,
        model_root=paths.model_root,
    ) as port:
        effective_target = target or port.run_hook
        with _phase("exec", title="Exec", icon="⚙️"):
            _run_execute_step(port, effective_target)
        with _phase("publish", title="Publish", icon="🚀"):
            port.publish(version=pub_version, mode=mode)

    return {
        "model": model_key,
        "target": effective_target,
        "version": pub_version,
        "mode": mode,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
