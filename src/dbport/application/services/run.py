"""RunService — execute the configured run hook for a model."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ...domain.ports.compute import ICompute
from ...domain.ports.lock import ILockStore

logger = logging.getLogger(__name__)

_SUPPORTED_EXTENSIONS = {".sql", ".py"}
DEFAULT_RUN_HOOK = "main.py"
LEGACY_SQL_RUN_HOOK = "sql/main.sql"


def resolve_run_hook(lock: ILockStore, model_root: str | None = None) -> str:
    """Return the configured run hook or the best available default hook."""
    configured = lock.read_run_hook()
    if configured:
        return configured

    if model_root is not None:
        root = Path(model_root)
        if (root / DEFAULT_RUN_HOOK).exists():
            return DEFAULT_RUN_HOOK
        if (root / LEGACY_SQL_RUN_HOOK).exists():
            return LEGACY_SQL_RUN_HOOK

    return DEFAULT_RUN_HOOK


def execute_hook(port: Any, hook: str) -> None:
    """Execute a hook file against the active DBPort instance."""
    ext = Path(hook).suffix.lower()

    if ext == ".sql":
        port.execute(hook)
        return
    if ext == ".py":
        _exec_python_hook(port, hook)
        return

    raise ValueError(
        f"Unsupported run hook extension '{ext}'. "
        f"Supported: {', '.join(sorted(_SUPPORTED_EXTENSIONS))}"
    )


def _exec_python_hook(port: Any, hook: str) -> None:
    """Execute a Python hook file with ``port`` available in scope.

    If the hook defines a top-level ``run(port)`` callable, invoke it after
    module execution. ``__name__`` is set to ``"__dbport_hook__"`` so that
    standalone ``if __name__ == "__main__"`` blocks are skipped during CLI
    execution while still working when the file is run directly.
    """
    path = Path(hook)
    if not path.is_absolute():
        path = Path(port._dataset.model_root) / path
    logger.info("Executing Python hook: %s", path)
    code = path.read_text(encoding="utf-8")
    namespace: dict[str, Any] = {
        "port": port,
        "__file__": str(path),
        "__name__": "__dbport_hook__",
    }
    exec(compile(code, str(path), "exec"), namespace)  # noqa: S102

    hook_runner = namespace.get("run")
    if callable(hook_runner):
        hook_runner(port)


class RunService:
    """Dispatch execution of a run hook by file extension.

    Supported hook types:
    - ``.sql`` — delegated to ``port.execute()``
    - ``.py``  — executed via ``exec()`` with the active ``port`` in scope

    The dispatcher is intentionally extensible: future hook types (e.g. dbt,
    sqlmesh) can be added as additional branches in ``_dispatch``.
    """

    def __init__(self, compute: ICompute, lock: ILockStore) -> None:
        self._compute = compute
        self._lock = lock

    def execute(
        self,
        port: Any,
        *,
        version: str | None = None,
        mode: str | None = None,
    ) -> None:
        """Run the hook and optionally publish.

        Args:
            port: The active ``DBPort`` instance (typed as Any to avoid
                  circular imports between application and adapter layers).
            version: If provided, ``port.publish()`` is called after the hook.
            mode: Publish mode forwarded to ``port.publish()``.
        """
        hook = resolve_run_hook(self._lock, getattr(port._dataset, "model_root", None))

        self._dispatch(port, hook)

        if version is not None:
            port.publish(version=version, mode=mode)

    def _dispatch(self, port: Any, hook: str) -> None:
        """Route hook execution by file extension."""
        execute_hook(port, hook)
