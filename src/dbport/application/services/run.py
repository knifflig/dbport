"""RunService — execute the configured run hook for a model."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ...domain.ports.compute import ICompute
from ...domain.ports.lock import ILockStore

logger = logging.getLogger(__name__)

_SUPPORTED_EXTENSIONS = {".sql", ".py"}


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
        hook = self._lock.read_run_hook()
        if not hook:
            hook = "main.py"
            logger.info("No run_hook configured — defaulting to %s", hook)

        self._dispatch(port, hook)

        if version is not None:
            port.publish(version=version, mode=mode)

    def _dispatch(self, port: Any, hook: str) -> None:
        """Route hook execution by file extension."""
        ext = Path(hook).suffix.lower()

        if ext == ".sql":
            port.execute(hook)
        elif ext == ".py":
            self._exec_python(port, hook)
        else:
            raise ValueError(
                f"Unsupported run hook extension '{ext}'. "
                f"Supported: {', '.join(sorted(_SUPPORTED_EXTENSIONS))}"
            )

    def _exec_python(self, port: Any, hook: str) -> None:
        """Execute a Python hook file with ``port`` available in scope."""
        path = Path(hook)
        if not path.is_absolute():
            path = Path(port._dataset.model_root) / path
        logger.info("Executing Python hook: %s", path)
        code = path.read_text(encoding="utf-8")
        exec(compile(code, str(path), "exec"), {"port": port, "__file__": str(path)})  # noqa: S102
