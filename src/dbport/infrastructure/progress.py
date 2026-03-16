"""Progress callback infrastructure — contextvars-based progress reporting.

Adapters read the context variable and emit progress events when set.
CLI layer sets it; library users and tests leave it as None (no-op).
"""

from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import Protocol, runtime_checkable


@runtime_checkable
class ProgressCallback(Protocol):
    """Callback protocol for reporting operation progress from adapters."""

    def started(self, description: str, total: int | None = None) -> None:
        """Signal that an operation has started.

        *total* is the expected number of rows. ``None`` means indeterminate
        (spinner mode — elapsed time only, no ETA).
        """
        ...

    def update(self, advance: int) -> None:
        """Advance the progress indicator by *advance* rows."""
        ...

    def log(self, message: str) -> None:
        """Emit a status message (rendered above progress bars in CLI)."""
        ...

    def finished(self, message: str | None = None) -> None:
        """Signal that the operation has completed."""
        ...


progress_callback: contextvars.ContextVar[ProgressCallback | None] = contextvars.ContextVar(
    "progress_callback", default=None
)


@contextmanager
def progress_phase(key: str, title: str, icon: str):
    """Route nested progress events into a named phase when supported.

    Tree-based CLI progress nodes can expose a ``phase(...)`` context manager.
    Other callbacks ignore phases and continue as a flat progress stream.
    """
    callback = progress_callback.get(None)
    phase_factory = getattr(callback, "phase", None)
    if callable(phase_factory):
        with phase_factory(key, title=title, icon=icon):
            yield
        return

    yield
