"""Auto-logging setup — called once from DBPort.__init__.

Uses rich for console output when available; falls back to stdlib logging.
Idempotent: safe to call multiple times (use force=True to reconfigure).
"""

from __future__ import annotations

import logging
from typing import Any

_configured = False

_NOISY_LOGGERS = ("pyiceberg", "fsspec", "s3fs", "urllib3", "botocore")


def setup_logging(
    level: int = logging.INFO,
    *,
    console: Any | None = None,
    force: bool = False,
) -> None:
    """Configure the ``dbport`` logger (not root). Idempotent unless *force*.

    Parameters
    ----------
    level:
        Logging level for the ``dbport`` logger hierarchy.
    console:
        Optional Rich Console instance.  When provided the RichHandler shares
        this console (used by the CLI to coordinate with the Progress widget).
    force:
        When True, reconfigure even if already set up.  The CLI uses this to
        apply ``--verbose`` / ``--quiet`` after the library's initial call.
    """
    global _configured
    if _configured and not force:
        return

    dbport_logger = logging.getLogger("dbport")
    dbport_logger.handlers.clear()
    dbport_logger.setLevel(level)

    try:
        from rich.logging import RichHandler

        if console is not None:
            handler = RichHandler(
                rich_tracebacks=True, show_path=False, console=console
            )
        else:
            handler = RichHandler(rich_tracebacks=True, show_path=False)
    except ImportError:
        handler = logging.StreamHandler()  # type: ignore[assignment]
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s %(name)s  %(message)s",
                datefmt="%H:%M:%S",
            )
        )

    dbport_logger.addHandler(handler)

    # Suppress noisy third-party loggers
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)

    _configured = True
