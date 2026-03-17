"""CLI error handling — consistent exit codes and messages.

Exit code contract:
    0   — success
    1   — user/validation error (bad input, missing config, schema drift)
    2   — internal/unexpected error
    130 — interrupted (Ctrl+C)
"""

from __future__ import annotations

import sys
from collections.abc import Generator
from contextlib import contextmanager

from . import render

# Exit codes
EXIT_SUCCESS = 0
EXIT_USER_ERROR = 1
EXIT_INTERNAL_ERROR = 2
EXIT_INTERRUPTED = 130


class CliUserError(Exception):
    """Raised for user-facing validation failures that should exit with code 1."""


@contextmanager
def cli_error_handler(command: str, *, json_output: bool = False) -> Generator[None]:
    """Catch known exceptions and render them as CLI errors.

    Exit codes:
        1   — user/validation errors (RuntimeError, FileNotFoundError, CliUserError)
        2   — unexpected/internal errors (everything else)
        130 — keyboard interrupt
    """
    try:
        yield
    except SystemExit:
        raise
    except KeyboardInterrupt:
        _emit(command, "interrupted", "interrupted", EXIT_INTERRUPTED, json_output=json_output)
    except CliUserError as exc:
        _emit(command, str(exc), "validation_error", EXIT_USER_ERROR, json_output=json_output)
    except RuntimeError as exc:
        _emit(command, str(exc), "runtime_error", EXIT_USER_ERROR, json_output=json_output)
    except FileNotFoundError as exc:
        _emit(command, str(exc), "file_not_found", EXIT_USER_ERROR, json_output=json_output)
    except Exception as exc:
        _emit(
            command,
            f"Unexpected error: {exc}",
            "internal_error",
            EXIT_INTERNAL_ERROR,
            json_output=json_output,
        )


def _emit(
    command: str,
    message: str,
    error_type: str,
    exit_code: int,
    *,
    json_output: bool,
) -> None:
    """Render an error and exit with the given code."""
    if json_output:
        render.print_json(
            command,
            {"error": message, "error_type": error_type},
            ok=False,
        )
    else:
        render.print_error(message)
    sys.exit(exit_code)
