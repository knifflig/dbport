"""CLI error handling — consistent exit codes and messages."""

from __future__ import annotations

import sys
from collections.abc import Generator
from contextlib import contextmanager

from . import render


@contextmanager
def cli_error_handler(command: str, *, json_output: bool = False) -> Generator[None]:
    """Catch known exceptions and render them as CLI errors."""
    try:
        yield
    except SystemExit:
        raise
    except KeyboardInterrupt:
        if json_output:
            render.print_json(command, {"error": "interrupted"}, ok=False)
        else:
            render.print_error("Interrupted.")
        sys.exit(130)
    except RuntimeError as exc:
        if json_output:
            render.print_json(command, {"error": str(exc)}, ok=False)
        else:
            render.print_error(str(exc))
        sys.exit(1)
    except FileNotFoundError as exc:
        if json_output:
            render.print_json(command, {"error": str(exc)}, ok=False)
        else:
            render.print_error(str(exc))
        sys.exit(1)
    except Exception as exc:
        if json_output:
            render.print_json(command, {"error": str(exc)}, ok=False)
        else:
            render.print_error(f"Unexpected error: {exc}")
        sys.exit(1)
