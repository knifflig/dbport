"""CLI output helpers — Rich for humans, JSON for machines."""

from __future__ import annotations

import json
import sys
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Module-level console; replaced in tests or when --no-color is set.
_console = Console(stderr=True)
_stdout = Console()


def get_console() -> Console:
    return _console


def get_stdout() -> Console:
    return _stdout


def set_no_color(no_color: bool) -> None:
    global _console, _stdout
    if no_color:
        _console = Console(stderr=True, no_color=True, highlight=False)
        _stdout = Console(no_color=True, highlight=False)


def configure_cli_logging(*, verbose: bool, quiet: bool) -> None:
    """Reconfigure the ``dbport`` logger for the CLI verbosity flags."""
    import logging as _logging

    from ..infrastructure.logging import setup_logging

    if quiet:
        level = _logging.ERROR
    elif verbose:
        level = _logging.DEBUG
    else:
        level = _logging.WARNING
    setup_logging(level=level, console=_console, force=True)


# -- JSON output -------------------------------------------------------------

def print_json(command: str, data: Any, *, ok: bool = True) -> None:
    payload = {"ok": ok, "command": command, "data": data}
    sys.stdout.write(json.dumps(payload, indent=2, default=str) + "\n")


# -- Human-readable output ---------------------------------------------------

def print_success(msg: str) -> None:
    _stdout.print(f"[green bold]OK[/] {msg}")


def print_error(msg: str) -> None:
    _console.print(f"[red bold]Error:[/] {msg}")


def print_warning(msg: str) -> None:
    _console.print(f"[yellow bold]Warning:[/] {msg}")


def print_info(msg: str) -> None:
    _stdout.print(msg)


def print_table(title: str, columns: list[str], rows: list[list[str]]) -> None:
    table = Table(title=title, show_header=True)
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*row)
    _stdout.print(table)


def print_panel(title: str, content: str) -> None:
    _stdout.print(Panel(content, title=title, expand=False))


# -- Progress helpers --------------------------------------------------------

from contextlib import contextmanager
from typing import Generator

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.text import Text


class _ConditionalColumn(ProgressColumn):
    """Wraps another column and renders it only for determinate tasks.

    Uses ``task.fields.get("determinate")`` to distinguish from indeterminate
    tasks that were marked complete with a synthetic ``total=1``.
    """

    def __init__(self, inner: ProgressColumn) -> None:
        super().__init__()
        self._inner = inner

    def render(self, task: Task) -> Text:  # type: ignore[override]
        if not task.fields.get("determinate"):
            return Text("")
        return self._inner.render(task)  # type: ignore[return-value]


class _ConditionalTextColumn(ProgressColumn):
    """Renders static text only for determinate tasks."""

    def __init__(self, label: str) -> None:
        super().__init__()
        self._label = label

    def render(self, task: Task) -> Text:
        if not task.fields.get("determinate"):
            return Text("")
        return Text(self._label)


class _SpinnerOrCheckColumn(ProgressColumn):
    """Shows a spinner while running, a green checkmark or red cross when finished."""

    def __init__(self) -> None:
        super().__init__()
        self._spinner = SpinnerColumn()

    def render(self, task: Task) -> Text:
        if task.finished:
            if task.fields.get("failed"):
                return Text("✗", style="red bold")
            return Text("✓", style="green bold")
        return self._spinner.render(task)  # type: ignore[return-value]


class RichProgressAdapter:
    """Implements :class:`ProgressCallback` by wrapping Rich's ``Progress`` widget."""

    def __init__(self, progress: Progress) -> None:
        self._progress = progress
        self._task_id: int | None = None

    def started(self, description: str, total: int | None = None) -> None:
        # Finish any previous task that was not explicitly finished
        if self._task_id is not None:
            self.finished()
        self._task_id = self._progress.add_task(
            description, total=total, determinate=(total is not None)
        )

    def update(self, advance: int) -> None:
        if self._task_id is not None:
            self._progress.update(self._task_id, advance=advance)

    def log(self, message: str) -> None:
        self._progress.console.print(message)

    def failed(self, message: str | None = None) -> None:
        """Mark the current task as failed (red ✗ in the progress display)."""
        if self._task_id is None:
            return
        if message:
            self._progress.update(self._task_id, description=message)
        self._progress.update(
            self._task_id, total=1, completed=1, failed=True
        )
        self._task_id = None

    def finished(self, message: str | None = None) -> None:
        if self._task_id is None:
            return
        task = self._progress.tasks[self._task_id]
        if message:
            self._progress.update(self._task_id, description=message)
        if task.total is not None:
            self._progress.update(self._task_id, completed=task.total)
        else:
            # Mark indeterminate tasks as "done" so the spinner stops
            self._progress.update(self._task_id, total=1, completed=1)
        self._task_id = None


@contextmanager
def cli_progress(*, enabled: bool = True) -> Generator[None, None, None]:
    """Context manager that wires Rich progress to the progress contextvar.

    When *enabled* is ``False`` (JSON mode), yields a no-op context — the
    contextvar stays ``None`` and adapters skip all progress reporting.
    """
    if not enabled:
        yield
        return

    from ..infrastructure.progress import progress_callback

    progress = Progress(
        _SpinnerOrCheckColumn(),
        TextColumn("[bold blue]{task.description}"),
        _ConditionalColumn(BarColumn()),
        _ConditionalColumn(MofNCompleteColumn()),
        _ConditionalTextColumn("rows"),
        TimeElapsedColumn(),
        _ConditionalColumn(TimeRemainingColumn()),
        console=_console,
        transient=False,
    )
    adapter = RichProgressAdapter(progress)
    token = progress_callback.set(adapter)
    try:
        with progress:
            yield
    finally:
        progress_callback.reset(token)
