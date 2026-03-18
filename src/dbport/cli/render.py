"""CLI output helpers — Rich for humans, JSON for machines."""

from __future__ import annotations

import json
import sys
import threading
import time as _time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
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
from rich.spinner import Spinner
from rich.table import Table
from rich.table import Table as RichTable
from rich.text import Text
from rich.tree import Tree

if TYPE_CHECKING:
    from rich.console import ConsoleOptions, RenderResult

# Module-level console; replaced in tests or when --no-color is set.
_console = Console(stderr=True)
_stdout = Console()


def get_console() -> Console:
    """Return the stderr console used for logging and progress."""
    return _console


def get_stdout() -> Console:
    """Return the stdout console used for primary output."""
    return _stdout


def set_no_color(no_color: bool) -> None:
    """Disable color output on both consoles."""
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


def print_json(command: str, data: dict | list | str | None, *, ok: bool = True) -> None:
    """Write a JSON response to stdout."""
    payload: dict[str, object] = {"ok": ok, "command": command, "data": data}
    sys.stdout.write(json.dumps(payload, indent=2, default=str) + "\n")


# -- Human-readable output ---------------------------------------------------


def print_success(msg: str) -> None:
    """Print a success message to stdout."""
    _stdout.print(f"[green bold]OK[/] {msg}")


def print_error(msg: str) -> None:
    """Print an error message to stderr."""
    _console.print(f"[red bold]Error:[/] {msg}")


def print_warning(msg: str) -> None:
    """Print a warning message to stderr."""
    _console.print(f"[yellow bold]Warning:[/] {msg}")


def print_info(msg: str) -> None:
    """Print an informational message to stdout."""
    _stdout.print(msg)


def print_table(title: str, columns: list[str], rows: list[list[str]]) -> None:
    """Print a Rich table to stdout."""
    table = Table(title=title, show_header=True)
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*row)
    _stdout.print(table)


def print_panel(title: str, content: str) -> None:
    """Print a Rich panel to stdout."""
    _stdout.print(Panel(content, title=title, expand=False))


# -- Progress helpers --------------------------------------------------------


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
        """Start tracking a new progress task."""
        # Finish any previous task that was not explicitly finished
        if self._task_id is not None:
            self.finished()
        self._task_id = self._progress.add_task(
            description, total=total, determinate=(total is not None)
        )

    def update(self, advance: int) -> None:
        """Advance the progress indicator by the given amount."""
        if self._task_id is not None:
            self._progress.update(self._task_id, advance=advance)

    def log(self, message: str) -> None:
        """Print a log message above the progress bar."""
        self._progress.console.print(message)

    def failed(self, message: str | None = None) -> None:
        """Mark the current task as failed (red ✗ in the progress display)."""
        if self._task_id is None:
            return
        if message:
            self._progress.update(self._task_id, description=message)
        self._progress.update(self._task_id, total=1, completed=1, failed=True)
        self._task_id = None

    def finished(self, message: str | None = None) -> None:
        """Mark the current task as completed."""
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
def cli_progress(*, enabled: bool = True) -> Generator[None]:
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


# -- Tree progress helpers ---------------------------------------------------


def _render_bar(fraction: float, width: int = 20) -> str:
    """Render a text-based progress bar using Rich markup."""
    filled = int(fraction * width)
    return f"[green]{'━' * filled}[/][dim]{'━' * (width - filled)}[/]"


class _LiveSpinnerLabel:
    """Renderable: animated spinner + description + live elapsed time.

    Recomputes elapsed time on every Rich render cycle so the counter
    ticks up alongside the animated spinner.
    """

    def __init__(self, text: str, style: str = "", start: float | None = None) -> None:
        self._text = text
        self._style = style
        self._spinner = Spinner("dots")
        self._start = start if start is not None else _time.monotonic()

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        grid = RichTable.grid(padding=(0, 1))
        elapsed = _fmt_elapsed(_time.monotonic() - self._start)
        grid.add_row(
            self._spinner,
            Text(self._text, style=self._style),
            Text(elapsed, style="dim"),
        )
        yield grid


class _LiveProgressLabel:
    """Renderable: spinner + description + progress bar + row counts + live elapsed + ETA.

    Used for determinate progress steps (streaming Arrow publish) where
    row counts and a progress bar are shown alongside an animated spinner.
    """

    def __init__(
        self,
        text: str,
        *,
        bar: str | None = None,
        completed: int = 0,
        total: int | None = None,
        start: float | None = None,
        eta: str = "",
    ) -> None:
        self._text = text
        self._bar = bar
        self._completed = completed
        self._total = total
        self._spinner = Spinner("dots")
        self._start = start if start is not None else _time.monotonic()
        self._eta = eta

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        from rich.markup import render as render_markup

        elapsed = _fmt_elapsed(_time.monotonic() - self._start)
        if self._total and self._bar:
            label = render_markup(
                f"{self._text}  {self._bar} "
                f"{self._completed:,} / {self._total:,} rows"
                f"  [dim]{elapsed}{self._eta}[/]"
            )
        else:
            label = render_markup(f"{self._text}  [dim]{self._completed:,} rows  {elapsed}[/]")
        grid = RichTable.grid(padding=(0, 1))
        grid.add_row(self._spinner, label)
        yield grid


def _fmt_elapsed(seconds: float) -> str:
    """Format elapsed seconds as H:MM:SS or M:SS."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


class _TreeProgressNode:
    """Shared tree progress behavior for model and phase branches."""

    def __init__(
        self,
        *,
        branch: Tree,
        root_tree: Tree,
        live: Live,
        lock: threading.Lock,
        on_fail: Callable[[], None] | None = None,
    ) -> None:
        self._branch = branch
        self._root_tree = root_tree
        self._live = live
        self._lock = lock
        self._on_fail = on_fail
        self._current_node: Tree | None = None
        self._current_desc: str = ""
        self._step_start: float = 0.0
        self._total: int | None = None
        self._completed: int = 0

    def started(self, description: str, total: int | None = None) -> None:
        with self._lock:
            if self._current_node is not None:
                self._finish_current_node()
            self._current_desc = description
            self._step_start = _time.monotonic()
            self._total = total
            self._completed = 0
            label = _LiveSpinnerLabel(description, start=self._step_start)
            self._current_node = self._branch.add(label)
            self._live.update(self._root_tree)

    def update(self, advance: int) -> None:
        with self._lock:
            if self._current_node is None:
                return
            self._completed += advance
            elapsed = _time.monotonic() - self._step_start
            if self._total and self._total > 0:
                pct = self._completed / self._total
                bar = _render_bar(pct)
                eta = ""
                if pct > 0.01:
                    remaining = elapsed / pct * (1 - pct)
                    eta = f"  ETA {_fmt_elapsed(remaining)}"
                self._current_node.label = _LiveProgressLabel(
                    self._current_desc,
                    bar=bar,
                    completed=self._completed,
                    total=self._total,
                    start=self._step_start,
                    eta=eta,
                )
            else:
                self._current_node.label = _LiveProgressLabel(
                    self._current_desc,
                    completed=self._completed,
                    start=self._step_start,
                )
            self._live.update(self._root_tree)

    def log(self, message: str) -> None:
        with self._lock:
            self._branch.add(f"[dim]{message}[/dim]")
            self._live.update(self._root_tree)

    def finished(self, message: str | None = None) -> None:
        with self._lock:
            if self._current_node is not None:
                self._finish_current_node(message)
                self._live.update(self._root_tree)

    def failed(self, message: str | None = None) -> None:
        with self._lock:
            if self._current_node is not None:
                elapsed = _time.monotonic() - self._step_start
                desc = message or self._current_desc
                self._current_node.label = f"[red bold]✗[/] {desc}  [dim]{_fmt_elapsed(elapsed)}[/]"
                self._current_node = None
            self._total = None
            self._completed = 0
            if self._on_fail is not None:
                self._on_fail()
            self._live.update(self._root_tree)

    def _finish_current_node(self, message: str | None = None) -> None:
        elapsed = _time.monotonic() - self._step_start
        desc = message or self._current_desc
        self._current_node.label = f"[green bold]✓[/] {desc}  [dim]{_fmt_elapsed(elapsed)}[/]"  # type: ignore[union-attr]
        self._current_node = None
        self._total = None
        self._completed = 0


class ModelNode(_TreeProgressNode):
    """Per-model progress callback that renders steps as children of a Rich Tree branch.

    Thread-safe: all tree mutations are guarded by *lock* and flushed via *live*.
    """

    def __init__(
        self,
        branch: Tree,
        root_tree: Tree,
        live: Live,
        lock: threading.Lock,
        model_key: str,
    ) -> None:
        super().__init__(
            branch=branch, root_tree=root_tree, live=live, lock=lock, on_fail=self._mark_failed
        )
        self._model_key = model_key
        self._model_start: float = _time.monotonic()
        self._model_failed = False
        self._phases: dict[str, _PhaseNode] = {}
        self._phase_order: list[str] = []

    @contextmanager
    def phase(self, key: str, *, title: str, icon: str) -> Generator[_PhaseNode]:
        """Create a scoped phase under this model's progress tree."""
        phase = self._ensure_phase(key, title=title, icon=icon)
        with _model_progress_context(phase):
            error: Exception | None = None
            phase.start()
            try:
                yield phase
            except Exception as exc:  # pragma: no cover — tested via integration
                error = exc
                raise
            finally:
                phase.finish(error)

    # -- Model-level finish (called by cli_tree_progress) --------------------

    def finish_model(self, error: Exception | None = None) -> None:
        """Finalize this model's progress branch."""
        with self._lock:
            # Auto-finish any dangling step
            if self._current_node is not None:
                self._finish_current_node()
            for key in self._phase_order:
                self._phases[key].finalize()
            elapsed = _time.monotonic() - self._model_start
            if error is not None:
                self._branch.add(f"[red bold]✗[/] Failed: {error}")
                self._branch.label = (
                    f"[red bold]✗[/] [bold]{self._model_key}[/]  [dim]{_fmt_elapsed(elapsed)}[/]"
                )
            elif self._model_failed:
                self._branch.label = (
                    f"[red bold]✗[/] [bold]{self._model_key}[/]  [dim]{_fmt_elapsed(elapsed)}[/]"
                )
            else:
                self._branch.label = (
                    f"[green bold]✓[/] [bold]{self._model_key}[/]  [dim]{_fmt_elapsed(elapsed)}[/]"
                )
            self._live.update(self._root_tree)

    def _mark_failed(self) -> None:
        self._model_failed = True

    def _ensure_phase(self, key: str, *, title: str, icon: str) -> _PhaseNode:
        with self._lock:
            if key in self._phases:  # pragma: no cover — phase reuse path
                return self._phases[key]

            phase_branch = self._branch.add(f"[dim]○[/] {icon} {title}")
            phase = _PhaseNode(
                branch=phase_branch,
                root_tree=self._root_tree,
                live=self._live,
                lock=self._lock,
                title=title,
                icon=icon,
                parent=self,
            )
            self._phases[key] = phase
            self._phase_order.append(key)
            self._live.update(self._root_tree)
            return phase


class _PhaseNode(_TreeProgressNode):
    """Nested progress callback for one fixed phase under a model branch."""

    def __init__(
        self,
        *,
        branch: Tree,
        root_tree: Tree,
        live: Live,
        lock: threading.Lock,
        title: str,
        icon: str,
        parent: ModelNode,
    ) -> None:
        super().__init__(
            branch=branch,
            root_tree=root_tree,
            live=live,
            lock=lock,
            on_fail=parent._mark_failed,
        )
        self._title = title
        self._icon = icon
        self._parent = parent
        self._phase_start: float | None = None
        self._failed = False
        self._finalized = False

    def start(self) -> None:
        with self._lock:
            if self._phase_start is None:
                self._phase_start = _time.monotonic()
            self._branch.label = _LiveSpinnerLabel(
                f"{self._icon} {self._title}", start=self._phase_start
            )
            self._live.update(self._root_tree)

    def failed(self, message: str | None = None) -> None:  # pragma: no cover
        self._failed = True
        super().failed(message)

    def finish(self, error: Exception | None = None) -> None:  # pragma: no cover
        with self._lock:
            if self._finalized:
                return
            if self._current_node is not None:
                self._finish_current_node()
            if self._phase_start is None:
                self._phase_start = _time.monotonic()
            elapsed = _time.monotonic() - self._phase_start
            if error is not None or self._failed:
                self._branch.label = (
                    f"[red bold]✗[/] {self._icon} {self._title}  [dim]{_fmt_elapsed(elapsed)}[/]"
                )
                self._parent._model_failed = True
            else:
                self._branch.label = (
                    f"[green bold]✓[/] {self._icon} {self._title}  [dim]{_fmt_elapsed(elapsed)}[/]"
                )
            self._finalized = True
            self._live.update(self._root_tree)

    def finalize(self) -> None:  # pragma: no cover
        if self._finalized:
            return
        self.finish(None)

    def _finish_current_node(self, message: str | None = None) -> None:
        super()._finish_current_node(message)


@contextmanager
def _model_progress_context(node: ModelNode) -> Generator[ModelNode]:
    """Set progress_callback to *node* for the current thread."""
    from ..infrastructure.progress import progress_callback

    token = progress_callback.set(node)
    try:
        yield node
    finally:
        progress_callback.reset(token)


@contextmanager
def cli_tree_progress(
    *,
    enabled: bool = True,
    title: str = "Initializing",
) -> Generator[Callable[[str], contextmanager]]:
    """Render a Rich tree with per-model progress branches.

    Yields a *model_context* factory: call ``model_context(model_key)`` to get
    a context manager that sets ``progress_callback`` to a :class:`ModelNode`.

    Usage::

        with cli_tree_progress(enabled=True) as model_ctx:
            with model_ctx("test.table1"):
                with DBPort(...):
                    pass  # progress events go to the model branch
    """
    if not enabled:

        @contextmanager
        def _noop_ctx(model_key: str) -> Generator[None]:
            yield None

        yield _noop_ctx
        return

    tree = Tree(f"[bold]{title}[/]")
    lock = threading.Lock()

    with Live(tree, console=_console, refresh_per_second=8, transient=True) as live:

        @contextmanager
        def model_context(model_key: str) -> Generator[ModelNode]:
            with lock:
                branch = tree.add(_LiveSpinnerLabel(model_key, style="bold blue"))
                live.update(tree)
            node = ModelNode(branch, tree, live, lock, model_key)
            with _model_progress_context(node):
                error: Exception | None = None
                try:
                    yield node
                except Exception as exc:
                    error = exc
                    raise
                finally:
                    node.finish_model(error)

        yield model_context

    _console.print(tree)
