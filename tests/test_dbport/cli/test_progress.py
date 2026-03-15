"""Tests for CLI progress helpers — RichProgressAdapter and cli_progress()."""

from __future__ import annotations

from dbport.cli.render import (
    RichProgressAdapter,
    _ConditionalColumn,
    _ConditionalTextColumn,
    _SpinnerOrCheckColumn,
    cli_progress,
    configure_cli_logging,
)
from dbport.infrastructure.progress import progress_callback


class TestCliProgress:
    def test_disabled_does_not_set_contextvar(self):
        with cli_progress(enabled=False):
            assert progress_callback.get(None) is None

    def test_enabled_sets_and_clears_contextvar(self):
        with cli_progress(enabled=True):
            cb = progress_callback.get(None)
            assert cb is not None
            assert isinstance(cb, RichProgressAdapter)
        # After exiting, contextvar should be cleared
        assert progress_callback.get(None) is None

    def test_contextvar_cleared_on_exception(self):
        try:
            with cli_progress(enabled=True):
                assert progress_callback.get(None) is not None
                raise ValueError("test error")
        except ValueError:
            pass
        assert progress_callback.get(None) is None


class TestConfigureCliLogging:
    def test_quiet_sets_error_level(self):
        import logging

        from dbport.infrastructure import logging as dbport_logging

        dbport_logging._configured = False
        configure_cli_logging(verbose=False, quiet=True)
        assert logging.getLogger("dbport").level == logging.ERROR

    def test_verbose_sets_debug_level(self):
        import logging

        from dbport.infrastructure import logging as dbport_logging

        dbport_logging._configured = False
        configure_cli_logging(verbose=True, quiet=False)
        assert logging.getLogger("dbport").level == logging.DEBUG

    def test_default_sets_warning_level(self):
        import logging

        from dbport.infrastructure import logging as dbport_logging

        dbport_logging._configured = False
        configure_cli_logging(verbose=False, quiet=False)
        assert logging.getLogger("dbport").level == logging.WARNING


class TestProgressColumns:
    """Test the custom ProgressColumn subclasses directly."""

    def _make_task(self, *, determinate: bool = False, finished: bool = False, failed: bool = False):
        """Create a minimal Task-like object for column rendering."""
        from unittest.mock import MagicMock

        task = MagicMock()
        task.fields = {"determinate": determinate}
        if failed:
            task.fields["failed"] = True
        task.finished = finished
        task.completed = 100 if finished else 50
        task.total = 100
        return task

    def test_conditional_column_hides_for_indeterminate(self):
        from rich.progress import BarColumn

        inner = BarColumn()
        col = _ConditionalColumn(inner)
        task = self._make_task(determinate=False)
        result = col.render(task)
        assert str(result) == ""

    def test_conditional_column_renders_for_determinate(self):
        from rich.progress import BarColumn

        inner = BarColumn()
        col = _ConditionalColumn(inner)
        task = self._make_task(determinate=True)
        result = col.render(task)
        assert str(result) != ""

    def test_conditional_text_column_hides_for_indeterminate(self):
        col = _ConditionalTextColumn("rows")
        task = self._make_task(determinate=False)
        result = col.render(task)
        assert str(result) == ""

    def test_conditional_text_column_renders_for_determinate(self):
        col = _ConditionalTextColumn("rows")
        task = self._make_task(determinate=True)
        result = col.render(task)
        assert str(result) == "rows"

    def test_spinner_or_check_shows_check_when_finished(self):
        col = _SpinnerOrCheckColumn()
        task = self._make_task(finished=True)
        result = col.render(task)
        assert "✓" in str(result)

    def test_spinner_or_check_shows_cross_when_failed(self):
        col = _SpinnerOrCheckColumn()
        task = self._make_task(finished=True, failed=True)
        result = col.render(task)
        assert "✗" in str(result)

    def test_spinner_or_check_shows_spinner_when_running(self):
        col = _SpinnerOrCheckColumn()
        task = self._make_task(finished=False)
        result = col.render(task)
        # Spinner renders some character, not ✓ or ✗
        text = str(result)
        assert "✓" not in text
        assert "✗" not in text


class TestRichProgressAdapter:
    def test_started_update_finished_no_crash(self):
        """RichProgressAdapter methods should not crash outside a live context."""
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        # These should not raise even without the Progress context active
        with progress:
            adapter.started("Test task", total=100)
            adapter.update(50)
            adapter.update(50)
            adapter.finished("Done")

    def test_indeterminate_mode_no_crash(self):
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.started("Spinner task", total=None)
            adapter.finished("Complete")

    def test_multiple_tasks_sequential(self):
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.started("Task 1", total=100)
            adapter.update(100)
            adapter.finished("Task 1 done")

            adapter.started("Task 2", total=200)
            adapter.update(200)
            adapter.finished("Task 2 done")

        assert len(progress.tasks) == 2

    def test_auto_finish_previous_on_new_started(self):
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.started("Task 1", total=100)
            # Start a new task without finishing the previous one
            adapter.started("Task 2", total=200)
            adapter.finished("Task 2 done")

        assert len(progress.tasks) == 2

    def test_failed_marks_task_with_failed_field(self):
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.started("Task 1", total=None)
            adapter.failed("Task 1 failed, retrying")

        task = progress.tasks[0]
        assert task.finished
        assert task.fields.get("failed") is True
        assert task.description == "Task 1 failed, retrying"

    def test_failed_then_started_shows_new_task(self):
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.started("Attempt 1", total=None)
            adapter.failed("Attempt 1 failed")
            adapter.started("Attempt 2", total=None)
            adapter.finished("Attempt 2 done")

        assert len(progress.tasks) == 2
        assert progress.tasks[0].fields.get("failed") is True
        assert progress.tasks[1].fields.get("failed") is not True

    def test_failed_without_task_is_noop(self):
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.failed("no task")  # should not raise

        assert len(progress.tasks) == 0

    def test_finished_without_task_is_noop(self):
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.finished("no task")  # should not raise

        assert len(progress.tasks) == 0

    def test_update_without_task_is_noop(self):
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.update(100)  # should not raise

    def test_log_prints_to_console(self, capsys):
        from io import StringIO

        from rich.console import Console
        from rich.progress import Progress

        output = StringIO()
        console = Console(file=output, no_color=True)
        progress = Progress(console=console, transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.log("Switching to streaming Arrow fallback")

        text = output.getvalue()
        assert "Switching to streaming Arrow fallback" in text
