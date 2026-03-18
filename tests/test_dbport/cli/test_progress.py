"""Tests for CLI progress helpers — RichProgressAdapter, ModelNode, tree progress."""

from __future__ import annotations

import threading
import time
from io import StringIO
from unittest.mock import MagicMock

import pytest
from rich.console import Console
from rich.live import Live
from rich.tree import Tree

from dbport.cli.render import (
    ModelNode,
    RichProgressAdapter,
    _ConditionalColumn,
    _ConditionalTextColumn,
    _fmt_elapsed,
    _LiveProgressLabel,
    _LiveSpinnerLabel,
    _model_progress_context,
    _render_bar,
    _SpinnerOrCheckColumn,
    cli_progress,
    cli_tree_progress,
    configure_cli_logging,
)
from dbport.infrastructure.progress import progress_callback


class TestCliProgress:
    """Tests for TestCliProgress."""

    def test_disabled_does_not_set_contextvar(self) -> None:
        """Test Disabled does not set contextvar."""
        with cli_progress(enabled=False):
            assert progress_callback.get(None) is None

    def test_enabled_sets_and_clears_contextvar(self) -> None:
        """Test Enabled sets and clears contextvar."""
        with cli_progress(enabled=True):
            cb = progress_callback.get(None)
            assert cb is not None
            assert isinstance(cb, RichProgressAdapter)
        # After exiting, contextvar should be cleared
        assert progress_callback.get(None) is None

    def test_contextvar_cleared_on_exception(self) -> None:
        """Test Contextvar cleared on exception."""
        try:
            with cli_progress(enabled=True):
                assert progress_callback.get(None) is not None
                raise ValueError("test error")
        except ValueError:
            pass
        assert progress_callback.get(None) is None


class TestConfigureCliLogging:
    """Tests for TestConfigureCliLogging."""

    def test_quiet_sets_error_level(self) -> None:
        """Test Quiet sets error level."""
        import logging

        from dbport.infrastructure import logging as dbport_logging

        dbport_logging._configured = False
        configure_cli_logging(verbose=False, quiet=True)
        assert logging.getLogger("dbport").level == logging.ERROR

    def test_verbose_sets_debug_level(self) -> None:
        """Test Verbose sets debug level."""
        import logging

        from dbport.infrastructure import logging as dbport_logging

        dbport_logging._configured = False
        configure_cli_logging(verbose=True, quiet=False)
        assert logging.getLogger("dbport").level == logging.DEBUG

    def test_default_sets_warning_level(self) -> None:
        """Test Default sets warning level."""
        import logging

        from dbport.infrastructure import logging as dbport_logging

        dbport_logging._configured = False
        configure_cli_logging(verbose=False, quiet=False)
        assert logging.getLogger("dbport").level == logging.WARNING


class TestProgressColumns:
    """Test the custom ProgressColumn subclasses directly."""

    def _make_task(
        self,
        *,
        determinate: bool = False,
        finished: bool = False,
        failed: bool = False,
    ) -> MagicMock:
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

    def test_conditional_column_hides_for_indeterminate(self) -> None:
        """Test Conditional column hides for indeterminate."""
        from rich.progress import BarColumn

        inner = BarColumn()
        col = _ConditionalColumn(inner)
        task = self._make_task(determinate=False)
        result = col.render(task)
        assert str(result) == ""

    def test_conditional_column_renders_for_determinate(self) -> None:
        """Test Conditional column renders for determinate."""
        from rich.progress import BarColumn

        inner = BarColumn()
        col = _ConditionalColumn(inner)
        task = self._make_task(determinate=True)
        result = col.render(task)
        assert str(result) != ""

    def test_conditional_text_column_hides_for_indeterminate(self) -> None:
        """Test Conditional text column hides for indeterminate."""
        col = _ConditionalTextColumn("rows")
        task = self._make_task(determinate=False)
        result = col.render(task)
        assert str(result) == ""

    def test_conditional_text_column_renders_for_determinate(self) -> None:
        """Test Conditional text column renders for determinate."""
        col = _ConditionalTextColumn("rows")
        task = self._make_task(determinate=True)
        result = col.render(task)
        assert str(result) == "rows"

    def test_spinner_or_check_shows_check_when_finished(self) -> None:
        """Test Spinner or check shows check when finished."""
        col = _SpinnerOrCheckColumn()
        task = self._make_task(finished=True)
        result = col.render(task)
        assert "✓" in str(result)

    def test_spinner_or_check_shows_cross_when_failed(self) -> None:
        """Test Spinner or check shows cross when failed."""
        col = _SpinnerOrCheckColumn()
        task = self._make_task(finished=True, failed=True)
        result = col.render(task)
        assert "✗" in str(result)

    def test_spinner_or_check_shows_spinner_when_running(self) -> None:
        """Test Spinner or check shows spinner when running."""
        col = _SpinnerOrCheckColumn()
        task = self._make_task(finished=False)
        result = col.render(task)
        # Spinner renders some character, not ✓ or ✗
        text = str(result)
        assert "✓" not in text
        assert "✗" not in text


class TestRichProgressAdapter:
    """Tests for TestRichProgressAdapter."""

    def test_started_update_finished_no_crash(self) -> None:
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

    def test_indeterminate_mode_no_crash(self) -> None:
        """Test Indeterminate mode no crash."""
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.started("Spinner task", total=None)
            adapter.finished("Complete")

    def test_multiple_tasks_sequential(self) -> None:
        """Test Multiple tasks sequential."""
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

    def test_auto_finish_previous_on_new_started(self) -> None:
        """Test Auto finish previous on new started."""
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.started("Task 1", total=100)
            # Start a new task without finishing the previous one
            adapter.started("Task 2", total=200)
            adapter.finished("Task 2 done")

        assert len(progress.tasks) == 2

    def test_failed_marks_task_with_failed_field(self) -> None:
        """Test Failed marks task with failed field."""
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

    def test_failed_then_started_shows_new_task(self) -> None:
        """Test Failed then started shows new task."""
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

    def test_failed_without_task_is_noop(self) -> None:
        """Test Failed without task is noop."""
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.failed("no task")  # should not raise

        assert len(progress.tasks) == 0

    def test_finished_without_task_is_noop(self) -> None:
        """Test Finished without task is noop."""
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.finished("no task")  # should not raise

        assert len(progress.tasks) == 0

    def test_update_without_task_is_noop(self) -> None:
        """Test Update without task is noop."""
        from rich.progress import Progress

        progress = Progress(transient=True)
        adapter = RichProgressAdapter(progress)

        with progress:
            adapter.update(100)  # should not raise

    def test_log_prints_to_console(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Test Log prints to console."""
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


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


class TestRenderBar:
    """Tests for TestRenderBar."""

    def test_empty_bar(self) -> None:
        """Test Empty bar."""
        bar = _render_bar(0.0)
        assert "━" in bar

    def test_full_bar(self) -> None:
        """Test Full bar."""
        bar = _render_bar(1.0)
        assert "━" in bar

    def test_half_bar(self) -> None:
        """Test Half bar."""
        bar = _render_bar(0.5, width=10)
        assert "━" in bar

    def test_custom_width(self) -> None:
        """Test Custom width."""
        bar = _render_bar(0.25, width=8)
        assert "━" in bar


class TestFmtElapsed:
    """Tests for TestFmtElapsed."""

    def test_zero_seconds(self) -> None:
        """Test Zero seconds."""
        assert _fmt_elapsed(0) == "0:00"

    def test_seconds_only(self) -> None:
        """Test Seconds only."""
        assert _fmt_elapsed(45) == "0:45"

    def test_minutes_and_seconds(self) -> None:
        """Test Minutes and seconds."""
        assert _fmt_elapsed(125) == "2:05"

    def test_hours(self) -> None:
        """Test Hours."""
        assert _fmt_elapsed(3661) == "1:01:01"

    def test_exact_hour(self) -> None:
        """Test Exact hour."""
        assert _fmt_elapsed(3600) == "1:00:00"


# ---------------------------------------------------------------------------
# Live renderables
# ---------------------------------------------------------------------------


def _render_to_text(renderable: object) -> str:
    """Render a Rich renderable to plain text."""
    console = Console(file=StringIO(), no_color=True, width=200)
    console.print(renderable)
    return console.file.getvalue()


class TestLiveSpinnerLabel:
    """Tests for TestLiveSpinnerLabel."""

    def test_renders_text_and_elapsed(self) -> None:
        """Test Renders text and elapsed."""
        label = _LiveSpinnerLabel("Loading data", start=time.monotonic() - 5)
        text = _render_to_text(label)
        assert "Loading data" in text
        assert "0:0" in text  # at least 0:05 or similar

    def test_renders_with_style(self) -> None:
        """Test Renders with style."""
        label = _LiveSpinnerLabel("model.key", style="bold blue", start=time.monotonic())
        text = _render_to_text(label)
        assert "model.key" in text

    def test_default_start_is_now(self) -> None:
        """Test Default start is now."""
        label = _LiveSpinnerLabel("test")
        text = _render_to_text(label)
        assert "test" in text
        assert "0:00" in text


class TestLiveProgressLabel:
    """Tests for TestLiveProgressLabel."""

    def test_determinate_with_bar(self) -> None:
        """Test Determinate with bar."""
        label = _LiveProgressLabel(
            "Publishing",
            bar=_render_bar(0.5),
            completed=500_000,
            total=1_000_000,
            start=time.monotonic() - 10,
            eta="  ETA 0:10",
        )
        text = _render_to_text(label)
        assert "Publishing" in text
        assert "500,000" in text
        assert "1,000,000" in text
        assert "ETA" in text

    def test_indeterminate_without_bar(self) -> None:
        """Test Indeterminate without bar."""
        label = _LiveProgressLabel(
            "Loading",
            completed=42_000,
            start=time.monotonic() - 3,
        )
        text = _render_to_text(label)
        assert "Loading" in text
        assert "42,000" in text

    def test_default_start_is_now(self) -> None:
        """Test Default start is now."""
        label = _LiveProgressLabel("test", completed=0)
        text = _render_to_text(label)
        assert "test" in text
        assert "0:00" in text


# ---------------------------------------------------------------------------
# ModelNode
# ---------------------------------------------------------------------------


def _make_model_node(model_key: str = "test.model") -> tuple[ModelNode, Tree, Live, Console]:
    """Create a ModelNode with a real Tree/Live for testing."""
    buf = StringIO()
    console = Console(file=buf, no_color=True, width=200)
    tree = Tree("Test")
    lock = threading.Lock()
    live = Live(tree, console=console, auto_refresh=False)
    live.start()
    branch = tree.add(f"[bold]{model_key}[/]")
    node = ModelNode(branch, tree, live, lock, model_key)
    return node, tree, live, console


def _stop_live(live: Live) -> None:
    try:
        live.stop()
    except Exception:
        pass


class TestModelNode:
    """Tests for TestModelNode."""

    def test_started_indeterminate_adds_child(self) -> None:
        """Test Started indeterminate adds child."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Detecting schema")
            assert node._current_node is not None
            assert node._current_desc == "Detecting schema"
            assert node._total is None
        finally:
            _stop_live(live)

    def test_started_determinate_adds_child(self) -> None:
        """Test Started determinate adds child."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Publishing", total=1000)
            assert node._current_node is not None
            assert node._total == 1000
            assert node._completed == 0
        finally:
            _stop_live(live)

    def test_started_auto_finishes_previous(self) -> None:
        """Test Started auto finishes previous."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Step 1")
            first_node = node._current_node
            node.started("Step 2")
            # First node should have been finished (label replaced with ✓)
            assert "✓" in str(first_node.label)
            assert node._current_desc == "Step 2"
        finally:
            _stop_live(live)

    def test_update_determinate_progress(self) -> None:
        """Test Update determinate progress."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Publishing", total=1000)
            node.update(500)
            assert node._completed == 500
            # Label should now be a _LiveProgressLabel
            assert isinstance(node._current_node.label, _LiveProgressLabel)
        finally:
            _stop_live(live)

    def test_update_indeterminate_progress(self) -> None:
        """Test Update indeterminate progress."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Loading", total=None)
            node.update(100)
            assert node._completed == 100
            # Without total, still uses _LiveProgressLabel but without bar
            assert isinstance(node._current_node.label, _LiveProgressLabel)
        finally:
            _stop_live(live)

    def test_update_without_current_node_is_noop(self) -> None:
        """Test Update without current node is noop."""
        node, tree, live, console = _make_model_node()
        try:
            node.update(100)  # no started() called, should not crash
        finally:
            _stop_live(live)

    def test_update_with_eta(self) -> None:
        """Test Update with eta."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Publishing", total=1000)
            node._step_start = time.monotonic() - 10  # fake 10s elapsed
            node.update(500)  # 50% done → ETA should appear
            assert node._completed == 500
        finally:
            _stop_live(live)

    def test_log_adds_dim_child(self) -> None:
        """Test Log adds dim child."""
        node, tree, live, console = _make_model_node()
        try:
            node.log("Skipping table (snapshot unchanged)")
            # Branch should have a child with the log message
            children = node._branch.children
            assert len(children) >= 1
            assert "Skipping" in str(children[-1].label)
        finally:
            _stop_live(live)

    def test_finished_marks_step_done(self) -> None:
        """Test Finished marks step done."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Creating table")
            step_node = node._current_node
            node.finished("Created table")
            assert node._current_node is None
            assert "✓" in str(step_node.label)
            assert "Created table" in str(step_node.label)
        finally:
            _stop_live(live)

    def test_finished_with_default_message(self) -> None:
        """Test Finished with default message."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Loading data")
            step_node = node._current_node
            node.finished()
            assert "✓" in str(step_node.label)
            assert "Loading data" in str(step_node.label)
        finally:
            _stop_live(live)

    def test_finished_without_current_node_is_noop(self) -> None:
        """Test Finished without current node is noop."""
        node, tree, live, console = _make_model_node()
        try:
            node.finished("no task")  # should not crash
        finally:
            _stop_live(live)

    def test_finished_resets_progress_state(self) -> None:
        """Test Finished resets progress state."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Publishing", total=1000)
            node.update(500)
            node.finished("Published")
            assert node._total is None
            assert node._completed == 0
            assert node._current_node is None
        finally:
            _stop_live(live)

    def test_failed_marks_step_with_cross(self) -> None:
        """Test Failed marks step with cross."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Loading table")
            step_node = node._current_node
            node.failed("Load failed, retrying")
            assert node._current_node is None
            assert "✗" in str(step_node.label)
            assert "Load failed" in str(step_node.label)
            assert node._model_failed is True
        finally:
            _stop_live(live)

    def test_failed_with_default_message(self) -> None:
        """Test Failed with default message."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Loading table")
            step_node = node._current_node
            node.failed()
            assert "✗" in str(step_node.label)
            assert "Loading table" in str(step_node.label)
        finally:
            _stop_live(live)

    def test_failed_without_current_node_is_noop(self) -> None:
        """Test Failed without current node is noop."""
        node, tree, live, console = _make_model_node()
        try:
            node.failed("no task")  # should not crash
        finally:
            _stop_live(live)

    def test_failed_resets_progress_state(self) -> None:
        """Test Failed resets progress state."""
        node, tree, live, console = _make_model_node()
        try:
            node.started("Publishing", total=1000)
            node.update(500)
            node.failed("Publish failed")
            assert node._total is None
            assert node._completed == 0
        finally:
            _stop_live(live)

    def test_finish_model_success(self) -> None:
        """Test Finish model success."""
        node, tree, live, console = _make_model_node("test.table1")
        try:
            node.started("Step 1")
            node.finished("Step 1 done")
            node.finish_model()
            assert "✓" in str(node._branch.label)
            assert "test.table1" in str(node._branch.label)
        finally:
            _stop_live(live)

    def test_finish_model_with_error(self) -> None:
        """Test Finish model with error."""
        node, tree, live, console = _make_model_node("test.table1")
        try:
            node.finish_model(error=RuntimeError("connection failed"))
            assert "✗" in str(node._branch.label)
            # Should add error child
            children = node._branch.children
            error_labels = [str(c.label) for c in children]
            assert any("connection failed" in lbl for lbl in error_labels)
        finally:
            _stop_live(live)

    def test_finish_model_after_failed_step(self) -> None:
        """Test Finish model after failed step."""
        node, tree, live, console = _make_model_node("test.table1")
        try:
            node.started("Loading")
            node.failed("Load failed")
            node.finish_model()
            # model_failed was set, so branch label should show ✗
            assert "✗" in str(node._branch.label)
        finally:
            _stop_live(live)

    def test_finish_model_auto_finishes_dangling_step(self) -> None:
        """Test Finish model auto finishes dangling step."""
        node, tree, live, console = _make_model_node("test.table1")
        try:
            node.started("Step in progress")
            step_node = node._current_node
            node.finish_model()
            # Dangling step should have been auto-finished
            assert "✓" in str(step_node.label)
            assert "✓" in str(node._branch.label)
        finally:
            _stop_live(live)


# ---------------------------------------------------------------------------
# _model_progress_context
# ---------------------------------------------------------------------------


class TestModelProgressContext:
    """Tests for TestModelProgressContext."""

    def test_sets_and_clears_contextvar(self) -> None:
        """Test Sets and clears contextvar."""
        node, tree, live, console = _make_model_node()
        try:
            assert progress_callback.get(None) is None
            with _model_progress_context(node) as n:
                assert progress_callback.get(None) is node
                assert n is node
            assert progress_callback.get(None) is None
        finally:
            _stop_live(live)

    def test_clears_contextvar_on_exception(self) -> None:
        """Test Clears contextvar on exception."""
        node, tree, live, console = _make_model_node()
        try:
            try:
                with _model_progress_context(node):
                    assert progress_callback.get(None) is node
                    raise ValueError("test")
            except ValueError:
                pass
            assert progress_callback.get(None) is None
        finally:
            _stop_live(live)


# ---------------------------------------------------------------------------
# cli_tree_progress
# ---------------------------------------------------------------------------


class TestCliTreeProgress:
    """Tests for TestCliTreeProgress."""

    def test_disabled_yields_noop_context(self) -> None:
        """Test Disabled yields noop context."""
        with cli_tree_progress(enabled=False) as model_ctx:
            with model_ctx("test.model") as node:
                assert node is None
                assert progress_callback.get(None) is None

    def test_enabled_yields_model_node(self) -> None:
        """Test Enabled yields model node."""
        with cli_tree_progress(enabled=True, title="Testing") as model_ctx:
            with model_ctx("test.table1") as node:
                assert isinstance(node, ModelNode)
                cb = progress_callback.get(None)
                assert cb is node

    def test_contextvar_cleared_after_model_context(self) -> None:
        """Test Contextvar cleared after model context."""
        with cli_tree_progress(enabled=True) as model_ctx:
            with model_ctx("test.table1"):
                assert progress_callback.get(None) is not None
            assert progress_callback.get(None) is None

    def test_multiple_models_sequential(self) -> None:
        """Test Multiple models sequential."""
        with cli_tree_progress(enabled=True) as model_ctx:
            with model_ctx("test.table1") as node1:
                assert isinstance(node1, ModelNode)
            with model_ctx("test.table2") as node2:
                assert isinstance(node2, ModelNode)

    def test_model_context_catches_and_reraises_exception(self) -> None:
        """Test Model context catches and reraises exception."""
        with cli_tree_progress(enabled=True) as model_ctx:
            try:
                with model_ctx("test.table1"):
                    raise RuntimeError("sync failed")
            except RuntimeError:
                pass
            # Should still be able to use more model contexts
            with model_ctx("test.table2") as node2:
                assert isinstance(node2, ModelNode)

    def test_finish_model_called_on_error(self) -> None:
        """Test Finish model called on error."""
        with cli_tree_progress(enabled=True) as model_ctx:
            try:
                with model_ctx("test.table1") as node:
                    node.started("Step 1")
                    raise RuntimeError("boom")
            except RuntimeError:
                pass
            # After error, branch label should show ✗
            assert "✗" in str(node._branch.label)

    def test_progress_events_work_through_tree(self) -> None:
        """Test Progress events work through tree."""
        with cli_tree_progress(enabled=True) as model_ctx:
            with model_ctx("test.table1") as node:
                node.started("Loading data")
                node.finished("Loaded 1,000 rows")
                node.log("Skipping cached table")
                node.started("Publishing", total=100)
                node.update(50)
                node.update(50)
                node.finished("Published")
            # Model should show ✓
            assert "✓" in str(node._branch.label)
