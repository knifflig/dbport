"""Tests for CLI render helpers."""

from __future__ import annotations

import json
from io import StringIO

import pytest
from rich.console import Console

from dbport.cli.render import (
    cli_tree_progress,
    get_console,
    get_stdout,
    print_error,
    print_info,
    print_json,
    print_panel,
    print_success,
    print_table,
    print_warning,
    set_no_color,
)
from dbport.infrastructure.progress import progress_callback, progress_phase


class TestRenderHelpers:
    """Tests for TestRenderHelpers."""

    def test_get_console_returns_console(self) -> None:
        """Test Get console returns console."""
        c = get_console()
        assert c is not None

    def test_get_stdout_returns_console(self) -> None:
        """Test Get stdout returns console."""
        c = get_stdout()
        assert c is not None

    def test_set_no_color_swaps_consoles(self) -> None:
        """Test Set no color swaps consoles."""
        old_console = get_console()
        old_stdout = get_stdout()
        set_no_color(True)
        # After set_no_color(True), consoles should be different instances
        new_console = get_console()
        new_stdout = get_stdout()
        assert new_console is not old_console
        assert new_stdout is not old_stdout
        # Reset
        set_no_color(False)  # no-op (doesn't reset), but shouldn't crash

    def test_print_json_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Test Print json output."""
        print_json("test_cmd", {"key": "val"}, ok=True)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["ok"] is True
        assert data["command"] == "test_cmd"
        assert data["data"]["key"] == "val"

    def test_print_json_not_ok(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Test Print json not ok."""
        print_json("fail_cmd", {"error": "oops"}, ok=False)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["ok"] is False

    def test_print_table_no_crash(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Test Print table no crash."""
        # Should not raise
        print_table("My Table", ["A", "B"], [["1", "2"], ["3", "4"]])

    def test_print_panel_no_crash(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Test Print panel no crash."""
        # Should not raise
        print_panel("Title", "Some content here")

    def test_print_success_outputs(self) -> None:
        """Test Print success outputs."""
        buf = StringIO()
        console = Console(file=buf, no_color=True)
        import dbport.cli.render as _mod

        old = _mod._stdout
        _mod._stdout = console
        try:
            print_success("it worked")
        finally:
            _mod._stdout = old
        assert "OK" in buf.getvalue()
        assert "it worked" in buf.getvalue()

    def test_print_error_outputs(self) -> None:
        """Test Print error outputs."""
        buf = StringIO()
        console = Console(file=buf, no_color=True)
        import dbport.cli.render as _mod

        old = _mod._console
        _mod._console = console
        try:
            print_error("bad thing")
        finally:
            _mod._console = old
        assert "Error:" in buf.getvalue()
        assert "bad thing" in buf.getvalue()

    def test_print_warning_outputs(self) -> None:
        """Test Print warning outputs."""
        buf = StringIO()
        console = Console(file=buf, no_color=True)
        import dbport.cli.render as _mod

        old = _mod._console
        _mod._console = console
        try:
            print_warning("careful")
        finally:
            _mod._console = old
        assert "Warning:" in buf.getvalue()
        assert "careful" in buf.getvalue()

    def test_print_info_outputs(self) -> None:
        """Test Print info outputs."""
        buf = StringIO()
        console = Console(file=buf, no_color=True)
        import dbport.cli.render as _mod

        old = _mod._stdout
        _mod._stdout = console
        try:
            print_info("some info")
        finally:
            _mod._stdout = old
        assert "some info" in buf.getvalue()

    def test_cli_tree_progress_shows_fixed_run_phases(self) -> None:
        """Test Cli tree progress shows fixed run phases."""
        buf = StringIO()
        console = Console(file=buf, no_color=True, force_terminal=False, width=120)
        import dbport.cli.render as _mod

        old = _mod._console
        _mod._console = console
        try:
            with cli_tree_progress(enabled=True, title="Running models") as model_ctx:
                with model_ctx("a.b"):
                    with progress_phase("sync", title="Sync", icon="🔄"):
                        cb = progress_callback.get()
                        assert cb is not None
                        cb.started("Detecting schema from warehouse")
                        cb.finished("No existing warehouse table")

                    with progress_phase("load", title="Load", icon="📥"):
                        cb = progress_callback.get()
                        assert cb is not None
                        cb.log("Skipping ns.tbl (snapshot unchanged)")

                    with progress_phase("exec", title="Exec", icon="⚙️"):
                        cb = progress_callback.get()
                        assert cb is not None
                        cb.started("Executing main.py")
                        cb.finished("Executed main.py")

                    with progress_phase("publish", title="Publish", icon="🚀"):
                        cb = progress_callback.get()
                        assert cb is not None
                        cb.started("Publishing a.b", total=10)
                        cb.update(5)
                        cb.finished("Published a.b")
        finally:
            _mod._console = old

        output = buf.getvalue()
        assert "🔄 Sync" in output
        assert "📥 Load" in output
        assert "⚙️ Exec" in output
        assert "🚀 Publish" in output
        assert "No existing warehouse table" in output
        assert "Skipping ns.tbl (snapshot unchanged)" in output
        assert "Published a.b" in output
