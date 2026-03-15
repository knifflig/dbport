"""Tests for CLI render helpers."""

from __future__ import annotations

import json

from dbport.cli.render import (
    get_console,
    get_stdout,
    print_json,
    print_panel,
    print_table,
    set_no_color,
)


class TestRenderHelpers:
    def test_get_console_returns_console(self):
        c = get_console()
        assert c is not None

    def test_get_stdout_returns_console(self):
        c = get_stdout()
        assert c is not None

    def test_set_no_color_swaps_consoles(self):
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

    def test_print_json_output(self, capsys):
        print_json("test_cmd", {"key": "val"}, ok=True)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["ok"] is True
        assert data["command"] == "test_cmd"
        assert data["data"]["key"] == "val"

    def test_print_json_not_ok(self, capsys):
        print_json("fail_cmd", {"error": "oops"}, ok=False)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["ok"] is False

    def test_print_table_no_crash(self, capsys):
        # Should not raise
        print_table("My Table", ["A", "B"], [["1", "2"], ["3", "4"]])

    def test_print_panel_no_crash(self, capsys):
        # Should not raise
        print_panel("Title", "Some content here")
