"""Tests for CLI error handling."""

from __future__ import annotations

import json

import pytest

from dbport.cli.errors import cli_error_handler


class TestCliErrorHandler:
    def test_no_exception(self):
        with cli_error_handler("test"):
            pass  # should not raise

    def test_runtime_error_exits_1(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise RuntimeError("something broke")
        assert exc_info.value.code == 1

    def test_file_not_found_exits_1(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise FileNotFoundError("missing.sql")
        assert exc_info.value.code == 1

    def test_generic_exception_exits_1(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise ValueError("bad value")
        assert exc_info.value.code == 1

    def test_keyboard_interrupt_exits_130(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise KeyboardInterrupt()
        assert exc_info.value.code == 130

    def test_system_exit_re_raised(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise SystemExit(42)
        assert exc_info.value.code == 42

    def test_runtime_error_json(self, capsys):
        with pytest.raises(SystemExit):
            with cli_error_handler("test", json_output=True):
                raise RuntimeError("bad")
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["ok"] is False
        assert data["command"] == "test"
        assert "bad" in data["data"]["error"]

    def test_file_not_found_json(self, capsys):
        with pytest.raises(SystemExit):
            with cli_error_handler("test", json_output=True):
                raise FileNotFoundError("gone.sql")
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["ok"] is False
        assert "gone.sql" in data["data"]["error"]

    def test_generic_exception_json(self, capsys):
        with pytest.raises(SystemExit):
            with cli_error_handler("test", json_output=True):
                raise ValueError("unexpected")
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["ok"] is False

    def test_keyboard_interrupt_json(self, capsys):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test", json_output=True):
                raise KeyboardInterrupt()
        assert exc_info.value.code == 130
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["ok"] is False
        assert data["data"]["error"] == "interrupted"
