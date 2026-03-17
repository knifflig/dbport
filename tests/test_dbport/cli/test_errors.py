"""Tests for CLI error handling and exit codes."""

from __future__ import annotations

import json

import pytest

from dbport.cli.errors import (
    EXIT_INTERNAL_ERROR,
    EXIT_INTERRUPTED,
    EXIT_USER_ERROR,
    CliUserError,
    cli_error_handler,
)


class TestExitCodes:
    """Verify the documented exit code contract."""

    def test_no_exception(self):
        with cli_error_handler("test"):
            pass  # should not raise

    def test_runtime_error_exits_1(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise RuntimeError("something broke")
        assert exc_info.value.code == EXIT_USER_ERROR

    def test_file_not_found_exits_1(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise FileNotFoundError("missing.sql")
        assert exc_info.value.code == EXIT_USER_ERROR

    def test_cli_user_error_exits_1(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise CliUserError("bad input")
        assert exc_info.value.code == EXIT_USER_ERROR

    def test_generic_exception_exits_2(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise ValueError("bad value")
        assert exc_info.value.code == EXIT_INTERNAL_ERROR

    def test_keyboard_interrupt_exits_130(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise KeyboardInterrupt()
        assert exc_info.value.code == EXIT_INTERRUPTED

    def test_system_exit_re_raised(self):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test"):
                raise SystemExit(42)
        assert exc_info.value.code == 42


class TestJsonErrorOutput:
    """Verify JSON output includes error_type for automation."""

    def test_runtime_error_json(self, capsys):
        with pytest.raises(SystemExit):
            with cli_error_handler("test", json_output=True):
                raise RuntimeError("bad")
        data = json.loads(capsys.readouterr().out)
        assert data["ok"] is False
        assert data["command"] == "test"
        assert "bad" in data["data"]["error"]
        assert data["data"]["error_type"] == "runtime_error"

    def test_file_not_found_json(self, capsys):
        with pytest.raises(SystemExit):
            with cli_error_handler("test", json_output=True):
                raise FileNotFoundError("gone.sql")
        data = json.loads(capsys.readouterr().out)
        assert data["ok"] is False
        assert data["data"]["error_type"] == "file_not_found"

    def test_cli_user_error_json(self, capsys):
        with pytest.raises(SystemExit):
            with cli_error_handler("test", json_output=True):
                raise CliUserError("missing model")
        data = json.loads(capsys.readouterr().out)
        assert data["ok"] is False
        assert data["data"]["error_type"] == "validation_error"

    def test_generic_exception_json(self, capsys):
        with pytest.raises(SystemExit):
            with cli_error_handler("test", json_output=True):
                raise ValueError("unexpected")
        data = json.loads(capsys.readouterr().out)
        assert data["ok"] is False
        assert data["data"]["error_type"] == "internal_error"

    def test_keyboard_interrupt_json(self, capsys):
        with pytest.raises(SystemExit) as exc_info:
            with cli_error_handler("test", json_output=True):
                raise KeyboardInterrupt()
        assert exc_info.value.code == EXIT_INTERRUPTED
        data = json.loads(capsys.readouterr().out)
        assert data["ok"] is False
        assert data["data"]["error_type"] == "interrupted"
