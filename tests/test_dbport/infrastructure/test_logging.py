"""Tests for infrastructure.logging."""

from __future__ import annotations

import logging
from types import ModuleType

import pytest

from dbport.infrastructure import logging as dbport_logging
from dbport.infrastructure.logging import setup_logging


class TestSetupLogging:
    """Tests for setup_logging."""

    def setup_method(self) -> None:
        """Reset logging state before each test."""
        # Reset the idempotency flag before each test
        dbport_logging._configured = False
        # Clear any handlers added to the dbport logger by previous tests
        logging.getLogger("dbport").handlers.clear()

    def test_setup_logging_configures_dbport_logger(self) -> None:
        """Test that setup_logging configures the dbport logger."""
        setup_logging()
        dbport_logger = logging.getLogger("dbport")
        assert dbport_logger.level == logging.INFO
        assert len(dbport_logger.handlers) >= 1

    def test_setup_logging_sets_configured_flag(self) -> None:
        """Test that setup_logging sets the configured flag."""
        assert dbport_logging._configured is False
        setup_logging()
        assert dbport_logging._configured is True

    def test_setup_logging_is_idempotent(self) -> None:
        """Test that repeated calls do not add duplicate handlers."""
        setup_logging()
        handler_count = len(logging.getLogger("dbport").handlers)
        setup_logging()
        assert len(logging.getLogger("dbport").handlers) == handler_count

    def test_setup_logging_custom_level(self) -> None:
        """Test setup_logging with a custom level."""
        setup_logging(level=logging.WARNING)
        dbport_logger = logging.getLogger("dbport")
        assert dbport_logger.level == logging.WARNING

    def test_force_reconfigures(self) -> None:
        """Test that force=True reconfigures logging."""
        setup_logging(level=logging.INFO)
        assert dbport_logging._configured is True
        setup_logging(level=logging.DEBUG, force=True)
        dbport_logger = logging.getLogger("dbport")
        assert dbport_logger.level == logging.DEBUG

    def test_third_party_loggers_suppressed(self) -> None:
        """Test that noisy third-party loggers are suppressed."""
        setup_logging()
        for name in dbport_logging._NOISY_LOGGERS:
            assert logging.getLogger(name).level >= logging.WARNING

    def test_console_parameter_forwarded(self) -> None:
        """RichHandler receives the console parameter when provided."""
        from rich.console import Console

        console = Console(stderr=True)
        setup_logging(console=console, force=True)
        dbport_logger = logging.getLogger("dbport")
        handler = dbport_logger.handlers[0]
        assert handler.console is console  # type: ignore[attr-defined]

    def test_stdlib_fallback_when_rich_unavailable(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Falls back to stdlib logging when rich is not installed."""
        import builtins

        real_import = builtins.__import__

        def mock_import(
            name: str,
            *args: object,
            **kwargs: object,
        ) -> ModuleType:
            if "rich" in name:
                raise ImportError("No module named 'rich'")
            return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(builtins, "__import__", mock_import)
        dbport_logging._configured = False
        setup_logging()
        assert dbport_logging._configured is True
