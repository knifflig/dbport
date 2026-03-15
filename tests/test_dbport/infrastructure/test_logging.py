"""Tests for infrastructure.logging."""

from __future__ import annotations

import logging

from dbport.infrastructure import logging as dbport_logging
from dbport.infrastructure.logging import setup_logging


class TestSetupLogging:
    def setup_method(self):
        # Reset the idempotency flag before each test
        dbport_logging._configured = False
        # Clear any handlers added to the dbport logger by previous tests
        logging.getLogger("dbport").handlers.clear()

    def test_setup_logging_configures_dbport_logger(self):
        setup_logging()
        dbport_logger = logging.getLogger("dbport")
        assert dbport_logger.level == logging.INFO
        assert len(dbport_logger.handlers) >= 1

    def test_setup_logging_sets_configured_flag(self):
        assert dbport_logging._configured is False
        setup_logging()
        assert dbport_logging._configured is True

    def test_setup_logging_is_idempotent(self):
        setup_logging()
        handler_count = len(logging.getLogger("dbport").handlers)
        setup_logging()
        assert len(logging.getLogger("dbport").handlers) == handler_count

    def test_setup_logging_custom_level(self):
        setup_logging(level=logging.WARNING)
        dbport_logger = logging.getLogger("dbport")
        assert dbport_logger.level == logging.WARNING

    def test_force_reconfigures(self):
        setup_logging(level=logging.INFO)
        assert dbport_logging._configured is True
        setup_logging(level=logging.DEBUG, force=True)
        dbport_logger = logging.getLogger("dbport")
        assert dbport_logger.level == logging.DEBUG

    def test_third_party_loggers_suppressed(self):
        setup_logging()
        for name in dbport_logging._NOISY_LOGGERS:
            assert logging.getLogger(name).level >= logging.WARNING

    def test_console_parameter_forwarded(self):
        """RichHandler receives the console parameter when provided."""
        from rich.console import Console

        console = Console(stderr=True)
        setup_logging(console=console, force=True)
        dbport_logger = logging.getLogger("dbport")
        handler = dbport_logger.handlers[0]
        assert handler.console is console  # type: ignore[attr-defined]

    def test_stdlib_fallback_when_rich_unavailable(self, monkeypatch):
        """Falls back to stdlib logging when rich is not installed."""
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "rich" in name:
                raise ImportError("No module named 'rich'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        dbport_logging._configured = False
        setup_logging()
        assert dbport_logging._configured is True
