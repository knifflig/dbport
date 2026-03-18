"""CLI contract tests -- lock the ``dbp`` command tree for 0.1.0.

These tests prevent accidental renames, reshuffles, or flag changes in the
CLI.  Any test failure here signals that the CLI compatibility contract is
shifting and must be reviewed deliberately.
"""

from __future__ import annotations

import inspect
import re

import typer
import typer.testing

from dbport.cli.main import app

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

runner = typer.testing.CliRunner()


def _collect_command_names(typer_app: typer.Typer) -> set[str]:
    """Return the set of registered command/sub-app names."""
    names: set[str] = set()
    for cmd in typer_app.registered_commands:
        names.add(cmd.name or cmd.callback.__name__)
    for group in typer_app.registered_groups:
        names.add(group.name or group.typer_instance.info.name or "")
    return names


# ---------------------------------------------------------------------------
# Top-level command tree
# ---------------------------------------------------------------------------


class TestTopLevelCommands:
    """The root ``dbp`` app must expose exactly these commands."""

    EXPECTED = {"init", "status", "model", "config"}

    def test_top_level_command_names(self) -> None:
        """Test top-level command names match expected set."""
        names = _collect_command_names(app)
        assert names == self.EXPECTED

    def test_help_exits_zero(self) -> None:
        """Test help exits with code zero."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

    def test_version_flag(self) -> None:
        """Test version flag shows version."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "dbp" in result.output


# ---------------------------------------------------------------------------
# Global options
# ---------------------------------------------------------------------------


class TestGlobalOptions:
    """Global flags that must be present on every invocation."""

    EXPECTED_FLAGS = {
        "--version",
        "--project",
        "--lockfile",
        "--model",
        "--verbose",
        "--quiet",
        "--json",
        "--no-color",
    }

    def test_global_options_in_help(self) -> None:
        """Test all global options appear in help output."""
        result = runner.invoke(app, ["--help"])
        output = _strip_ansi(result.output)
        for flag in self.EXPECTED_FLAGS:
            assert flag in output, f"Missing global option: {flag}"


# ---------------------------------------------------------------------------
# ``dbp status`` subcommands
# ---------------------------------------------------------------------------


class TestStatusSubcommands:
    """Tests for status subcommands."""

    def test_status_check_exists(self) -> None:
        """Test status check subcommand exists."""
        result = runner.invoke(app, ["status", "check", "--help"])
        assert result.exit_code == 0
        assert "--strict" in _strip_ansi(result.output)

    def test_status_flags(self) -> None:
        """Test status flags are present."""
        result = runner.invoke(app, ["status", "--help"])
        output = _strip_ansi(result.output)
        for flag in ("--inputs", "--history", "--raw"):
            assert flag in output, f"Missing status flag: {flag}"


# ---------------------------------------------------------------------------
# ``dbp model`` subcommands
# ---------------------------------------------------------------------------


class TestModelSubcommands:
    """Tests for model subcommands."""

    EXPECTED = {"sync", "load", "exec", "publish", "run"}

    def test_model_subcommand_names(self) -> None:
        """Test model subcommand names match expected set."""
        from dbport.cli.commands.model import model_app

        names = _collect_command_names(model_app)
        assert names == self.EXPECTED

    def test_model_sync_help(self) -> None:
        """Test model sync help exits zero."""
        result = runner.invoke(app, ["model", "sync", "--help"])
        assert result.exit_code == 0

    def test_model_load_flags(self) -> None:
        """Test model load flags are present."""
        result = runner.invoke(app, ["model", "load", "--help"])
        assert result.exit_code == 0
        assert "--update" in _strip_ansi(result.output)

    def test_model_exec_flags(self) -> None:
        """Test model exec flags are present."""
        result = runner.invoke(app, ["model", "exec", "--help"])
        assert result.exit_code == 0
        output = _strip_ansi(result.output)
        for flag in ("--target", "--timing"):
            assert flag in output, f"Missing exec flag: {flag}"

    def test_model_publish_flags(self) -> None:
        """Test model publish flags are present."""
        result = runner.invoke(app, ["model", "publish", "--help"])
        assert result.exit_code == 0
        output = _strip_ansi(result.output)
        for flag in ("--version", "--dry-run", "--refresh", "--message"):
            assert flag in output, f"Missing publish flag: {flag}"

    def test_model_run_flags(self) -> None:
        """Test model run flags are present."""
        result = runner.invoke(app, ["model", "run", "--help"])
        assert result.exit_code == 0
        output = _strip_ansi(result.output)
        for flag in (
            "--version",
            "--target",
            "--timing",
            "--dry-run",
            "--refresh",
        ):
            assert flag in output, f"Missing run flag: {flag}"


# ---------------------------------------------------------------------------
# ``dbp config`` subcommands
# ---------------------------------------------------------------------------


class TestConfigSubcommands:
    """Tests for config subcommands."""

    EXPECTED_TOP = {"default", "model"}

    def test_config_subcommand_names(self) -> None:
        """Test config subcommand names match expected set."""
        from dbport.cli.commands.config import config_app

        names = _collect_command_names(config_app)
        assert names == self.EXPECTED_TOP

    def test_config_default_subcommands(self) -> None:
        """Test config default subcommand names."""
        from dbport.cli.commands.config import default_app

        names = _collect_command_names(default_app)
        assert names == {"model", "folder", "hook"}

    def test_config_model_subcommands(self) -> None:
        """Test config model subcommand names."""
        from dbport.cli.commands.config import model_app

        names = _collect_command_names(model_app)
        assert names == {"version", "input", "schema", "columns"}

    def test_config_columns_subcommands(self) -> None:
        """Test config columns subcommand names."""
        from dbport.cli.commands.config import columns_app

        names = _collect_command_names(columns_app)
        assert names == {"set", "attach"}


# ---------------------------------------------------------------------------
# ``dbp init`` flags
# ---------------------------------------------------------------------------


class TestInitFlags:
    """Tests for init command flags."""

    def test_init_flags(self) -> None:
        """Test init flags are present."""
        result = runner.invoke(app, ["init", "--help"])
        assert result.exit_code == 0
        output = _strip_ansi(result.output)
        for flag in ("--template", "--dataset", "--agency", "--path", "--force"):
            assert flag in output, f"Missing init flag: {flag}"


# ---------------------------------------------------------------------------
# No stale command references
# ---------------------------------------------------------------------------


class TestNoStaleReferences:
    """Ensure removed commands are not referenced in user-facing output."""

    def test_no_project_sync_in_init_help(self) -> None:
        """The removed ``dbp project sync`` must not appear anywhere."""
        from dbport.cli.commands import init as init_mod

        source = inspect.getsource(init_mod)
        assert "dbp project sync" not in source
        assert "project sync" not in source
