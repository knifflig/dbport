"""Tests for the root CLI app."""

from typer.testing import CliRunner

from dbport.cli.main import app, get_cli_ctx

runner = CliRunner()


class TestAppHelp:
    def test_help_shows_commands(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "init" in result.output
        assert "status" in result.output
        assert "check" in result.output
        assert "schema" in result.output
        assert "load" in result.output
        assert "run" in result.output
        assert "publish" in result.output

    def test_no_args_shows_help(self):
        result = runner.invoke(app, [])
        # Typer returns exit code 2 for no-args-is-help
        assert result.exit_code in (0, 2)
        assert "Usage" in result.output

    def test_version_flag(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "dbp" in result.output

    def test_version_fallback_on_error(self):
        """When importlib.metadata.version raises, fallback to 0.1.0."""
        from unittest.mock import patch

        with patch("importlib.metadata.version", side_effect=Exception("not installed")):
            result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_config_subcommand_in_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "config" in result.output

    def test_config_help(self):
        result = runner.invoke(app, ["config", "--help"])
        assert result.exit_code == 0
        assert "default" in result.output
        assert "info" in result.output

    def test_no_color_flag(self, tmp_path):
        lock = tmp_path / "dbport.lock"
        lock.write_text("# ok\n")
        result = runner.invoke(app, [
            "--no-color",
            "--lockfile", str(lock),
            "check",
        ])
        assert result.exit_code == 0


class TestCliEntrypoint:
    def test_main_function_importable(self):
        from dbport.cli import main
        assert callable(main)

    def test_main_invokes_app(self):
        """Calling main() should invoke the typer app."""
        from unittest.mock import patch
        import dbport.cli as cli_mod

        with patch.object(cli_mod, "app") as mock_app:
            cli_mod.main()
            mock_app.assert_called_once()
