"""Root Typer app — command registration and global options."""

from __future__ import annotations

import typer

from .context import CliContext, resolve_context
from .options import (
    JsonOption,
    LockfileOption,
    ModelOption,
    NoColorOption,
    ProjectOption,
    QuietOption,
    VerboseOption,
)
from .render import configure_cli_logging, set_no_color

app = typer.Typer(
    name="dbp",
    help="DBPort CLI — build and run reproducible DuckDB data products.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)


def _version_callback(value: bool) -> None:
    if value:
        from importlib.metadata import version

        try:
            v = version("dbport")
        except Exception:
            v = "unknown"
        typer.echo(f"dbp {v}")
        raise typer.Exit()


@app.callback()
def main_callback(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show version and exit.",
    ),
    project: str | None = ProjectOption,
    lockfile: str | None = LockfileOption,
    model: str | None = ModelOption,
    verbose: bool = VerboseOption,
    quiet: bool = QuietOption,
    json_output: bool = JsonOption,
    no_color: bool = NoColorOption,
) -> None:
    """Global options for all DBPort commands."""
    set_no_color(no_color)
    configure_cli_logging(verbose=verbose, quiet=quiet)
    ctx.ensure_object(dict)
    ctx.obj["cli_ctx"] = resolve_context(
        project=project,
        lockfile=lockfile,
        model=model,
        verbose=verbose,
        quiet=quiet,
        json_output=json_output,
        no_color=no_color,
    )


def get_cli_ctx(ctx: typer.Context) -> CliContext:
    """Extract CliContext from a Typer context."""
    return ctx.obj["cli_ctx"]


# -- Register commands --------------------------------------------------------

from .commands import config, init, model, status  # noqa: E402

app.command(name="init")(init.init_cmd)
app.add_typer(status.status_app, name="status")
app.add_typer(model.model_app, name="model")
app.add_typer(config.config_app, name="config")
