"""DBPort CLI — ``dbp`` console entrypoint."""

from .main import app


def main() -> None:
    """Run the dbp CLI application."""
    app()
