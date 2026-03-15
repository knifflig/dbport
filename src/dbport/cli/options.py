"""Shared Typer option definitions for global CLI flags."""

from __future__ import annotations

import typer

# Global options — used in the main app callback
ProjectOption = typer.Option(None, "--project", help="Project root directory.")
LockfileOption = typer.Option(None, "--lockfile", help="Path to dbport.lock.")
ModelOption = typer.Option(None, "--model", help="Model directory relative to project root.")
VerboseOption = typer.Option(False, "--verbose", "-v", help="Increase output verbosity.")
QuietOption = typer.Option(False, "--quiet", "-q", help="Reduce output.")
JsonOption = typer.Option(False, "--json", help="Output as structured JSON.")
NoColorOption = typer.Option(False, "--no-color", help="Disable color output.")
