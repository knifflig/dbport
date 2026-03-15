"""TransformService — client.execute(...) use case."""

from __future__ import annotations

from pathlib import Path

from ...domain.ports.compute import ICompute


class TransformService:
    """Execute a SQL string or .sql file in DuckDB.

    Responsibilities:
    - Accept an inline SQL string or a path to a .sql file
    - Resolve file paths relative to the caller's working directory
    - Delegate execution to ICompute
    """

    def __init__(self, compute: ICompute) -> None:
        self._compute = compute

    def execute(self, sql_or_path: str, base_dir: str) -> None:
        """Run the SQL. Raises on execution errors."""
        stripped = sql_or_path.strip()
        if stripped.lower().endswith(".sql"):
            path = Path(stripped)
            if not path.is_absolute():
                path = Path(base_dir) / path
                resolved = path.resolve()
                base_resolved = Path(base_dir).resolve()
                if not str(resolved).startswith(str(base_resolved)):
                    raise ValueError(f"SQL file path escapes base directory: {stripped!r}")
            self._compute.execute_file(str(path))
        else:
            self._compute.execute(stripped)
