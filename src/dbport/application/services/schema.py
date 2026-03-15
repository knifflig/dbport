"""DefineSchemaService — client.schema(...) use case."""

from __future__ import annotations

import re
from pathlib import Path

from ...domain.entities.schema import ColumnDef, DatasetSchema, SqlDdl
from ...domain.ports.compute import ICompute
from ...domain.ports.lock import ILockStore


class DefineSchemaService:
    """Parse and apply a DDL to DuckDB; persist columns to dbport.lock.

    Responsibilities:
    - Accept a DDL string or resolve a .sql file path to a string
    - Parse column names, positions, and SQL types from the DDL
    - Execute `CREATE OR REPLACE TABLE` in DuckDB
    - Persist the schema (DDL + columns) to dbport.lock
    - Initialise default CodelistEntry records for each column
    """

    def __init__(self, compute: ICompute, lock: ILockStore) -> None:
        self._compute = compute
        self._lock = lock

    def execute(self, ddl_or_path: str, base_dir: str) -> DatasetSchema:
        """Apply and persist the schema. Returns the parsed DatasetSchema."""
        stripped = ddl_or_path.strip()

        # Resolve file path or use inline DDL
        if stripped.lower().endswith(".sql"):
            path = Path(stripped)
            if not path.is_absolute():
                path = Path(base_dir) / path
                resolved = path.resolve()
                base_resolved = Path(base_dir).resolve()
                if not str(resolved).startswith(str(base_resolved)):
                    raise ValueError(f"SQL file path escapes base directory: {stripped!r}")
            ddl = path.read_text(encoding="utf-8").strip()
        else:
            ddl = stripped

        # Validate DDL before executing — must contain CREATE TABLE
        m = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([\w.]+)",
            ddl,
            re.IGNORECASE,
        )
        if not m:
            raise ValueError(f"Could not parse table address from DDL: {ddl[:120]!r}")

        # Execute in DuckDB
        self._compute.execute(ddl)
        table_address = m.group(1)

        parts = table_address.split(".", 1)
        if len(parts) == 2:
            schema_name, table_name = parts
        else:
            schema_name, table_name = "main", parts[0]

        # Query DuckDB for the canonical column list
        rows = self._compute.execute(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [schema_name, table_name],
        ).fetchall()

        columns = tuple(
            ColumnDef(name=str(row[0]), pos=i, sql_type=str(row[1]))
            for i, row in enumerate(rows)
        )

        schema = DatasetSchema(ddl=SqlDdl(statement=ddl), columns=columns, source="local")
        self._lock.write_schema(schema)
        return schema
