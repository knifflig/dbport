"""AutoSchemaService — auto-detect output schema from warehouse on init."""

from __future__ import annotations

import logging
from typing import Any

from ...domain.entities.schema import ColumnDef, DatasetSchema, SqlDdl
from ...domain.ports.catalog import ICatalog
from ...domain.ports.compute import ICompute
from ...domain.ports.lock import ILockStore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PyArrow type string → DuckDB SQL type
# ---------------------------------------------------------------------------

_ARROW_TO_DUCKDB: dict[str, str] = {
    "string": "VARCHAR",
    "large_string": "VARCHAR",
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "uint8": "UTINYINT",
    "uint16": "USMALLINT",
    "uint32": "UINTEGER",
    "uint64": "UBIGINT",
    "float16": "FLOAT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "bool": "BOOLEAN",
    "date32[day]": "DATE",
    "binary": "BLOB",
    "large_binary": "BLOB",
}


def _arrow_type_to_duckdb(arrow_type: Any) -> str:
    """Convert a PyArrow type to a DuckDB SQL type string."""
    type_str = str(arrow_type)

    # Direct lookup
    if type_str in _ARROW_TO_DUCKDB:
        return _ARROW_TO_DUCKDB[type_str]

    # Timestamp variants
    if type_str.startswith("timestamp["):
        if "tz=" in type_str:
            return "TIMESTAMPTZ"
        return "TIMESTAMP"

    # Decimal
    if type_str.startswith("decimal128("):
        return type_str.replace("decimal128", "DECIMAL")

    # Fallback
    return "VARCHAR"


class AutoSchemaService:
    """Detect output schema from warehouse and create table in DuckDB.

    Runs during DBPort init. If the output table already exists in the
    warehouse, its schema is used to create the local DuckDB table and
    persist the schema to dbport.lock (with source='warehouse').

    Skipped when the lock file already has a user-declared schema
    (source='local').
    """

    def __init__(
        self,
        catalog: ICatalog,
        compute: ICompute,
        lock: ILockStore,
    ) -> None:
        self._catalog = catalog
        self._compute = compute
        self._lock = lock

    def execute(self, table_address: str) -> DatasetSchema | None:
        """Auto-detect schema from warehouse. Returns DatasetSchema or None.

        Returns None when:
        - The lock already has a user-declared schema (source='local')
        - The table does not exist in the warehouse
        """
        # Skip if user already declared schema explicitly
        existing = self._lock.read_schema()
        if existing is not None and existing.source == "local":
            logger.debug(
                "Auto-schema skipped — local schema already declared for %s",
                table_address,
            )
            return None

        # Check warehouse
        if not self._catalog.table_exists(table_address):
            logger.debug(
                "Table %s not found in warehouse", table_address
            )
            return None

        # Fetch PyArrow schema from warehouse
        arrow_schema = self._catalog.load_arrow_schema(table_address)

        # Build ColumnDef tuples from PyArrow fields
        columns = tuple(
            ColumnDef(
                name=field.name,
                pos=i,
                sql_type=_arrow_type_to_duckdb(field.type),
            )
            for i, field in enumerate(arrow_schema)
        )

        # Synthesize DDL
        col_defs = ", ".join(f"{c.name} {c.sql_type}" for c in columns)
        ddl_statement = f"CREATE OR REPLACE TABLE {table_address} ({col_defs})"

        # Execute in DuckDB — create the output table structure
        ns = table_address.split(".", 1)[0] if "." in table_address else None
        if ns:
            self._compute.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
        self._compute.execute(ddl_statement)

        # Build and persist schema
        schema = DatasetSchema(
            ddl=SqlDdl(statement=ddl_statement),
            columns=columns,
            source="warehouse",
        )
        self._lock.write_schema(schema)

        logger.debug(
            "Detected %d columns from warehouse for %s",
            len(columns),
            table_address,
        )
        return schema
