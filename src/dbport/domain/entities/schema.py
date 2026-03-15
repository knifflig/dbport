"""Output schema value objects."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class SqlDdl(BaseModel):
    """A SQL DDL statement that defines the output table."""

    model_config = ConfigDict(frozen=True)

    statement: str
    """The raw CREATE TABLE DDL, fully resolved (no file references)."""


class ColumnDef(BaseModel):
    """A single column parsed from the output DDL."""

    model_config = ConfigDict(frozen=True)

    name: str
    pos: int
    """Zero-based ordinal position in the DDL column list."""
    sql_type: str
    """SQL type string as declared in the DDL (e.g. 'VARCHAR', 'TIMESTAMPTZ')."""


class DatasetSchema(BaseModel):
    """The complete parsed schema for an output dataset."""

    model_config = ConfigDict(frozen=True)

    ddl: SqlDdl
    columns: tuple[ColumnDef, ...]
    source: str = "local"
    """Origin of the schema: 'local' (user-declared) or 'warehouse' (auto-detected)."""
