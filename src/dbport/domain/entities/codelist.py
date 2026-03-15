"""Codelist and column metadata value objects."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class CodelistEntry(BaseModel):
    """Codelist configuration for a single output column."""

    model_config = ConfigDict(frozen=True)

    column_name: str
    column_pos: int

    codelist_id: str
    """Identifier for the codelist (defaults to column_name)."""

    codelist_type: str | None = None
    """e.g. 'categorical', 'hierarchical'. Inferred from SQL type where possible."""

    codelist_kind: str | None = None
    """e.g. 'reference', 'derived'."""

    codelist_labels: dict[str, str] | None = None
    """Optional multilingual labels keyed by language code."""

    attach_table: str | None = None
    """If set, use this DuckDB table (loaded via client.load()) as the codelist source."""


class ColumnCodelist(BaseModel):
    """All codelist entries for a dataset, keyed by column_name."""

    model_config = ConfigDict(frozen=True)

    entries: dict[str, CodelistEntry] = {}
