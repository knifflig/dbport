"""ColumnRegistry and ColumnConfig — fluent column metadata API.

Usage:
    client.columns.geo.meta(codelist_id="NUTS2024", codelist_kind="hierarchical")
    client.columns.geo.attach(table="wifor.cl_nuts2024")  # DuckDB table loaded via client.load()
"""

from __future__ import annotations

from ...domain.entities.codelist import CodelistEntry
from ...domain.ports.lock import ILockStore


class ColumnConfig:
    """Fluent configurator for a single output column's codelist metadata."""

    def __init__(self, column_name: str, lock: ILockStore) -> None:
        self._name = column_name
        self._lock = lock

    def meta(
        self,
        *,
        codelist_id: str | None = None,
        codelist_type: str | None = None,
        codelist_kind: str | None = None,
        codelist_labels: dict[str, str] | None = None,
    ) -> ColumnConfig:
        """Override codelist metadata for this column. Persists to dbport.lock immediately.

        Returns self for optional chaining.
        """
        existing = self._lock.read_codelist_entries().get(self._name)
        if existing is None:
            existing = CodelistEntry(
                column_name=self._name,
                column_pos=0,
                codelist_id=self._name,
            )
        overrides: dict = {}
        if codelist_id is not None:
            overrides["codelist_id"] = codelist_id
        if codelist_type is not None:
            overrides["codelist_type"] = codelist_type
        if codelist_kind is not None:
            overrides["codelist_kind"] = codelist_kind
        if codelist_labels is not None:
            overrides["codelist_labels"] = codelist_labels
        updated = existing.model_copy(update=overrides)
        self._lock.write_codelist_entry(updated)
        return self

    def attach(self, *, table: str) -> ColumnConfig:
        """Set a DuckDB table as the codelist source for this column.

        The table should already be loaded into DuckDB (via client.load()).
        On publish(), the full table is exported as the codelist instead of
        auto-generating from distinct output values. Persists to dbport.lock.

        Returns self for optional chaining.
        """
        existing = self._lock.read_codelist_entries().get(self._name)
        if existing is None:
            existing = CodelistEntry(
                column_name=self._name,
                column_pos=0,
                codelist_id=self._name,
            )
        updated = existing.model_copy(update={"attach_table": table})
        self._lock.write_codelist_entry(updated)
        return self


class ColumnRegistry:
    """Attribute-style access to per-column ColumnConfig instances.

    Accessed via client.columns.<column_name>.
    Columns are populated when client.schema(...) is called.
    """

    def __init__(self, lock: ILockStore) -> None:
        self._lock = lock
        self._cache: dict[str, ColumnConfig] = {}

    def __getattr__(self, column_name: str) -> ColumnConfig:
        if column_name.startswith("_"):
            raise AttributeError(column_name)
        if column_name not in self._cache:
            self._cache[column_name] = ColumnConfig(column_name, self._lock)
        return self._cache[column_name]

    def _refresh(self) -> None:
        """Clear the cache so new columns from schema() are picked up."""
        self._cache.clear()
