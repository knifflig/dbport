"""Schema drift detection — compare local DDL against an existing warehouse schema."""

from __future__ import annotations

import pyarrow as pa


class SchemaDriftError(Exception):
    """Raised when the local schema is incompatible with the warehouse schema."""


_EQUIVALENT_TYPES = {"large_string": "string", "large_binary": "binary"}


def _normalize_type(t: str) -> str:
    return _EQUIVALENT_TYPES.get(t, t)


def check_schema_drift(local_arrow_schema: pa.Schema, warehouse_arrow_schema: pa.Schema) -> None:
    """Compare two PyArrow schemas. Raises SchemaDriftError with a diff if incompatible."""
    local_fields = {f.name: _normalize_type(str(f.type)) for f in local_arrow_schema}
    warehouse_fields = {f.name: _normalize_type(str(f.type)) for f in warehouse_arrow_schema}

    added = set(local_fields) - set(warehouse_fields)
    removed = set(warehouse_fields) - set(local_fields)
    changed = {
        n for n in local_fields if n in warehouse_fields and local_fields[n] != warehouse_fields[n]
    }

    if added or removed or changed:
        diff_lines = []
        for n in sorted(added):
            diff_lines.append(f"  + {n}: {local_fields[n]}  (new column, not in warehouse)")
        for n in sorted(removed):
            diff_lines.append(f"  - {n}: {warehouse_fields[n]}  (missing locally)")
        for n in sorted(changed):
            diff_lines.append(
                f"  ~ {n}: {warehouse_fields[n]} -> {local_fields[n]}  (type changed)"
            )
        raise SchemaDriftError("Schema drift detected:\n" + "\n".join(diff_lines))
