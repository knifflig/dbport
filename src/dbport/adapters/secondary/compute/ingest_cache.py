"""Snapshot-based ingest caching.

Checks whether a table has already been ingested at the current Iceberg
snapshot, allowing IngestService to skip re-loading unchanged inputs.
"""

from __future__ import annotations

from ....domain.entities.input import IngestRecord


def should_skip_ingest(
    current_record: IngestRecord | None,
    current_snapshot_id: int | None,
    table_address: str,
    filters: dict[str, str] | None,
) -> bool:
    """Return True if the input is already ingested at the current snapshot.

    Skips if:
    - A record exists for this table_address + filters combination
    - The recorded snapshot_id matches the current warehouse snapshot
    """
    if current_record is None:
        return False
    if current_snapshot_id is None:
        return False
    if current_record.last_snapshot_id != current_snapshot_id:
        return False
    if current_record.table_address != table_address:
        return False
    if current_record.filters != filters:
        return False
    return True
