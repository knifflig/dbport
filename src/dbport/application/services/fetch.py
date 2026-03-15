"""FetchService — client.fetch() use case."""

from __future__ import annotations

from datetime import UTC, datetime

from ...domain.entities.dataset import DatasetKey
from ...domain.ports.catalog import ICatalog


class FetchService:
    """Update last_fetched_at in the warehouse without uploading data.

    Responsibilities:
    - Write only `dbport.last_fetched_at` to Iceberg table properties
    - No data upload; no new snapshot
    - Used when the model runs but determines inputs are unchanged
    """

    def __init__(self, key: DatasetKey, catalog: ICatalog) -> None:
        self._key = key
        self._catalog = catalog

    def execute(self) -> datetime:
        """Update last_fetched_at. Returns the timestamp written."""
        now = datetime.now(UTC).replace(microsecond=0)
        ts_str = now.isoformat().replace("+00:00", "Z")
        table_address = self._key.table_address
        try:
            if self._catalog.table_exists(table_address):
                self._catalog.update_table_properties(
                    table_address,
                    {"dbport.last_fetched_at": ts_str},
                )
        except Exception:
            pass
        return now
