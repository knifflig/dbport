"""MetadataAdapter — implements IMetadataStore.

Builds metadata JSON in-memory from domain state, generates codelist CSV
bytes via DuckDB, and attaches both to Iceberg table properties.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ....domain.entities.codelist import ColumnCodelist
    from ....domain.entities.dataset import DatasetKey
    from ....domain.entities.input import IngestRecord
    from ....domain.entities.version import DatasetVersion

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class MetadataAdapter:
    """Implements IMetadataStore.

    All codelist and metadata payloads are generated in-memory — no
    intermediate files are written to disk.
    """

    def build_metadata_json(
        self,
        key: DatasetKey,
        version: DatasetVersion,
        inputs: list[IngestRecord],
        codelists: ColumnCodelist,
        previous_metadata_json: str | None = None,
        snapshot_id: int | None = None,
    ) -> bytes:
        """Build metadata.json payload in-memory. Returns UTF-8 bytes.

        *snapshot_id* is appended to the ``versions`` list so that downstream
        ``client.load()`` callers can resolve any published version to its
        exact Iceberg snapshot without a data scan.
        """
        now = _utc_now_iso()

        # Preserve created_at and versions from previous metadata
        existing: dict[str, Any] = {}
        if previous_metadata_json:
            try:
                existing = json.loads(previous_metadata_json) or {}
            except Exception:
                existing = {}

        created_at = str(existing.get("created_at") or "").strip() or now
        existing_versions: list[dict[str, Any]] = existing.get("versions") or []

        # Upsert this version into the versions list (keyed by version string)
        new_version_entry: dict[str, Any] = {
            "version": version.version,
            "published_at": now,
        }
        if snapshot_id is not None:
            new_version_entry["iceberg_snapshot_id"] = snapshot_id
        if version.params:
            new_version_entry["params"] = version.params

        updated_versions = [
            v for v in existing_versions if v.get("version") != version.version
        ]
        updated_versions.append(new_version_entry)

        # Build inputs payload
        inputs_payload = []
        for r in inputs:
            item: dict[str, Any] = {"table_address": r.table_address}
            if r.filters:
                item["filters"] = r.filters
            if r.last_snapshot_id is not None:
                item["last_snapshot_id"] = r.last_snapshot_id
            if r.rows_loaded is not None:
                item["rows_loaded"] = r.rows_loaded
            inputs_payload.append(item)

        # Build codelists payload
        codelists_payload = []
        for col_name, entry in codelists.entries.items():
            cl_item: dict[str, Any] = {
                "column_name": entry.column_name,
                "column_pos": entry.column_pos,
                "codelist_id": entry.codelist_id,
            }
            if entry.codelist_type is not None:
                cl_item["codelist_type"] = entry.codelist_type
            if entry.codelist_kind is not None:
                cl_item["codelist_kind"] = entry.codelist_kind
            if entry.codelist_labels is not None:
                cl_item["codelist_labels"] = entry.codelist_labels
            if entry.attach_table is not None:
                cl_item["source_table"] = entry.attach_table
            codelists_payload.append(cl_item)

        metadata_obj: dict[str, Any] = {
            "schema_version": 1,
            "agency_id": key.agency,
            "dataset_id": key.dataset_id,
            "created_at": created_at,
            "last_fetched_at": now,
            "last_updated_at": now,
            "last_updated_data_at": version.version,
            "params": version.params or {},
            "inputs": inputs_payload,
            "codelists": codelists_payload,
            "versions": updated_versions,
        }

        data = (json.dumps(metadata_obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        logger.debug("MetadataAdapter.build_metadata_json: built %d bytes", len(data))
        return data

    def generate_codelist_bytes(
        self,
        codelists: ColumnCodelist,
        compute: Any,
        output_table: str,
    ) -> dict[str, bytes]:
        """Generate in-memory CSV bytes for each column's codelist.

        Returns {column_name: csv_bytes}.
        """
        from .codelists import generate_csv_for_attached, generate_csv_for_column

        result: dict[str, bytes] = {}

        failed: list[str] = []
        for col_name, entry in codelists.entries.items():
            try:
                if entry.attach_table:
                    result[col_name] = generate_csv_for_attached(compute, entry.attach_table)
                else:
                    result[col_name] = generate_csv_for_column(compute, output_table, col_name)
            except Exception as exc:
                failed.append(col_name)
                logger.error(
                    "generate_codelist_bytes: failed for column %s: %s", col_name, exc
                )

        if failed:
            logger.warning(
                "Codelist generation failed for %d column(s): %s",
                len(failed), ", ".join(failed),
            )

        return result

    def attach_to_table(
        self,
        table_address: str,
        metadata_bytes: bytes,
        codelist_bytes: dict[str, bytes],
        codelist_entries: dict[str, Any],
        catalog: Any,
    ) -> None:
        """Embed metadata and codelist payloads in Iceberg table properties."""
        from .attach import attach_codelist_csv, attach_metadata_json

        attach_metadata_json(catalog, table_address, metadata_bytes)

        for col_name, csv_bytes in codelist_bytes.items():
            entry = codelist_entries.get(col_name)
            attach_codelist_csv(catalog, table_address, col_name, csv_bytes, entry)
