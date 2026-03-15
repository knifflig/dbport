"""Attach metadata and codelist payloads to Iceberg table properties."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def attach_metadata_json(
    catalog: Any,
    table_address: str,
    metadata_bytes: bytes,
) -> None:
    """Compress *metadata_bytes* with gzip+base64 and write to table properties.

    Keys written:
    - dbport.metadata_json      : uncompressed JSON string
    - dbport.metadata_json_gz   : gzip+base64 compressed JSON
    - dbport.metadata_sha256    : SHA-256 hex digest of the uncompressed JSON
    """
    sha = hashlib.sha256(metadata_bytes).hexdigest()
    md_gz_b64 = base64.b64encode(gzip.compress(metadata_bytes)).decode("ascii")

    catalog.update_table_properties(
        table_address,
        {
            "dbport.metadata_json": metadata_bytes.decode("utf-8"),
            "dbport.metadata_json_gz": md_gz_b64,
            "dbport.metadata_sha256": sha,
        },
    )
    logger.debug("attach_metadata_json: attached metadata to %s (sha256=%s)", table_address, sha)


def attach_codelist_csv(
    catalog: Any,
    table_address: str,
    column_name: str,
    csv_bytes: bytes,
    codelist_entry: Any = None,
) -> None:
    """Compress codelist CSV bytes and write to column documentation in Iceberg."""
    sha = hashlib.sha256(csv_bytes).hexdigest()
    csv_gz_b64 = base64.b64encode(gzip.compress(csv_bytes)).decode("ascii")

    payload: dict[str, Any] = {
        "dbport": {
            "csv_sha256": sha,
            "csv_gzip_base64": csv_gz_b64,
        }
    }

    if codelist_entry is not None:
        payload["dbport"].update(
            {
                "codelist_id": getattr(codelist_entry, "codelist_id", None),
                "codelist_kind": getattr(codelist_entry, "codelist_kind", None),
                "codelist_type": getattr(codelist_entry, "codelist_type", None),
                "codelist_labels": getattr(codelist_entry, "codelist_labels", None),
            }
        )

    doc_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    update_fn = getattr(catalog, "update_column_docs", None)
    if callable(update_fn):
        update_fn(table_address, {column_name: doc_json})
        logger.debug(
            "attach_codelist_csv: attached column %s on %s", column_name, table_address
        )
    else:
        logger.warning(
            "attach_codelist_csv: catalog has no update_column_docs; skipping column %s",
            column_name,
        )
