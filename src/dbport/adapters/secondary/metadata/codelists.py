"""Codelist CSV generation from DuckDB — returns in-memory bytes."""

from __future__ import annotations

import csv
import io
import logging
from typing import Any

logger = logging.getLogger(__name__)


def generate_csv_for_column(
    compute: Any,
    output_table: str,
    column_name: str,
) -> bytes:
    """Build a ``code,name`` CSV from distinct values in *output_table.column_name*.

    Returns the CSV content as UTF-8 encoded bytes (no file is written).
    """
    rows = compute.execute(
        f"SELECT DISTINCT {column_name} FROM {output_table} ORDER BY {column_name}"
    ).fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["code", "name"])
    for row in rows:
        code = str(row[0]) if row[0] is not None else ""
        writer.writerow([code, code])

    data = buf.getvalue().encode("utf-8")
    logger.debug("generate_csv_for_column: %d rows for %s.%s", len(rows), output_table, column_name)
    return data


def generate_csv_for_attached(
    compute: Any,
    table_address: str,
) -> bytes:
    """Export an entire DuckDB table as CSV bytes.

    The table should already be loaded into DuckDB (via ``client.load()``).
    Returns the CSV content as UTF-8 encoded bytes (no file is written).
    """
    result = compute.execute(f"SELECT * FROM {table_address}")
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([str(v) if v is not None else "" for v in row])

    data = buf.getvalue().encode("utf-8")
    logger.debug("generate_csv_for_attached: %d rows from %s", len(rows), table_address)
    return data
