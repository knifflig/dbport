"""Minimal DBPort example — demonstrates the full Python client API.

Loads inputs from the warehouse (with filters), configures column metadata
and codelist attachments, runs multi-step transforms, and publishes with
all three modes (dry-run, normal, refresh).
"""

from dbport import DBPort

with DBPort(agency="test", dataset_id="table1") as port:
    # 1. Ensure target schema exists in DuckDB
    port.execute("CREATE SCHEMA IF NOT EXISTS test")

    # 2. Declare the output schema from a .sql file
    port.schema("sql/create_output.sql")

    # 3. Column metadata — persisted to dbport.lock immediately
    port.columns.geo.meta(codelist_id="GEO", codelist_kind="reference")
    port.columns.year.meta(codelist_type="categorical")

    # 4. Load input with filters (pushed down to Iceberg scan)
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})

    # 5. Load a codelist reference table and attach it to a column
    port.load("wifor.cl_nuts2024")
    port.columns.geo.attach(table="wifor.cl_nuts2024")

    # 6. Multi-step transforms: staging view first, then final insert
    port.execute("sql/staging.sql")
    port.execute("sql/transform.sql")

    # 7. Dry-run publish — validates schema only, no data written
    port.publish(version="2026-03-16", params={"wstatus": "EMP"}, mode="dry")

    # 8. Normal publish — idempotent, skips if version already completed
    port.publish(version="2026-03-16", params={"wstatus": "EMP"})

    # 9. Refresh publish — overwrites existing version unconditionally
    port.publish(version="2026-03-16", params={"wstatus": "EMP"}, mode="refresh")
