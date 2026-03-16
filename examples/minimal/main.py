"""Minimal DBPort example and CLI-safe model hook for table1.

When executed through the CLI, DBPort injects an active ``port`` and calls
``run(port)``. When executed directly as a Python script, this file opens its
own DBPort instance.
"""

from dbport import DBPort


def run(port: DBPort) -> None:
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


if __name__ == "__main__":
    with DBPort(agency="test", dataset_id="table1") as port:
        run(port)
        # Standalone client usage can still own publish explicitly.
        port.publish(version="2026-03-16", params={"wstatus": "EMP"}, mode="dry")
        port.publish(version="2026-03-16", params={"wstatus": "EMP"})
        port.publish(version="2026-03-16", params={"wstatus": "EMP"}, mode="refresh")
