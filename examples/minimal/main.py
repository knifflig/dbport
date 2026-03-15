"""Minimal DBPort example.

Loads one input (estat.nama_10r_3empers), runs a trivial transform,
and publishes the result to test.table1.
"""

from dbport import DBPort

with DBPort(agency="test", dataset_id="table1") as port:
    # 1. Ensure target schema exists in DuckDB (not created by default)
    port.execute("CREATE SCHEMA IF NOT EXISTS test")

    # 2. Declare the output schema
    port.schema("sql/create_output.sql")

    # 3. Load one input from the warehouse (no filters — scope in SQL)
    port.load("estat.nama_10r_3empers")

    # 4. Populate the output table
    port.execute("sql/transform.sql")

    # 5. Publish to test.table1
    port.publish(version="2026-03-13")
