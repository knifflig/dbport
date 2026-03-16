"""Minimal DBPort example.

Loads one input (estat.nama_10r_3empers), runs a trivial transform,
and publishes the result to test.table1.

Works both as a standalone script (``python main.py``) and as a CLI
run hook (``dbp run test.table1``).
"""

from dbport import DBPort


def run(port):
    """Model logic — called by both CLI and standalone execution."""
    # 1. Ensure target schema exists in DuckDB (not created by default)
    port.execute("CREATE SCHEMA IF NOT EXISTS test")

    # 2. Declare the output schema
    port.schema("sql/create_output.sql")

    # 3. Load one input from the warehouse (no filters — scope in SQL)
    port.load("estat.nama_10r_3empers")

    # 4. Populate the output table
    port.execute("sql/transform.sql")


if __name__ == "__main__":
    with DBPort(agency="test", dataset_id="table1") as port:
        run(port)
        port.publish(version="2026-03-13")
