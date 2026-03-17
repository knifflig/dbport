# Python Workflow

A complete Python example demonstrating the full DBPort lifecycle.

## Project structure

```
my_model/
  main.py
  sql/
    create_output.sql
    staging.sql
    transform.sql
  data/                    # created automatically
```

## `main.py`

```python
from dbport import DBPort


def run(port: DBPort) -> None:
    """Model logic — also callable from the CLI via dbp model run."""

    # Ensure the target schema exists in DuckDB
    port.execute("CREATE SCHEMA IF NOT EXISTS test")

    # Declare the output schema from a SQL file
    port.schema("sql/create_output.sql")

    # Configure column metadata (persisted to dbport.lock immediately)
    port.columns.geo.meta(codelist_id="GEO", codelist_kind="reference")
    port.columns.year.meta(codelist_type="categorical")

    # Load input with filters (pushed down to Iceberg scan)
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})

    # Load a codelist reference table and attach it to a column
    port.load("wifor.cl_nuts2024")
    port.columns.geo.attach(table="wifor.cl_nuts2024")

    # Multi-step transforms
    port.execute("sql/staging.sql")
    port.execute("sql/transform.sql")


if __name__ == "__main__":
    with DBPort(agency="test", dataset_id="table1") as port:
        run(port)

        # Dry run — validate schemas only
        port.publish(version="2026-03-16", params={"wstatus": "EMP"}, mode="dry")

        # Normal publish — idempotent
        port.publish(version="2026-03-16", params={"wstatus": "EMP"})

        # Refresh — overwrite existing version
        port.publish(version="2026-03-16", params={"wstatus": "EMP"}, mode="refresh")
```

## Key points

- **`run(port)` function** — the CLI can call this directly via `dbp model run`, so it works both as a standalone script and as a CLI hook (see [Hooks & Execution](../concepts/hooks.md) for details on hook resolution and dispatch)
- **Schema first** — `port.schema()` is called early, before any loads or transforms
- **Column metadata** — `.meta()` and `.attach()` configure codelist behavior per column
- **Filters** — `filters={"wstatus": "EMP"}` pushes predicates down to the Iceberg scan
- **Multi-step transforms** — SQL files are executed in sequence
- **Publish modes** — `dry` for validation, default for idempotent write, `refresh` for overwrite
