# Quickstart

This guide walks through a complete workflow: define an output schema, load inputs, transform data, and publish results.

## Python API

```python
from dbport import DBPort

with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:

    # 1. Declare the output schema
    port.schema("""
        CREATE OR REPLACE TABLE wifor.emp__regional_trends (
            freq     VARCHAR,
            year     DATE,
            nuts2024 VARCHAR,
            value    DOUBLE
        )
    """)

    # 2. Configure column metadata
    port.columns.nuts2024.meta(
        codelist_id="NUTS2024",
        codelist_kind="hierarchical",
    )
    port.columns.nuts2024.attach(table="wifor.cl_nuts2024")

    # 3. Load inputs from the warehouse
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
    port.load("wifor.cl_nuts2024")

    # 4. Run SQL transforms
    port.execute("sql/staging.sql")
    port.execute("sql/final_output.sql")

    # 5. Publish to the warehouse
    port.publish(version="2026-03-09", params={"wstatus": "EMP"})
```

## CLI workflow

The same workflow using the `dbp` command:

```bash
# Initialize a new model
dbp init regional_trends --agency wifor --dataset emp__regional_trends

cd regional_trends

# Apply the output schema
dbp config model wifor.emp__regional_trends schema sql/create_output.sql

# Configure column metadata
dbp config model wifor.emp__regional_trends columns set nuts2024 \
    --id NUTS2024 --kind hierarchical

# Configure inputs
dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers
dbp config model wifor.emp__regional_trends input wifor.cl_nuts2024

# Run the full lifecycle (sync, execute, publish)
dbp model run --version 2026-03-09 --timing
```

## What happened

1. **Schema declared** — the output table was created in DuckDB and persisted to `dbport.lock`
2. **Inputs loaded** — Iceberg tables were scanned and streamed into DuckDB via Arrow
3. **Transforms ran** — SQL files executed in DuckDB
4. **Output published** — data written to Iceberg, metadata and codelists attached automatically
5. **State recorded** — version history, input snapshots, and schema all tracked in `dbport.lock`

## Next steps

- Learn about [inputs and loading](../concepts/inputs.md) in depth
- Explore the full [Python API reference](../api/python.md)
- See the [CLI reference](../api/cli.md) for all commands
