# Getting Started

You recompute datasets on a regular cycle. You download inputs, run model logic, and upload the result. It works — until it doesn't: a schema drifts silently, an interrupted publish corrupts the warehouse, a rerun overwrites data it shouldn't have, or nobody can tell which input version produced last month's output.

**DBPort makes that workflow reliable.** It gives you a DuckDB-native runtime that loads warehouse inputs, enforces schema contracts, tracks every version, and publishes safely — with resumable checkpoints and full provenance. You write the model logic. DBPort handles everything around it.

## See it in action

=== "CLI"

    ```bash
    pip install dbport

    # Initialize a project
    dbp init regional_trends --agency wifor --dataset emp__regional_trends
    cd regional_trends

    # Configure and run the full lifecycle
    dbp config model wifor.emp__regional_trends schema sql/create_output.sql
    dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers
    dbp model run --version 2026-03-09 --timing
    ```

=== "Python"

    ```python
    from dbport import DBPort

    with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
        port.schema("sql/create_output.sql")
        port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
        port.execute("sql/transform.sql")
        port.publish(version="2026-03-09", params={"wstatus": "EMP"})
    ```

That is a complete lifecycle: inputs loaded from an Iceberg warehouse into DuckDB, SQL transforms executed, and versioned output published back — with schema validation, metadata, and codelists attached automatically.

## What you get

- :material-download:{ .middle } **Snapshot-cached inputs** — unchanged tables are skipped automatically on reload
- :material-shield-check:{ .middle } **Schema contracts** — drift is caught before anything is written to the warehouse
- :material-tag:{ .middle } **Version tracking** — every publish records its version, parameters, timestamps, and row count
- :material-lock:{ .middle } **Committable state** — `dbport.lock` tracks everything in TOML, safe for code review and CI
- :material-restart:{ .middle } **Resumable publishes** — interrupted runs pick up from checkpoint, never corrupt

## What you'll need

- Python 3.11 or 3.12
- An Iceberg REST catalog with S3-compatible object storage
- Catalog credentials (URI, token, warehouse name)

## Next steps

<div class="grid cards" markdown>

-   **About**

    ---

    Why DBPort exists, who it's for, and how it fits with DuckDB, dbt, and orchestrators.

    [:octicons-arrow-right-24: Read more](about.md)

-   **Installation**

    ---

    Install DBPort and verify your environment.

    [:octicons-arrow-right-24: Install](installation.md)

-   **Credentials**

    ---

    Configure warehouse access credentials.

    [:octicons-arrow-right-24: Configure](credentials.md)

-   **Quickstart**

    ---

    Walk through a complete project from init to publish.

    [:octicons-arrow-right-24: Start building](quickstart.md)

</div>
