---
hide:
  - toc
---

# DBPort

**The production layer for DuckDB data products.**

You recompute datasets on a regular cycle — download inputs from a warehouse, run your model, publish the result. That workflow is straightforward until something breaks: a schema drifts silently, an interrupted publish corrupts the warehouse, a rerun overwrites data it shouldn't, or nobody can trace which inputs produced last month's output.

DBPort handles the lifecycle around your model so those problems go away. Load governed inputs into DuckDB, enforce output contracts, track every version, and publish safely — with resumable checkpoints and full provenance. You write the model logic however you want. DBPort manages everything around it.

[:octicons-arrow-right-24: Why DBPort — and who it's for](getting-started/about.md){ .md-button } [:octicons-arrow-right-24: Get started](getting-started/index.md){ .md-button .md-button--primary }

---

## See it in action

=== "CLI"

    ```bash
    pip install dbport

    # Initialize a project and configure the model
    dbp init regional_trends --agency wifor --dataset emp__regional_trends
    cd regional_trends
    dbp config model wifor.emp__regional_trends schema sql/create_output.sql
    dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers

    # Run the full lifecycle: load → execute → publish
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

That is a complete lifecycle: inputs loaded from an Iceberg warehouse, SQL transforms executed in DuckDB, and a versioned output published back — with schema validation, metadata, and codelists attached automatically.

---

## Why teams use DBPort

<div class="grid cards" markdown>

-   :material-download:{ .lg .middle } **Snapshot-cached inputs**

    ---

    Load Iceberg tables into DuckDB. Unchanged snapshots are skipped automatically — no wasted reads, no stale data.

-   :material-shield-check:{ .lg .middle } **Schema contracts**

    ---

    Declare the output shape upfront. Schema drift is caught before anything is written to the warehouse, not after.

-   :material-tag:{ .lg .middle } **Version tracking**

    ---

    Every publish records its version, parameters, timestamps, and row count. Re-running a completed version is a safe no-op.

-   :material-restart:{ .lg .middle } **Resumable publishes**

    ---

    Interrupted runs pick up from checkpoint. Nothing is corrupted, nothing is lost.

-   :material-lock:{ .lg .middle } **Committable state**

    ---

    `dbport.lock` is TOML, credential-free, and tracks schema, inputs, and versions — ready for code review and CI.

-   :material-text-box-check:{ .lg .middle } **Automatic metadata**

    ---

    Timestamps, input provenance, codelists, and version history are attached to published tables without any manual work.

</div>

---

## It fits with what you already use

DBPort is not a warehouse, not an orchestrator, and not a transformation framework. It is the production lifecycle layer that connects them.

| | |
|---|---|
| **DuckDB** | The execution engine. DBPort adds governed inputs, output contracts, and publish semantics around it. |
| **dbt** | Complementary. dbt handles transformations in the middle; DBPort manages dataset lifecycle at the edges. |
| **Airflow, Dagster, …** | DBPort defines what a safe run means. Orchestrators decide when to trigger it. |

---

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Getting Started**

    ---

    Install DBPort, configure credentials, and run your first model.

    [:octicons-arrow-right-24: Start here](getting-started/index.md)

-   :material-book-open-variant:{ .lg .middle } **Concepts**

    ---

    How inputs, schemas, metadata, versioning, and the lock file work together.

    [:octicons-arrow-right-24: Read the concepts](concepts/index.md)

-   :material-console:{ .lg .middle } **CLI Reference**

    ---

    Full command reference for `dbp init`, `dbp model`, `dbp config`, and `dbp status`.

    [:octicons-arrow-right-24: See all commands](api/cli.md)

-   :material-language-python:{ .lg .middle } **Python API**

    ---

    Constructor options, methods, and lifecycle for the `DBPort` class.

    [:octicons-arrow-right-24: See the API](api/python.md)

</div>
