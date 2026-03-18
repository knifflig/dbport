---
hide:
  - toc
---

# DBPort

**Build locally. Publish safely.**

Governance and orchestration for recomputable warehouse datasets.

You build models that produce datasets — and those datasets depend on each other. When external sources update, you need to recompute downstream models in the right order, knowing exactly which input versions went into each output. As the number of models grows, keeping track of dependencies, provenance, and data quality becomes harder than the modeling itself.

DBPort is the orchestration layer on top of your warehouse that enforces governance into recomputable workflows. It tracks dependencies between your models and on external inputs, so you can build with the confidence that future updates will be picked up correctly — and that other models can pick up your results.

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

## Why using DBPort

<div class="grid cards" markdown>

-   :material-graph-outline:{ .lg .middle } **Model dependencies, tracked**

    ---

    Models produce datasets that feed other models. DBPort tracks these dependencies so you always know what depends on what — across your entire organisation.

-   :material-history:{ .lg .middle } **Full input provenance**

    ---

    Every publish records exactly which input versions and snapshots were used. Months later, you can trace any output back to the data that produced it.

-   :material-refresh-auto:{ .lg .middle } **Recompute when sources update**

    ---

    Snapshot-cached inputs detect when external sources change. Unchanged tables are skipped automatically — only what's new gets reprocessed.

-   :material-shield-check:{ .lg .middle } **Schema drift, caught early**

    ---

    Declare the output shape upfront. Drift is caught before anything is written to the warehouse — no fraudulent data, no silent corruption.

-   :material-tag:{ .lg .middle } **Versioned, resumable publishes**

    ---

    Every publish records version, parameters, and row count. Interrupted runs resume from checkpoint. Re-running a completed version is a safe no-op.

-   :material-lock:{ .lg .middle } **Committable state**

    ---

    `dbport.lock` is TOML, credential-free, and tracks schema, inputs, and versions — ready for code review and CI.

</div>

---

## It fits with what you already use

DBPort doesn't deliver the models — it delivers the platform to keep track of dependencies between them. It is the governance layer that connects your tools.

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
