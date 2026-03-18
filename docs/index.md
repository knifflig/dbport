---
hide:
  - toc
---

# DBPort

**Versioned dataset recomputation on DuckDB, published to Iceberg.**

Load inputs, enforce schema contracts, track versions, and publish safely — so you can focus on the model.

---

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Getting Started**

    ---

    Install DBPort, configure credentials, and run your first model in five minutes.

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

-   :material-file-code:{ .lg .middle } **Examples**

    ---

    End-to-end CLI and Python workflows showing load, transform, and publish.

    [:octicons-arrow-right-24: View examples](examples/index.md)

-   :material-history:{ .lg .middle } **Changelog**

    ---

    What changed in each release, from `0.0.1` through the current version.

    [:octicons-arrow-right-24: View changelog](changelog.md)

</div>

---

## Quick example

=== "CLI"

    ```bash
    # Initialize a project
    dbp init regional_trends --agency wifor --dataset emp__regional_trends
    cd regional_trends

    # Configure the model
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
