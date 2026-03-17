---
hide:
  - navigation
  - toc
---

# DBPort

**Versioned dataset recomputation on DuckDB, published to Iceberg.**

Analytic workloads often recompute the same large dataset periodically to produce a new version. DBPort handles the lifecycle around that — loading inputs, enforcing schema contracts, tracking versions, and publishing safely — so you can focus on the model.

---

<div class="grid cards" markdown>

-   :material-console:{ .lg .middle } **CLI-driven workflow**

    ---

    `dbp model run` loads inputs, executes your model, and publishes the result in one command. Configure once, recompute on demand.

    [:octicons-arrow-right-24: Getting started](getting-started/index.md)

-   :material-database:{ .lg .middle } **DuckDB-native execution**

    ---

    All data operations run through DuckDB. Tables stream in via Arrow, transforms run in SQL or Python, outputs publish to Iceberg.

    [:octicons-arrow-right-24: Concepts](concepts/index.md)

-   :material-shield-check:{ .lg .middle } **Safe, versioned publish**

    ---

    Schema drift protection, version checkpoints, and resumable writes. Re-running a completed version is always a no-op.

    [:octicons-arrow-right-24: CLI Reference](api/cli.md)

-   :material-file-document:{ .lg .middle } **Automatic metadata**

    ---

    Codelists, version history, and lifecycle timestamps are managed hands-free. No manual metadata files to maintain.

    [:octicons-arrow-right-24: Examples](examples/index.md)

</div>

---

## Quick example

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

For programmatic control, the same workflow in Python:

```python
from dbport import DBPort

with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
    port.schema("sql/create_output.sql")
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
    port.execute("sql/transform.sql")
    port.publish(version="2026-03-09", params={"wstatus": "EMP"})
```

---

## Key features

- **CLI-first** — `dbp` covers init, config, load, exec, publish, and run. One command for the full lifecycle.
- **DuckDB-native** — ingest and publish go through the DuckDB Iceberg extension. No batch loops, no memory copies.
- **Snapshot-cached ingest** — `dbp model load` skips unchanged tables automatically.
- **Schema contracts** — publish checks local vs warehouse schema before writing anything.
- **Idempotent publish** — interrupted runs resume from checkpoint. Completed versions are skipped.
- **Automatic metadata** — timestamps, input provenance, codelists, and version history — all managed without manual files.
- **Committable lock file** — `dbport.lock` is TOML, credential-free, and safe to commit.
