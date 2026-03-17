---
hide:
  - navigation
  - toc
---

# DBPort

**The production layer for DuckDB data products.**

Run warehouse-connected models on DuckDB with governed inputs, explicit output contracts, and safe publication workflows.

---

<div class="grid cards" markdown>

-   :material-lightning-bolt:{ .lg .middle } **One import, full lifecycle**

    ---

    `from dbport import DBPort` — load inputs, transform data, publish outputs. One class handles the entire dataset lifecycle.

    [:octicons-arrow-right-24: Getting started](getting-started/index.md)

-   :material-database:{ .lg .middle } **DuckDB-native execution**

    ---

    All data operations run through DuckDB. Tables up to 1 billion rows stream directly in SQL — no batch loops, no memory blowups.

    [:octicons-arrow-right-24: Concepts](concepts/index.md)

-   :material-shield-check:{ .lg .middle } **Safe, idempotent publish**

    ---

    Schema drift protection, version checkpoints, and resumable writes. Re-running the same version is always safe.

    [:octicons-arrow-right-24: API Reference](api/index.md)

-   :material-file-document:{ .lg .middle } **Automatic metadata**

    ---

    Codelists, version history, and lifecycle timestamps are managed hands-free. No `metadata.json` to write manually.

    [:octicons-arrow-right-24: Examples](examples/index.md)

</div>

---

## Quick example

```python
from dbport import DBPort

with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
    port.schema("sql/create_output.sql")
    port.columns.nuts2024.meta(codelist_id="NUTS2024", codelist_kind="hierarchical")
    port.columns.nuts2024.attach(table="wifor.cl_nuts2024")
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
    port.load("wifor.cl_nuts2024")
    port.execute("sql/staging.sql")
    port.execute("sql/final_output.sql")
    port.publish(version="2026-03-09", params={"wstatus": "EMP"})
```

---

## Key features

- **One import** — `from dbport import DBPort`. No other public symbols.
- **DuckDB-first** — ingest and publish go through the DuckDB `iceberg` extension.
- **Snapshot-cached ingest** — `port.load()` skips unchanged tables automatically.
- **Idempotent publish** — interrupted runs resume from checkpoint.
- **Schema drift protection** — `publish()` compares local vs warehouse schema before writing.
- **Automatic metadata** — `created_at`, `last_updated_at`, inputs, codelists, versions — all managed hands-free.
- **Committable lock file** — `dbport.lock` is TOML, credential-free, and safe to commit.
- **CLI-first** — `dbp` command covers init, status, load, exec, publish, and run.
