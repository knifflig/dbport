# Versioning & Publish

Publishing is the final step in the DBPort workflow. It writes data from DuckDB to the Iceberg warehouse with full metadata, codelist, and version tracking.

## Basic publish

=== "CLI"

    ```bash
    dbp model publish --version 2026-03-09
    ```

=== "Python"

    ```python
    port.publish(
        version="2026-03-09",
        params={"wstatus": "EMP", "nace_r2": "TOTAL"},
    )
    ```

## Publish modes

| Mode | Behavior |
|---|---|
| Default | Idempotent. Skips silently if the version is already completed. Resumes from checkpoint if interrupted. |
| Dry run | Schema validation only. No data is written. Useful for CI checks. |
| Refresh | Overwrites an existing version unconditionally. |

=== "CLI"

    ```bash
    # Validate schemas without writing
    dbp model publish --version 2026-03-09 --dry-run

    # Overwrite a previously published version
    dbp model publish --version 2026-03-09 --refresh
    ```

=== "Python"

    ```python
    # Validate schemas without writing
    port.publish(version="2026-03-09", mode="dry")

    # Overwrite a previously published version
    port.publish(version="2026-03-09", mode="refresh")
    ```

## Full lifecycle in one command

The CLI provides `dbp model run` to execute the hook and publish in a single step:

```bash
dbp model run --version 2026-03-09 --timing
```

This syncs state, executes the configured hook, and publishes — equivalent to calling `port.run(version="2026-03-09")` in Python. See [Hooks & Execution](hooks.md) for details.

## Pre-publish checks

Before writing any data, publish runs these checks in order:

1. **Schema defined** — fails if no schema has been declared
2. **Version idempotency** — if the version already completed, returns immediately (skipped in refresh mode)
3. **Schema drift** — compares local schema to warehouse schema; fails with a diff if incompatible. Catalog connection failures also block the publish.

## Idempotency and checkpoints

Every publish writes checkpoint properties to the Iceberg table:

- `dbport.upload.v2.<version>.completed` — marks the version as done
- `dbport.upload.v2.<version>.rows_appended` — tracks row count

If a publish is interrupted, the next run detects the incomplete checkpoint and resumes from where it left off. Re-running a completed version is a no-op.

## What happens on success

1. Data is written to the Iceberg table at `<agency>.<dataset_id>`
2. Codelists are auto-generated (or fetched from attached tables)
3. `metadata.json` is materialized and embedded in Iceberg table properties
4. A `VersionRecord` is appended to `dbport.lock`
5. `created_at` is set on first publish only; `last_updated_at` is updated every time

## Write strategy

The DuckDB `iceberg` extension is the primary write path. When the catalog does not support multi-table commits (e.g., returns 404 on the transactions endpoint), the adapter auto-switches to **streaming Arrow fallback**:

- Streams 50K-row Arrow batches from DuckDB
- Each batch committed in a single pyiceberg transaction
- Checkpoint properties updated per batch
- On commit conflict: reload metadata, resume from remote checkpoint (max 5 retries)
- Peak memory: ~50K rows per batch (bounded)

This session-level fallback is transparent — once DuckDB writes fail, all subsequent writes use Arrow without retrying.
