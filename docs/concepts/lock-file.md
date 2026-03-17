# Lock File

`dbport.lock` is the committable state file that tracks schemas, ingest state, and version history for all models in a repository. It is always written by DBPort — never by hand — but it appears in diffs, merge conflicts, and debugging sessions, so understanding its structure matters.

## Location

The lock file lives at the **repository root**, next to `pyproject.toml`. When not provided explicitly, DBPort walks up from the working directory until it finds a `pyproject.toml`.

## Properties

- **TOML format** — human-readable and diff-friendly
- **No secrets** — credentials are never written to disk
- **Multi-model** — each model gets a namespaced section under `[models."agency.dataset_id"]`
- **Safe to commit** — belongs in version control

## When does the lock file change?

Every mutating operation writes to disk immediately — there is no deferred flush.

=== "CLI"

    | Command | What changes |
    |---|---|
    | `dbp init` | Model header created, set as default |
    | `dbp model sync` | Model header created if new; `last_fetched_at` updated |
    | `dbp config model … schema` | `[schema]` section written or replaced |
    | `dbp config model … input` | `[[inputs]]` entry added or updated |
    | `dbp config model … input --load` | `[[inputs]]` entry added + `last_snapshot_id` resolved |
    | `dbp model load` | `[[inputs]]` entries updated with `last_snapshot_id` |
    | `dbp model publish` | `[[versions]]` entry appended |
    | `dbp model run` | All of the above (sync → load → exec → publish) |

=== "Python"

    | Operation | What changes |
    |---|---|
    | `DBPort()` init | Model header created if new model |
    | `port.schema(ddl)` | `[schema]` section written or replaced |
    | `port.columns.*.meta(...)` | Column entry in `[[schema.columns]]` updated |
    | `port.columns.*.attach(...)` | `attach_table` field set on column entry |
    | `port.load(table)` | `[[inputs]]` entry added or updated with `last_snapshot_id` |
    | `port.publish(version)` | `[[versions]]` entry appended |

## Why commit the lock file

- **Reproducibility** — snapshot IDs pin exact input data, so a checkout reproduces the same pipeline state
- **Code review** — reviewers can see whether a schema changed, which inputs updated, and what was published
- **CI integration** — automated pipelines can validate schemas and detect drift without running the full pipeline

## Recognizing diffs in pull requests

Normal development produces predictable diffs. Here are the three most common patterns.

### Schema change — adding a column

```diff
 [models."wifor.emp__regional_trends".schema]
-ddl = "CREATE OR REPLACE TABLE wifor.emp__regional_trends (nuts2024 VARCHAR, year SMALLINT, value DOUBLE)"
+ddl = "CREATE OR REPLACE TABLE wifor.emp__regional_trends (nuts2024 VARCHAR, year SMALLINT, value DOUBLE, source VARCHAR)"

+[[models."wifor.emp__regional_trends".schema.columns]]
+column_name = "source"
+column_pos  = 3
```

### Snapshot update — new data loaded

```diff
 [[models."wifor.emp__regional_trends".inputs]]
 table_address    = "estat.nama_10r_3empers"
 filters          = {wstatus = "EMP", nace_r2 = "TOTAL"}
-last_snapshot_id = 123456789
+last_snapshot_id = 234567890
```

### New version published

```diff
+[[models."wifor.emp__regional_trends".versions]]
+version      = "2026-03-15"
+published_at = "2026-03-15T10:22:01Z"
+params       = {wstatus = "EMP", nace_r2 = "TOTAL"}
+rows         = 1345678
+completed    = true
```

## Merge conflicts

The lock file is structured to minimize conflicts:

- **`[[versions]]`** — append-only. Accept both sides; each publish appends a new entry.
- **`[schema]`** — take the version that matches the current DDL in your SQL file. If both sides changed the DDL, resolve the DDL first, then re-run the schema command.
- **`[[inputs]]`** — take the entry with the higher `last_snapshot_id`. The next load will re-check against the warehouse regardless.

## Stale or missing lock file

If the lock file is deleted or becomes stale, DBPort recovers gracefully on the next run.

=== "CLI"

    ```bash
    # 1. Sync re-creates the model header and detects the schema
    dbp model sync

    # 2. Load resolves fresh snapshots for all configured inputs
    dbp model load

    # 3. Or do everything in one step
    dbp model run --version 2026-03-15
    ```

=== "Python"

    ```python
    # 1. DBPort() re-creates a fresh model header on init
    with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
        # 2. Re-run schema to restore the schema section
        port.schema("sql/create_output.sql")

        # 3. Re-run load for each input — snapshots fetched from warehouse
        port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})

        # 4. Publish as normal
        port.publish(version="2026-03-15", params={"wstatus": "EMP"})
    ```

Version history is lost locally, but the warehouse table properties retain the authoritative publish record.

## Regeneration

Running the full pipeline rebuilds all lock state from scratch. The warehouse is the source of truth.

=== "CLI"

    ```bash
    dbp model run --version 2026-03-15
    ```

=== "Python"

    ```python
    # schema() → load() → execute() → publish() regenerates everything
    with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
        port.schema("sql/create_output.sql")
        port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
        port.execute("sql/transform.sql")
        port.publish(version="2026-03-15", params={"wstatus": "EMP"})
    ```

## Fields that matter for correctness

Most fields are informational, but three have behavioral consequences:

- **`last_snapshot_id`** — drives the ingest cache; a wrong value causes stale data or unnecessary reloads
- **`completed`** on versions — drives publish idempotency; flipping this causes skipped or duplicate publishes
- **`ddl`** — must match what DuckDB actually has; use the schema command to change it

## Multi-model support

Multiple models in the same repository each get their own `[models."agency.dataset_id"]` section. The `default_model` key at the top level determines which model is used when no explicit model is specified in the CLI.
