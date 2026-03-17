# Lock File

`dbport.lock` is the committable state file that tracks schemas, ingest state, and version history for all models in a repository. It is always written by DBPort — never by hand — but it appears in diffs, merge conflicts, and debugging sessions, so understanding its structure matters.

## Location

The lock file lives at the **repository root**, next to `pyproject.toml`. When `lock_path` is not provided, `DBPort` walks up from the calling script's directory until it finds a `pyproject.toml`.

## Properties

- **TOML format** — human-readable and diff-friendly
- **No secrets** — credentials are never written to disk
- **Multi-model** — each model gets a namespaced section under `[models."agency.dataset_id"]`
- **Safe to commit** — belongs in version control

## When does the lock file change?

| Operation | What changes |
|---|---|
| `DBPort()` init | Model header created if new model |
| `port.schema(ddl)` | `[schema]` section written or replaced |
| `port.columns.*.meta(...)` | Column entry in `[[schema.columns]]` updated |
| `port.columns.*.attach(...)` | `attach_table` field set on column entry |
| `port.load(table)` | `[[inputs]]` entry added or updated with `last_snapshot_id` |
| `port.publish(version)` | `[[versions]]` entry appended |

Changes are written to disk immediately after each operation. There is no deferred flush.

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

## Recovery and merge conflicts

### Merge conflicts

The lock file is structured to minimize conflicts:

- **`[[versions]]`** — append-only. Accept both sides; each publish appends a new entry.
- **`[schema]`** — take the version that matches the current DDL in your SQL file. If both sides changed the DDL, resolve the DDL first, then re-run `port.schema()`.
- **`[[inputs]]`** — take the entry with the higher `last_snapshot_id`. The next `port.load()` will re-check against the warehouse regardless.

### Stale or missing lock file

If the lock file is deleted or stale:

1. `DBPort()` re-creates a fresh model header on next initialization
2. Re-run `port.schema()` to restore the schema section
3. Re-run `port.load()` for each input — snapshots will be fetched from the warehouse
4. Version history is lost locally but the warehouse table properties retain the authoritative publish record

### Fields that matter for correctness

Most fields are informational, but three have behavioral consequences:

- **`last_snapshot_id`** — drives the ingest cache; a wrong value causes stale data or unnecessary reloads
- **`completed`** on versions — drives publish idempotency; flipping this causes skipped or duplicate publishes
- **`ddl`** — must match what DuckDB actually has; use `port.schema()` to change it

### Regeneration

Running the full pipeline (`schema()` → `load()` → `publish()`) rebuilds all lock state from scratch. The warehouse is the source of truth.

## Multi-model support

Multiple models in the same repository each get their own `[models."agency.dataset_id"]` section. The `default_model` key at the top level determines which model is used when no explicit model is specified in the CLI.
