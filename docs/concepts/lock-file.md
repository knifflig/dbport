# Lock File

`dbport.lock` is the committable state file that tracks schemas, ingest state, and version history for all models in a repository.

## Location

The lock file lives at the **repository root**, next to `pyproject.toml`. When `lock_path` is not provided, `DBPort` walks up from the calling script's directory until it finds a `pyproject.toml`.

## Properties

- **TOML format** — human-readable and diff-friendly
- **No secrets** — credentials are never written to disk
- **Multi-model** — each model gets a namespaced section
- **Safe to commit** — belongs in version control

## Structure

The lock file has a top-level `default_model` key and per-model sections.

### Top-level

```toml
default_model = "wifor.emp__regional_trends" # (1)!
```

1. The model used when no explicit model is specified in the CLI. Set via `dbp config default model`.

### Model header

```toml
[models."wifor.emp__regional_trends"]
agency      = "wifor"           # (1)!
dataset_id  = "emp__regional_trends"
model_root  = "examples/regional_trends"  # (2)!
duckdb_path = "examples/regional_trends/data/emp__regional_trends.duckdb"
```

1. Agency and dataset ID match the `DBPort(agency=..., dataset_id=...)` constructor.
2. The directory containing the model's hook file, SQL scripts, and DuckDB file.

### Schema

```toml
[models."wifor.emp__regional_trends".schema]
ddl = "CREATE OR REPLACE TABLE wifor.emp__regional_trends (...)" # (1)!

[[models."wifor.emp__regional_trends".schema.columns]]
column_name     = "nuts2024"
column_pos      = 2
codelist_id     = "NUTS2024"          # (2)!
codelist_kind   = "hierarchical"
codelist_labels = {en = "NUTS 2024 Regions"}
attach_table    = "wifor.cl_nuts2024" # (3)!
```

1. The full DDL statement, written by `port.schema()`.
2. Codelist metadata, set via `port.columns.nuts2024.meta(...)`.
3. The DuckDB table used as the codelist source, set via `port.columns.nuts2024.attach(...)`.

### Inputs

```toml
[[models."wifor.emp__regional_trends".inputs]]
table_address    = "estat.nama_10r_3empers"
filters          = {wstatus = "EMP", nace_r2 = "TOTAL"}
last_snapshot_id = 123456789  # (1)!
```

1. Iceberg snapshot ID from the last successful `port.load()`. When this matches the current warehouse snapshot, loading is skipped automatically.

### Versions

```toml
[[models."wifor.emp__regional_trends".versions]]
version      = "2026-03-09"
published_at = "2026-03-09T14:33:12Z"
params       = {wstatus = "EMP", nace_r2 = "TOTAL"}
rows         = 1234567
completed    = true  # (1)!
```

1. Marks the version as successfully published. `port.publish()` skips completed versions by default (idempotency).

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

The lock file belongs in version control for four reasons:

- **Reproducibility** — snapshot IDs pin exact input data, so a checkout reproduces the same pipeline state
- **Auditability** — the version history records every publish with timestamp, parameters, and row count
- **Collaboration** — teammates see the same schema declarations, input filters, and codelist configuration
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

### When manual edits are safe

**Safe to edit manually:**

- `default_model` — switch the active model for the CLI
- Column metadata fields (`codelist_id`, `codelist_kind`, `codelist_labels`) — these are informational

**Unsafe to edit manually:**

- `last_snapshot_id` — incorrect values break the ingest cache (stale data or unnecessary reloads)
- `completed` flag on versions — changing this breaks publish idempotency
- `ddl` statement — must match what DuckDB actually has; use `port.schema()` instead

### Regeneration

Running the full pipeline again (`schema()` → `load()` → `publish()`) rebuilds all lock state from scratch. The warehouse is the source of truth for table existence and schema compatibility.

## Multi-model support

Multiple models in the same repository each get their own `[models."agency.dataset_id"]` section. The `default_model` key at the top level determines which model is used when no explicit model is specified in the CLI.
