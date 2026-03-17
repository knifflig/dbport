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

```toml
default_model = "wifor.emp__regional_trends"

[models."wifor.emp__regional_trends"]
agency      = "wifor"
dataset_id  = "emp__regional_trends"
model_root  = "examples/regional_trends"
duckdb_path = "examples/regional_trends/data/emp__regional_trends.duckdb"

[models."wifor.emp__regional_trends".schema]
ddl = "CREATE OR REPLACE TABLE wifor.emp__regional_trends (...)"

[[models."wifor.emp__regional_trends".schema.columns]]
column_name     = "nuts2024"
column_pos      = 2
codelist_id     = "NUTS2024"
codelist_kind   = "hierarchical"
codelist_labels = {en = "NUTS 2024 Regions"}
attach_table    = "wifor.cl_nuts2024"

[[models."wifor.emp__regional_trends".inputs]]
table_address    = "estat.nama_10r_3empers"
filters          = {wstatus = "EMP", nace_r2 = "TOTAL"}
last_snapshot_id = 123456789

[[models."wifor.emp__regional_trends".versions]]
version      = "2026-03-09"
published_at = "2026-03-09T14:33:12Z"
params       = {wstatus = "EMP", nace_r2 = "TOTAL"}
rows         = 1234567
completed    = true
```

## Three roles

### 1. Schema registry

Stores the DDL and per-column codelist configuration for each model. Updated by `port.schema()` and `port.columns.<name>.meta()`.

### 2. Ingest cache

Tracks Iceberg snapshot IDs for each loaded input. When a snapshot has not changed, `port.load()` skips the table automatically.

### 3. Version history

Append-only list of completed publishes per model. Each entry records the version string, timestamp, parameters, row count, and completion status.

## Multi-model support

Multiple models in the same repository each get their own `[models."agency.dataset_id"]` section. The `default_model` key at the top level determines which model is used when no explicit model is specified in the CLI.
