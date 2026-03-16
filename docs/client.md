# DBPort — User Guide

`DBPort` is the single entry point for all warehouse operations: loading input tables, running SQL transforms in DuckDB, and publishing outputs to the Iceberg REST catalog with full metadata and codelist management.

```python
from dbport import DBPort
```

This is the **only import you need**.

---

## Installation

```bash
pip install dbport
```

---

## Credentials

Credentials are resolved in this order:

1. Explicit keyword arguments passed to `DBPort(...)`
2. Environment variables

| Environment variable | Constructor argument | Description |
|---|---|---|
| `ICEBERG_REST_URI` | `catalog_uri` | Iceberg REST catalog URL |
| `ICEBERG_CATALOG_TOKEN` | `catalog_token` | Bearer token for catalog authentication |
| `ICEBERG_WAREHOUSE` | `warehouse` | Warehouse name in the catalog |
| `S3_ENDPOINT` | `s3_endpoint` | S3-compatible object store endpoint (optional) |
| `AWS_ACCESS_KEY_ID` | `s3_access_key` | S3 access key ID (optional) |
| `AWS_SECRET_ACCESS_KEY` | `s3_secret_key` | S3 secret access key (optional) |

The three required credentials (`catalog_uri`, `catalog_token`, `warehouse`) must be provided via one of the two methods above. Missing credentials raise a clear error at startup.

**Credentials are never written to disk.** The `dbport.lock` file is credential-free and safe to commit.

---

## Initialization

```python
from dbport import DBPort

port = DBPort(
    agency="wifor",
    dataset_id="emp__regional_trends",
    # Optional — fall back to env vars:
    catalog_uri=None,
    catalog_token=None,
    warehouse=None,
    s3_endpoint=None,
    s3_access_key=None,
    s3_secret_key=None,
    # Storage paths — auto-discovered when not provided:
    duckdb_path=None,   # default: <script_dir>/data/<dataset_id>.duckdb
    lock_path=None,     # default: repo root (next to pyproject.toml)
)
```

On initialization, `DBPort`:

- Resolves and validates credentials
- Creates the `data/` directory if it does not exist
- Opens (or creates) the file-backed DuckDB database
- Connects to the Iceberg REST catalog
- Reads (or creates) the `dbport.lock` file
- Updates `last_fetched_at` in the warehouse table properties

### Context manager (recommended)

```python
with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
    ...
# port.close() is called automatically
```

---

## API Reference

### `port.schema(ddl)`

Declares the output table schema. Accepts an inline DDL string or a path to a `.sql` file. The table is created in DuckDB and the schema (DDL + column list) is persisted to `dbport.lock`. A default codelist entry is initialised for every column.

```python
# Path to a .sql file (resolved relative to the caller's directory)
port.schema("sql/create_output.sql")

# Or inline DDL
port.schema("""
    CREATE OR REPLACE TABLE wifor.emp__regional_trends (
        freq     VARCHAR,
        year     DATE,
        nuts2024 VARCHAR,
        value    DOUBLE
    )
""")
```

Call `port.schema()` once, early in the script. Re-running the same DDL is idempotent.

**Warehouse validation**: If the output table already exists in the warehouse, `port.schema()` compares the local DDL schema against the warehouse schema immediately. If they are incompatible, `SchemaDriftError` is raised before any loads or transforms run. If the table does not yet exist (first publish), the check is skipped.

---

### `port.load(table_address, *, filters=None)`

Loads an Iceberg table from the warehouse into DuckDB. The table is available in DuckDB under its **exact original address** — no aliasing.

```python
# Load with equality filters
port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP", "nace_r2": "TOTAL"})
# Available in DuckDB as: estat.nama_10r_3empers

# Load without filters (full table)
port.load("wifor.cl_nuts2024")
# Available in DuckDB as: wifor.cl_nuts2024
```

**Snapshot-based caching**: if the table's Iceberg snapshot has not changed since the last run and the DuckDB relation already exists, the load is skipped automatically. This makes repeated runs fast.

**No row cap**: `load()` always fetches the full table. Use `filters` to scope data deliberately.

The input declaration (table address, filters, snapshot ID) is recorded in `dbport.lock` and included in the published metadata.

---

### `port.columns.<name>`

Attribute-style access to per-column metadata configuration. Changes are persisted to `dbport.lock` immediately.

#### `.meta(...)` — override codelist metadata

```python
port.columns.nuts2024.meta(
    codelist_id="NUTS2024",
    codelist_kind="hierarchical",       # "flat" | "hierarchical"
    codelist_labels={"en": "NUTS 2024 Regions"},
)
```

| Parameter | Default | Description |
|---|---|---|
| `codelist_id` | column name | Identifier for the codelist |
| `codelist_kind` | inferred from SQL type | `"flat"` or `"hierarchical"` |
| `codelist_type` | inferred from SQL type | Value type hint (e.g. `"string"`, `"integer"`) |
| `codelist_labels` | `null` | Human-readable labels per language, e.g. `{"en": "..."}` |

`.meta()` returns `self` for chaining:

```python
port.columns.nuts2024.meta(codelist_id="NUTS2024", codelist_kind="hierarchical").attach(table="wifor.cl_nuts2024")
```

#### `.attach(table=...)` — use a DuckDB table as the codelist source

```python
port.columns.nuts2024.attach(table="wifor.cl_nuts2024")
```

The table should already be loaded into DuckDB (via `port.load()`). On `publish()`, the full table is exported as the codelist instead of auto-generating from distinct output values.

---

### `port.execute(sql)`

Runs a SQL statement or a `.sql` file in DuckDB.

```python
# Path to a .sql file (resolved relative to the caller's directory)
port.execute("sql/staging_encoded.sql")
port.execute("sql/final_output.sql")

# Inline SQL
port.execute("CREATE TABLE staging.ranked AS SELECT ...")
```

For SQL that requires dynamic parameters (table names, levels, etc.), render the template before passing it:

```python
sql = read_sql("bench_build_level.sql").format(
    bench_table="staging.bench_lvl2",
    parent_bench_table="staging.bench_lvl1",
    child_level=2,
)
port.execute(sql)
```

---

### `port.publish(*, version, params=None, mode=None)`

Writes `<agency>.<dataset_id>` from DuckDB to the Iceberg warehouse. Performs fail-fast checks before writing anything.

```python
port.publish(
    version="2026-03-09",
    params={"wstatus": "EMP", "nace_r2": "TOTAL"},
)
```

#### `mode` parameter

| Value | Behaviour |
|---|---|
| `None` (default) | Normal idempotent publish. Skips silently if the version is already completed; resumes from checkpoint if a previous run was interrupted. |
| `"dry"` | Validates schemas only. No data is written to the warehouse. Useful for CI checks or pre-flight validation. |
| `"refresh"` | Overwrites an existing version unconditionally. Ignores the completed checkpoint and re-writes all rows. |

```python
# Validate schemas without writing data
port.publish(version="2026-03-09", mode="dry")

# Overwrite a previously published version
port.publish(version="2026-03-09", mode="refresh")
```

#### Pre-publish checks (in order)

1. **Schema defined** — raises `RuntimeError` if `port.schema()` has not been called.
2. **Version idempotency** — if `version` already completed successfully, returns immediately without writing. Skipped when `mode="refresh"`.
3. **Schema drift** — if the Iceberg table already exists, compares the local schema to the warehouse schema. This check runs at both `port.schema()` time (early fail-fast) and `port.publish()` time (safety net for changes between schema declaration and publish). Raises `SchemaDriftError` with a diff if they are incompatible:
   ```
   SchemaDriftError: Schema drift detected:
     + new_column (string)     # added locally, not in warehouse
     - old_column (int32)      # in warehouse, removed locally
     ~ value (int32 → float64) # type changed
   ```
   This check runs for all modes including `"dry"`.

#### On success (non-dry)

- Data is written to the Iceberg table at `<agency>.<dataset_id>`
- Codelists are auto-generated for each column (or fetched from `attach()`-specified tables)
- A `metadata.json` is materialized and embedded in the Iceberg table properties
- A `VersionRecord` is appended to `dbport.lock`
- `created_at` is set on first publish only; `last_updated_at` is updated on every publish

---

### `port.close()`

Closes the DuckDB connection. Called automatically when using the context manager.

---

## The `dbport.lock` File

`dbport.lock` lives at the **repository root** (next to `pyproject.toml`) and is shared across all models in the repo. It contains **no credentials** and is safe to commit.

Each model is stored under a namespaced section keyed by `agency.dataset_id`. Multiple models coexist without interfering:

```toml
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

### Lock file roles

- **Schema registry** — DDL + per-column codelist configuration per model
- **Ingest cache** — snapshot IDs to skip re-loading unchanged Iceberg tables
- **Version history** — append-only list of completed publishes per model

### Path discovery

When `lock_path` is not provided, `DBPort` walks up from the calling script's directory until it finds a `pyproject.toml`, then places `dbport.lock` in that directory. The `model_root` is computed as the relative path from the repo root to the calling script's directory.

---

## Metadata Lifecycle

All metadata is managed automatically. You never write `metadata.json` manually.

| Field | Source | When set |
|---|---|---|
| `agency_id` | `DBPort(agency=...)` | Every init |
| `dataset_id` | `DBPort(dataset_id=...)` | Every init |
| `created_at` | Auto | First `publish()` only |
| `last_updated_at` | Auto (publish time) | Every `publish()` |
| `last_fetched_at` | Auto | Every `DBPort()` initialization |
| `params` | `publish(params=...)` | Every `publish()` |
| `inputs` | `port.load(...)` | Accumulated across all `load()` calls |
| `versions` | Auto (append) | Every `publish()` |
| `codelists[].codelist_id` | Default = column name; override via `.meta()` | Schema + overrides |
| `codelists[].codelist_kind` | Inferred or `.meta()`-set | Schema + overrides |
| `codelists[].attach_table` | `.attach(table=...)` | Optional |

On `publish()`, the finalized `metadata.json` is built in-memory and embedded directly in the Iceberg table properties (compressed). No local files are written.

---

## Full Usage Example

```python
from dbport import DBPort

with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:

    # 1. Declare output schema
    port.schema("sql/create_output.sql")

    # 2. Override column metadata
    port.columns.nuts2024.meta(
        codelist_id="NUTS2024",
        codelist_kind="hierarchical",
        codelist_labels={"en": "NUTS 2024 Regions"},
    )
    port.columns.nuts2024.attach(table="wifor.cl_nuts2024")

    # 3. Load inputs (snapshot-cached; skipped if unchanged)
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP", "nace_r2": "TOTAL"})
    port.load("wifor.cl_nuts2024")

    # 4. Transform
    port.execute("sql/staging_encoded.sql")
    port.execute("sql/staging_benchmarks.sql")
    port.execute("sql/final_output.sql")

    # 5. Publish (idempotent — safe to re-run)
    port.publish(
        version="2026-03-09",
        params={"wstatus": "EMP", "nace_r2": "TOTAL"},
    )
```

---

## Error Reference

| Exception | When raised |
|---|---|
| `ValidationError` | Missing required credentials (`catalog_uri`, `catalog_token`, `warehouse`) |
| `ValueError` | `port.schema()` called with a string that is not a valid `CREATE TABLE` DDL |
| `RuntimeError` | `port.publish()` called before `port.schema()`; DuckDB iceberg extension unavailable |
| `SchemaDriftError` | Local schema is incompatible with the existing warehouse table schema (raised by both `port.schema()` and `port.publish()`) |
| `FileNotFoundError` | `.sql` file path passed to `schema()` or `execute()` does not exist |
