# Python API

`DBPort` is the single public import and entry point for all warehouse operations.

```python
from dbport import DBPort
```

## Constructor

```python
DBPort(
    agency: str,
    dataset_id: str,
    *,
    catalog_uri: str | None = None,     # falls back to ICEBERG_REST_URI
    catalog_token: str | None = None,   # falls back to ICEBERG_CATALOG_TOKEN
    warehouse: str | None = None,       # falls back to ICEBERG_WAREHOUSE
    s3_endpoint: str | None = None,     # falls back to S3_ENDPOINT
    s3_access_key: str | None = None,   # falls back to AWS_ACCESS_KEY_ID
    s3_secret_key: str | None = None,   # falls back to AWS_SECRET_ACCESS_KEY
    duckdb_path: str | None = None,     # default: <caller_dir>/data/<dataset_id>.duckdb
    lock_path: str | None = None,       # default: repo root (next to pyproject.toml)
    model_root: str | None = None,      # default: caller's directory (auto-detected)
    load_inputs_on_init: bool = True,   # reload lock-file inputs on startup
    config_only: bool = False,          # lightweight mode (no DuckDB/catalog)
)
```

### Parameters

| Parameter | Default | Description |
|---|---|---|
| `agency` | *(required)* | Agency namespace (e.g. `"wifor"`, `"estat"`) |
| `dataset_id` | *(required)* | Dataset identifier (e.g. `"emp__regional_trends"`) |
| `catalog_uri` | `None` | Iceberg REST catalog URL. Falls back to `ICEBERG_REST_URI` |
| `catalog_token` | `None` | Bearer token for catalog. Falls back to `ICEBERG_CATALOG_TOKEN` |
| `warehouse` | `None` | Warehouse name. Falls back to `ICEBERG_WAREHOUSE` |
| `s3_endpoint` | `None` | S3-compatible endpoint. Falls back to `S3_ENDPOINT` |
| `s3_access_key` | `None` | S3 access key. Falls back to `AWS_ACCESS_KEY_ID` |
| `s3_secret_key` | `None` | S3 secret key. Falls back to `AWS_SECRET_ACCESS_KEY` |
| `duckdb_path` | `None` | Path to the DuckDB file. Default: `<model_root>/data/<dataset_id>.duckdb` |
| `lock_path` | `None` | Path to `dbport.lock`. Default: repo root (next to `pyproject.toml`) |
| `model_root` | `None` | Model directory for resolving SQL file paths and the default DuckDB location. Default: auto-detected from the calling script's directory |
| `load_inputs_on_init` | `True` | When `True`, inputs previously declared in `dbport.lock` are reloaded into DuckDB on startup. Set to `False` to skip automatic input loading |
| `config_only` | `False` | Lightweight mode — see [Full mode vs. config_only](#full-mode-vs-config_only) |

### Context manager (recommended)

```python
with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
    ...
# port.close() is called automatically
```

---

## Initialization behavior

Creating a `DBPort` instance runs through four phases. In **full mode** (default):

1. **Path resolution** — discovers the model root directory (from `model_root` kwarg or auto-detected from the calling script), walks up to `pyproject.toml` to find the repo root, and derives the lock path and DuckDB path.

2. **Credential resolution** — merges explicit constructor kwargs with environment variables via `WarehouseCreds`. Explicit kwargs always take precedence.

3. **Adapter wiring** — creates the lock adapter (reads/initializes `dbport.lock`), opens DuckDB, connects to the Iceberg REST catalog, and sets up the in-memory metadata builder.

4. **State sync** — runs four operations, all of which are resilient to errors (logged but never fail initialization):

    | Step | What it does | On error |
    |---|---|---|
    | Auto-detect schema | If no user-declared schema exists in the lock, checks the warehouse for an existing table and imports its schema | Logged at debug level; skipped |
    | Sync output table | Creates the output table in DuckDB from the lock file schema (skipped if the table already exists) | Logged at warning level; skipped |
    | Update `last_fetched_at` | Writes a timestamp to the warehouse table properties (no new snapshot) | Logged at debug level; skipped |
    | Reload inputs | Reloads all inputs declared in `dbport.lock` into DuckDB (only when `load_inputs_on_init=True`) | Per-input errors logged; other inputs still loaded |

In **config_only mode**, only the lock adapter and column registry are created. All sync phases are skipped entirely.

---

## Full mode vs. `config_only`

| Aspect | Full mode (default) | `config_only=True` |
|---|---|---|
| Credentials | Required (kwargs or env vars) | Not needed |
| DuckDB | Opened; `data/` directory created | Not opened; no directory created |
| Catalog connection | Established | Not established |
| Lock file | Read/initialized | Read/initialized |
| State sync | All four phases run | All skipped |
| `columns.meta()` / `columns.attach()` | Works | Works |
| `schema()`, `load()`, `execute()`, `run()`, `configure_input()`, `publish()` | Works | Raises `RuntimeError` |
| `close()` | Releases DuckDB | No-op |

Use `config_only=True` when you need to manipulate column metadata or lock file state without warehouse access:

```python
with DBPort(agency="wifor", dataset_id="emp__regional_trends", config_only=True) as port:
    port.columns.nuts2024.meta(codelist_id="NUTS2024", codelist_kind="hierarchical")
    port.columns.nuts2024.attach(table="wifor.cl_nuts2024")
```

---

## `port.schema(ddl)`

Declares the output table schema. Accepts an inline DDL string or a path to a `.sql` file.

```python
# Path to a .sql file (resolved relative to model_root)
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

The table is created in DuckDB and the schema (DDL + column list) is persisted to `dbport.lock`. A default codelist entry is initialized for every column.

Call `port.schema()` once, early in the script. Re-running the same DDL is idempotent.

**Warehouse validation**: if the output table already exists in the warehouse, `port.schema()` compares the local DDL against the warehouse schema. Raises `SchemaDriftError` if incompatible.

---

## `port.load(table_address, *, filters=None, version=None)`

Loads an Iceberg table from the warehouse into DuckDB under its exact original address.

```python
port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP", "nace_r2": "TOTAL"})
port.load("wifor.cl_nuts2024")
```

**Returns**: `IngestRecord` — a frozen record of the completed ingest, including the snapshot ID, timestamp, row count, and any filters applied.

**Snapshot-based caching**: if the table's snapshot has not changed and the DuckDB relation already exists, the load is skipped automatically.

**No row cap**: `load()` always fetches the full table. Use `filters` to scope data.

**Version pinning**: for tables published by DBPort, load is automatically pinned to a specific Iceberg snapshot. Pass an explicit `version` string to load a historical release:

```python
port.load("wifor.emp__regional_trends", version="2025-01-01")
```

For tables without DBPort metadata (e.g. Eurostat inputs), `version` is ignored and the current Iceberg snapshot is used.

---

## `port.configure_input(table_address, *, filters=None, version=None)`

Validates and persists an input declaration to `dbport.lock` **without loading data**.

```python
port.configure_input("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
```

**Returns**: `IngestRecord` — the persisted declaration.

This is the configuration-only counterpart of `load()`:

| Method | Validates | Persists to lock | Loads data into DuckDB |
|---|---|---|---|
| `load()` | Yes | Yes | Yes |
| `configure_input()` | Yes | Yes | No |

Use `configure_input()` when you want to declare inputs for the lock file (e.g. during project setup) without requiring warehouse connectivity for the actual data load.

---

## `port.columns.<name>`

Attribute-style access to per-column metadata. Changes are persisted to `dbport.lock` immediately.

### `.meta(...)` — override codelist metadata

```python
port.columns.nuts2024.meta(
    codelist_id="NUTS2024",
    codelist_kind="hierarchical",
    codelist_labels={"en": "NUTS 2024 Regions"},
)
```

| Parameter | Default | Description |
|---|---|---|
| `codelist_id` | column name | Identifier for the codelist |
| `codelist_kind` | inferred from SQL type | `"flat"` or `"hierarchical"` |
| `codelist_type` | inferred from SQL type | Value type hint |
| `codelist_labels` | `null` | Human-readable labels per language |

Returns `self` for chaining.

### `.attach(table=...)` — use a DuckDB table as codelist source

```python
port.columns.nuts2024.attach(table="wifor.cl_nuts2024")
```

The table should already be loaded via `port.load()`. On `publish()`, the full table is exported as the codelist.

---

## `port.execute(sql)`

Runs a SQL statement or a `.sql` file in DuckDB.

```python
port.execute("sql/staging.sql")
port.execute("CREATE TABLE staging.ranked AS SELECT ...")
```

File paths are resolved relative to `model_root`. For dynamic SQL, render the template before passing:

```python
sql = template.format(level=2, parent_table="staging.bench_lvl1")
port.execute(sql)
```

---

## `port.run(*, version=None, mode=None)`

Executes the configured run hook, optionally publishing afterward.

```python
# Run the hook without publishing
port.run()

# Run the hook and publish
port.run(version="2026-03-09", mode="dry")
```

### Hook resolution

The run hook is resolved in this order:

1. Explicit hook path set in `dbport.lock` (via CLI `dbp config run-hook`)
2. `main.py` in the model root (if it exists)
3. `sql/main.sql` in the model root (if it exists)
4. Falls back to `main.py` (may not exist — will error on execution)

### Hook dispatch by file extension

| Extension | Behavior |
|---|---|
| `.sql` | Executed via `port.execute()` |
| `.py` | Loaded as a Python module with `port` available in scope. If the module defines a top-level `run(port)` function, it is called after the module is loaded |
| Other | Raises `ValueError` |

### Optional publish

When `version` is provided, `port.publish(version=version, mode=mode)` is called automatically after the hook completes. When `version` is `None`, the hook runs without publishing.

---

## `port.run_hook`

Read-only property that returns the resolved hook path as a string, or `None` if no hook can be found.

```python
print(port.run_hook)  # e.g. "main.py" or "sql/main.sql"
```

This returns the path that `port.run()` would execute, without actually running it. Useful for inspection and debugging.

---

## `port.publish(*, version, params=None, mode=None)`

Writes the output table from DuckDB to the Iceberg warehouse.

```python
port.publish(
    version="2026-03-09",
    params={"wstatus": "EMP", "nace_r2": "TOTAL"},
)
```

### `mode` parameter

| Value | Behavior |
|---|---|
| `None` (default) | Idempotent — skip if version already completed; resume from checkpoint if interrupted |
| `"dry"` | Schema validation only — no data written |
| `"refresh"` | Overwrite existing version unconditionally |

### Pre-publish checks (in order)

1. **Schema defined** — raises `RuntimeError` if `port.schema()` has not been called
2. **Version idempotency** — if version already completed, returns immediately (skipped in `refresh` mode)
3. **Schema drift** — compares local vs warehouse schema; raises `SchemaDriftError` with a diff if incompatible

### On success

- Data written to `<agency>.<dataset_id>` in Iceberg
- Codelists auto-generated per column
- `metadata.json` materialized and embedded in table properties
- `VersionRecord` appended to `dbport.lock`
- `created_at` set on first publish; `last_updated_at` updated every time

---

## `port.close()`

Closes the DuckDB connection. Called automatically when using the context manager. In `config_only` mode, this is a no-op.

---

## Errors

| Exception | When raised |
|---|---|
| `ValidationError` | Missing required credentials |
| `ValueError` | Invalid DDL string passed to `schema()`; unsupported hook file extension |
| `RuntimeError` | `publish()` called before `schema()`; data method called in `config_only` mode; DuckDB extension unavailable |
| `SchemaDriftError` | Local schema incompatible with warehouse (raised by both `schema()` and `publish()`) |
| `FileNotFoundError` | `.sql` file path does not exist |
