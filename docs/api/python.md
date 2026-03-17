# Python API

`DBPort` is the single public import and entry point for all warehouse operations.

```python
from dbport import DBPort
```

## Quick reference

| Method | Purpose |
|---|---|
| [`DBPort(agency, dataset_id, ...)`](#constructor) | Create a client instance with credentials and paths |
| [`port.schema(ddl_or_path)`](#schema) | Declare the output table schema |
| [`port.load(table, filters, version)`](#load) | Load an Iceberg table into DuckDB |
| [`port.configure_input(table, filters, version)`](#configure_input) | Persist an input declaration without loading data |
| [`port.columns.<name>.meta(...)`](#meta) | Override codelist metadata for a column |
| [`port.columns.<name>.attach(table)`](#attach) | Use a DuckDB table as codelist source |
| [`port.execute(sql_or_path)`](#execute) | Run SQL in DuckDB |
| [`port.run(version, mode)`](#run) | Execute the configured run hook |
| [`port.publish(version, params, mode)`](#publish) | Write output to the Iceberg warehouse |
| [`port.close()`](#close) | Release resources |

---

## Constructor

```python
DBPort(
    agency: str,
    dataset_id: str,
    *,
    catalog_uri: str | None = None,
    catalog_token: str | None = None,
    warehouse: str | None = None,
    s3_endpoint: str | None = None,
    s3_access_key: str | None = None,
    s3_secret_key: str | None = None,
    duckdb_path: str | None = None,
    lock_path: str | None = None,
    model_root: str | None = None,
    load_inputs_on_init: bool = True,
    config_only: bool = False,
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
| `config_only` | `False` | Lightweight mode — see [Full mode vs. config_only](#full-mode-vs-config_only) below |

### Context manager (recommended)

```python
with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
    ...
# port.close() is called automatically
```

### Initialization behavior

Creating a `DBPort` instance runs through four phases:

1. **Path resolution** — discovers the model root directory (from `model_root` kwarg or auto-detected from the calling script), walks up to `pyproject.toml` to find the repo root, and derives the lock path and DuckDB path.

2. **Credential resolution** — merges explicit constructor kwargs with environment variables via `WarehouseCreds`. Explicit kwargs always take precedence.

3. **Adapter wiring** — creates the lock adapter (reads/initializes `dbport.lock`), opens DuckDB, connects to the Iceberg REST catalog, and sets up the in-memory metadata builder.

4. **State sync** — runs four operations, all resilient to errors (logged but never fail initialization):

    | Step | What it does | On error |
    |---|---|---|
    | Auto-detect schema | If no user-declared schema exists in the lock, checks the warehouse for an existing table and imports its schema | Logged at debug level; skipped |
    | Sync output table | Creates the output table in DuckDB from the lock file schema (skipped if the table already exists) | Logged at warning level; skipped |
    | Update `last_fetched_at` | Writes a timestamp to the warehouse table properties (no new snapshot) | Logged at debug level; skipped |
    | Reload inputs | Reloads all inputs declared in `dbport.lock` into DuckDB (only when `load_inputs_on_init=True`) | Per-input errors logged; other inputs still loaded |

### Full mode vs. `config_only`

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

## Methods

Methods are listed in typical workflow order: declare schema, load inputs, configure columns, transform, run, publish.

### `schema()` { #schema }

```python
port.schema(ddl_or_path: str) -> None
```

Declares the output table schema from a DDL string or `.sql` file path.

**Parameters:**

- **`ddl_or_path`** (`str`) — A `CREATE TABLE` DDL string, or a path to a `.sql` file (resolved relative to `model_root`).

**Returns:** `None`

**Raises:**

- `ValueError` — Invalid DDL string.
- `SchemaDriftError` — Local DDL is incompatible with the existing warehouse table.

**Examples:**

```python
# From a .sql file
port.schema("sql/create_output.sql")

# Inline DDL
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

---

### `load()` { #load }

```python
port.load(
    table_address: str,
    *,
    filters: dict[str, str] | None = None,
    version: str | None = None,
) -> IngestRecord
```

Loads an Iceberg table from the warehouse into DuckDB under its exact original address.

**Parameters:**

- **`table_address`** (`str`) — Fully-qualified table address (e.g. `"estat.nama_10r_3empers"`).
- **`filters`** (`dict[str, str] | None`) — Optional equality filters pushed to the Iceberg scan (e.g. `{"wstatus": "EMP"}`). Default: `None` (loads all rows).
- **`version`** (`str | None`) — Pin to a specific dataset version (e.g. `"2025-01-01"`). Default: `None` (latest version). Ignored for tables without DBPort metadata.

**Returns:** `IngestRecord` — a frozen record of the completed ingest, including the snapshot ID, timestamp, row count, and any filters applied.

**Raises:**

- `RuntimeError` — Called in `config_only` mode.

**Examples:**

```python
port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP", "nace_r2": "TOTAL"})
port.load("wifor.cl_nuts2024")

# Pin to a specific version
port.load("wifor.emp__regional_trends", version="2025-01-01")
```

**Snapshot-based caching**: if the table's snapshot has not changed and the DuckDB relation already exists, the load is skipped automatically.

**No row cap**: `load()` always fetches the full table. Use `filters` to scope data.

---

### `configure_input()` { #configure_input }

```python
port.configure_input(
    table_address: str,
    *,
    filters: dict[str, str] | None = None,
    version: str | None = None,
) -> IngestRecord
```

Validates and persists an input declaration to `dbport.lock` **without loading data**.

**Parameters:** Same as [`load()`](#load).

**Returns:** `IngestRecord` — the persisted declaration.

**Raises:**

- `RuntimeError` — Called in `config_only` mode.

**Example:**

```python
port.configure_input("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
```

This is the configuration-only counterpart of `load()`:

| Method | Validates | Persists to lock | Loads data into DuckDB |
|---|---|---|---|
| `load()` | Yes | Yes | Yes |
| `configure_input()` | Yes | Yes | No |

Use `configure_input()` when you want to declare inputs for the lock file (e.g. during project setup) without requiring warehouse connectivity for the actual data load.

---

### `columns` { #columns }

Attribute-style access to per-column metadata. Changes are persisted to `dbport.lock` immediately.

#### `.meta(...)` — override codelist metadata { #meta }

```python
port.columns.<name>.meta(
    codelist_id: str | None = None,
    codelist_kind: str | None = None,
    codelist_type: str | None = None,
    codelist_labels: dict[str, str] | None = None,
) -> ColumnConfig
```

**Parameters:**

- **`codelist_id`** (`str | None`) — Identifier for the codelist. Default: column name.
- **`codelist_kind`** (`str | None`) — `"flat"` or `"hierarchical"`. Default: inferred from SQL type.
- **`codelist_type`** (`str | None`) — Value type hint. Default: inferred from SQL type.
- **`codelist_labels`** (`dict[str, str] | None`) — Human-readable labels per language. Default: `None`.

**Returns:** `ColumnConfig` (self, for chaining).

**Example:**

```python
port.columns.nuts2024.meta(
    codelist_id="NUTS2024",
    codelist_kind="hierarchical",
    codelist_labels={"en": "NUTS 2024 Regions"},
)
```

#### `.attach(table=...)` — use a DuckDB table as codelist source { #attach }

```python
port.columns.<name>.attach(table: str) -> ColumnConfig
```

**Parameters:**

- **`table`** (`str`) — Address of a DuckDB table to use as the codelist source. Should already be loaded via `port.load()`.

**Returns:** `ColumnConfig` (self, for chaining).

**Example:**

```python
port.columns.nuts2024.attach(table="wifor.cl_nuts2024")
```

On `publish()`, the full table is exported as the codelist for this column.

---

### `execute()` { #execute }

```python
port.execute(sql_or_path: str) -> None
```

Runs a SQL statement or a `.sql` file in DuckDB.

**Parameters:**

- **`sql_or_path`** (`str`) — Inline SQL string, or a path to a `.sql` file (resolved relative to `model_root`).

**Returns:** `None`

**Raises:**

- `FileNotFoundError` — `.sql` file path does not exist.
- `RuntimeError` — Called in `config_only` mode.

**Examples:**

```python
port.execute("sql/staging.sql")
port.execute("CREATE TABLE staging.ranked AS SELECT ...")

# Dynamic SQL
sql = template.format(level=2, parent_table="staging.bench_lvl1")
port.execute(sql)
```

---

### `run()` { #run }

```python
port.run(*, version: str | None = None, mode: str | None = None) -> None
```

Executes the configured run hook, optionally publishing afterward.

**Parameters:**

- **`version`** (`str | None`) — When provided, `publish(version=version, mode=mode)` is called automatically after the hook completes. Default: `None` (no publish).
- **`mode`** (`str | None`) — Publish mode, forwarded to `publish()`. Only used when `version` is set. See [`publish()`](#publish) for valid values.

**Returns:** `None`

**Raises:**

- `ValueError` — Hook file has an unsupported extension (not `.py` or `.sql`).
- `RuntimeError` — Called in `config_only` mode.

**Examples:**

```python
# Run the hook without publishing
port.run()

# Run the hook and publish
port.run(version="2026-03-09", mode="dry")
```

#### Hook resolution

The run hook is resolved in this order:

1. Explicit hook path set in `dbport.lock` (via CLI `dbp config run-hook`)
2. `main.py` in the model root (if it exists)
3. `sql/main.sql` in the model root (if it exists)
4. Falls back to `main.py` (may not exist — will error on execution)

#### Hook dispatch by file extension

| Extension | Behavior |
|---|---|
| `.sql` | Executed via `port.execute()` |
| `.py` | Loaded as a Python module with `port` available in scope. If the module defines a top-level `run(port)` function, it is called after the module is loaded |
| Other | Raises `ValueError` |

#### `port.run_hook` property

```python
port.run_hook -> str | None
```

Read-only property that returns the resolved hook path as a string, or `None` if no hook can be found. Returns the path that `run()` would execute, without actually running it.

---

### `publish()` { #publish }

```python
port.publish(
    *,
    version: str,
    params: dict[str, str] | None = None,
    mode: str | None = None,
) -> None
```

Writes the output table from DuckDB to the Iceberg warehouse.

**Parameters:**

- **`version`** (`str`) — Version label for this publish (e.g. `"2026-03-09"`). Required.
- **`params`** (`dict[str, str] | None`) — Key-value parameters describing this version (e.g. `{"wstatus": "EMP"}`). Default: `None`.
- **`mode`** (`str | None`) — Publish mode. Default: `None`.

| `mode` value | Behavior |
|---|---|
| `None` (default) | Idempotent — skip if version already completed; resume from checkpoint if interrupted |
| `"dry"` | Schema validation only — no data written |
| `"refresh"` | Overwrite existing version unconditionally |

**Returns:** `None`

**Raises:**

- `RuntimeError` — `port.schema()` has not been called; or called in `config_only` mode.
- `SchemaDriftError` — Local schema incompatible with warehouse.

**Pre-publish checks** (in order):

1. **Schema defined** — raises `RuntimeError` if `port.schema()` has not been called
2. **Version idempotency** — if version already completed, returns immediately (skipped in `refresh` mode)
3. **Schema drift** — compares local vs warehouse schema; raises `SchemaDriftError` with a diff if incompatible

**Example:**

```python
port.publish(
    version="2026-03-09",
    params={"wstatus": "EMP", "nace_r2": "TOTAL"},
)
```

**On success:**

- Data written to `<agency>.<dataset_id>` in Iceberg
- Codelists auto-generated per column
- `metadata.json` materialized and embedded in table properties
- `VersionRecord` appended to `dbport.lock`
- `created_at` set on first publish; `last_updated_at` updated every time

---

### `close()` { #close }

```python
port.close() -> None
```

Closes the DuckDB connection. Called automatically when using the context manager. In `config_only` mode, this is a no-op.

**Parameters:** None

**Returns:** `None`

---

## Complete example

```python
from dbport import DBPort

with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:

    # 1. Declare output schema
    port.schema("sql/create_output.sql")

    # 2. Column metadata
    port.columns.nuts2024.meta(codelist_id="NUTS2024", codelist_kind="hierarchical")
    port.columns.nuts2024.attach(table="wifor.cl_nuts2024")

    # 3. Load inputs from warehouse
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
    port.load("wifor.cl_nuts2024")

    # 4. Run SQL transforms
    port.execute("sql/staging.sql")
    port.execute("sql/final_output.sql")

    # 5. Publish to warehouse
    port.publish(version="2026-03-09", params={"wstatus": "EMP"})
```

See also: [Python workflow example](../examples/python-workflow.md), [CLI workflow example](../examples/cli-workflow.md).

---

## Errors

| Exception | When raised |
|---|---|
| `ValidationError` | Missing required credentials |
| `ValueError` | Invalid DDL string passed to `schema()`; unsupported hook file extension |
| `RuntimeError` | `publish()` called before `schema()`; data method called in `config_only` mode; DuckDB extension unavailable |
| `SchemaDriftError` | Local schema incompatible with warehouse (raised by both `schema()` and `publish()`) |
| `FileNotFoundError` | `.sql` file path does not exist |
