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

## `port.schema(ddl)`

Declares the output table schema. Accepts an inline DDL string or a path to a `.sql` file.

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

The table is created in DuckDB and the schema (DDL + column list) is persisted to `dbport.lock`. A default codelist entry is initialized for every column.

Call `port.schema()` once, early in the script. Re-running the same DDL is idempotent.

**Warehouse validation**: if the output table already exists in the warehouse, `port.schema()` compares the local DDL against the warehouse schema. Raises `SchemaDriftError` if incompatible.

---

## `port.load(table_address, *, filters=None)`

Loads an Iceberg table from the warehouse into DuckDB under its exact original address.

```python
port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP", "nace_r2": "TOTAL"})
port.load("wifor.cl_nuts2024")
```

**Snapshot-based caching**: if the table's snapshot has not changed and the DuckDB relation already exists, the load is skipped automatically.

**No row cap**: `load()` always fetches the full table. Use `filters` to scope data.

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

For dynamic SQL, render the template before passing:

```python
sql = template.format(level=2, parent_table="staging.bench_lvl1")
port.execute(sql)
```

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

Closes the DuckDB connection. Called automatically when using the context manager.

---

## Errors

| Exception | When raised |
|---|---|
| `ValidationError` | Missing required credentials |
| `ValueError` | Invalid DDL string passed to `schema()` |
| `RuntimeError` | `publish()` called before `schema()`; DuckDB extension unavailable |
| `SchemaDriftError` | Local schema incompatible with warehouse (raised by both `schema()` and `publish()`) |
| `FileNotFoundError` | `.sql` file path does not exist |
