# DBPort — Claude Instructions

## Project Purpose

DuckDB-native runtime for building reproducible warehouse datasets. Provides `DBPort` as the **sole public import** for all warehouse interactions: loading Iceberg tables into DuckDB, running SQL transforms, and publishing outputs back to the warehouse with full metadata and codelist management.

```python
from dbport import DBPort
```

This is the **only public export**. No other symbols from `dbport` should be used by consumers.

---

## First Steps for a New Agent Session

### 1. Install dependencies

```bash
uv sync
```

This installs all Python dependencies into `.venv/` via `uv` (universal Python/venv manager). DuckDB extensions (`iceberg`, `httpfs`) are installed automatically at runtime via HTTPS when first needed — no manual pre-download required.

### 2. Run tests

```bash
uv run pytest
```

Tests live under `tests/test_dbport/`. The 418-test suite covers all domain entities, ports, adapters, and application services. All tests must pass before committing.

### 3. Credentials

Credentials live in `.claude/.env` (git-ignored). Required variables:

| Variable | Description |
|---|---|
| `ICEBERG_REST_URI` | Iceberg REST catalog URL |
| `ICEBERG_CATALOG_TOKEN` | Bearer token for catalog |
| `ICEBERG_WAREHOUSE` | Warehouse name |
| `S3_ENDPOINT` | S3-compatible object store endpoint (optional) |
| `AWS_ACCESS_KEY_ID` | S3 access key ID (optional) |
| `AWS_SECRET_ACCESS_KEY` | S3 secret access key (optional) |

---

## Package Layout

```
src/dbport/                       # source (mapped to import name dbport via pyproject.toml)
  __init__.py                     # single export: DBPort
  adapters/
    primary/
      client.py                   # DBPort — public entrypoint
      columns.py                  # ColumnRegistry + ColumnConfig (.meta(), .attach())
    secondary/
      catalog/
        iceberg.py                # IcebergCatalogAdapter — DuckDB-first data ops
        drift.py                  # SchemaDriftError + check_schema_drift()
      compute/
        duckdb.py                 # DuckDBComputeAdapter — file-backed DuckDB
        ingest_cache.py           # Snapshot-based skip logic for ingest
      lock/
        toml.py                   # TomlLockAdapter — dbport.lock TOML read/write
      metadata/
        materialize.py            # MetadataAdapter — in-memory metadata + codelist bytes
        codelists.py              # Codelist CSV bytes generation from DuckDB (in-memory)
        attach.py                 # Attach metadata/codelists to Iceberg table props
  application/
    services/
      ingest.py                   # IngestService (port.load)
      transform.py                # TransformService (port.execute)
      schema.py                   # DefineSchemaService (port.schema)
      publish.py                  # PublishService (port.publish)
      fetch.py                    # FetchService (auto-called on DBPort init)
  domain/
    entities/                     # Pydantic frozen value objects
      dataset.py                  # Dataset, DatasetKey
      input.py                    # InputDeclaration, IngestRecord
      version.py                  # DatasetVersion, VersionRecord
      schema.py                   # SqlDdl, ColumnDef, DatasetSchema
      codelist.py                 # CodelistEntry, ColumnCodelist
    ports/                        # Protocol interfaces (no I/O)
      catalog.py                  # ICatalog
      compute.py                  # ICompute
      lock.py                     # ILockStore
      metadata.py                 # IMetadataStore
  infrastructure/
    credentials.py                # WarehouseCreds (pydantic-settings, industry-standard env vars)
    logging.py                    # setup_logging() — rich or stdlib fallback

examples/
  minimal/main.py                 # Full Python client API example (filters, metadata, attach, publish modes)
  minimal_cli/run.sh              # Full CLI-driven workflow (all commands and options)

tests/test_dbport/                     # Mirrors src/dbport/ structure exactly
```

---

## Architecture

Hexagonal architecture with three layers:

1. **Domain** — Pure Python value objects (Pydantic frozen models) and port protocols. No I/O.
2. **Application** — Use-case services. Depend only on ports (interfaces), never on adapters.
3. **Adapters** — Concrete implementations (DuckDB, Iceberg REST, TOML, metadata). Wired in `DBPort.__init__`.

All domain entities are immutable (`model_config = ConfigDict(frozen=True)`).

---

## Key Design Rules

- **Single public import**: `from dbport import DBPort` — no other symbols exported.
- **No config files** — credentials from constructor kwargs or environment variables only.
- **No `limit` in `port.load()`** — always loads the full table; scope deliberately with `filters`.
- **Table addresses preserved** — `estat.foo` is always `estat.foo` in DuckDB, never aliased.
- **DuckDB always file-backed** — default path: `data/<dataset_id>.duckdb` relative to the calling script's directory.
- **Arrow-first reads, DuckDB-first writes** — `port.load()` always uses pyiceberg's Arrow C++ multi-threaded parquet reader, streamed into DuckDB via `RecordBatchReader` (no full-table Python memory). `port.publish()` uses the DuckDB `iceberg` extension as the primary write path, falling back to streaming Arrow when the catalog does not support the multi-table commit endpoint. pyiceberg is also used for all metadata operations (snapshot IDs, table properties, column docs).
- **`dbport.lock` is committable** — TOML, no secrets, tracks schema/inputs/versions. Lives at repo root (next to `pyproject.toml`). Multi-model: each model gets `[models."agency.dataset_id"]` section.
- **Fail fast on publish** — check schema drift (local vs warehouse) before writing anything.
- **Idempotent publish** — if `version` already completed, skip silently. Interrupted runs resume from checkpoint in Iceberg table properties (`dbport.upload.v2.<version>.completed`).

---

## DBPort API

```python
from dbport import DBPort

with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:

    # 1. Declare output schema (DDL string or path to .sql file)
    port.schema("sql/create_output.sql")

    # 2. Column metadata (persists to dbport.lock immediately; chain if desired)
    port.columns.nuts2024.meta(codelist_id="NUTS2024", codelist_kind="hierarchical")
    port.columns.nuts2024.attach(table="wifor.cl_nuts2024")

    # 3. Load inputs from warehouse into DuckDB (skipped if snapshot unchanged)
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
    port.load("wifor.cl_nuts2024")

    # 4. Run SQL transforms (inline string or path to .sql file)
    port.execute("sql/staging.sql")
    port.execute("sql/final_output.sql")

    # 5. Publish to warehouse
    port.publish(version="2026-03-09", params={"wstatus": "EMP"})
```

### Constructor

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

On init: sets up logging, validates credentials, creates `data/` dir, opens DuckDB, connects to catalog, reads/creates `dbport.lock`, auto-updates `last_fetched_at`.

### Method summary

| Method | Description |
|---|---|
| `schema(ddl_or_path)` | Declare output table; create in DuckDB; persist DDL + columns to lock |
| `load(table_address, filters=None)` | Load Iceberg table into DuckDB; skip if snapshot unchanged |
| `columns.<name>.meta(...)` | Override codelist metadata for a column; persist immediately |
| `columns.<name>.attach(table=...)` | Use a DuckDB table (loaded via `load()`) as codelist source |
| `execute(sql_or_path)` | Run inline SQL or a `.sql` file in DuckDB |
| `publish(version, params, mode)` | Write output to Iceberg; attach metadata and codelists |
| `close()` | Release DuckDB connection (auto on context manager exit) |

### `publish(mode=...)`

| `mode` | Behaviour |
|---|---|
| `None` (default) | Idempotent — skip if version already completed |
| `"dry"` | Schema validation only; no data written |
| `"refresh"` | Overwrite existing version unconditionally |

---

## Metadata (Fully Automatic)

Never write `metadata.json` or codelist CSVs manually. All lifecycle fields are managed automatically:

- `created_at` — first `publish()` only
- `last_updated_at` — every `publish()`
- `last_fetched_at` — every `DBPort()` initialization
- `inputs` — accumulated from every `port.load()`
- `codelists` — auto-generated per column; override with `.meta()` / `.attach()`
- `versions` — appended on every successful `publish()`

On `publish()`, `metadata.json` is built in-memory and embedded in Iceberg table properties (gzip+base64). Codelist CSVs are also generated in-memory from DuckDB and embedded in Iceberg column docs. No intermediate files are written to disk.

---

## Ingest and Publish Implementation

The DuckDB `iceberg` extension is mandatory for **publish**. It is installed automatically via HTTPS when first needed (see `DuckDBComputeAdapter.ensure_extensions()`).

**Ingest** (`port.load()`) — Arrow path (always):
```python
# pyiceberg scan → RecordBatchReader → DuckDB (Arrow C++ multi-threaded parquet)
scan = iceberg_table.scan(snapshot_id=..., row_filter=EqualTo("wstatus", "EMP"))
reader = scan.to_arrow_batch_reader()
compute.register_arrow("_dbport_ingest_tmp", reader)
compute.execute("CREATE OR REPLACE TABLE estat.nama_10r_3empers AS SELECT * FROM _dbport_ingest_tmp")
compute.unregister_arrow("_dbport_ingest_tmp")
```

**Publish** (`port.publish()`):
```sql
-- First publish (table does not exist):
CREATE TABLE dbport_warehouse.wifor.emp__regional_trends AS
    SELECT * FROM wifor.emp__regional_trends;

-- Subsequent (overwrite=False):
INSERT INTO dbport_warehouse.wifor.emp__regional_trends
    SELECT * FROM wifor.emp__regional_trends;

-- Refresh mode (overwrite=True — DROP then CREATE, since CREATE OR REPLACE
-- is not supported by the DuckDB iceberg extension):
DROP TABLE dbport_warehouse.wifor.emp__regional_trends;
CREATE TABLE dbport_warehouse.wifor.emp__regional_trends AS
    SELECT * FROM wifor.emp__regional_trends;
```

Idempotency checkpoints via pyiceberg table properties:
- `dbport.upload.v2.<version_key>.completed` = `"1"`
- `dbport.upload.v2.<version_key>.rows_appended` = `"<count>"`

### Write Strategy: DuckDB-first with streaming Arrow fallback

DuckDB iceberg extension is the primary write path. When the catalog does not support the multi-table commit endpoint (`POST /transactions/commit` returns 404 — e.g. Supabase), the adapter **auto-switches to streaming Arrow** for the rest of the session.

**Fallback behaviour**:
- Streams 50K-row Arrow batches from DuckDB via `to_arrow_batches()`
- Each batch committed in a single pyiceberg transaction: `tx.append()` + `tx.set_properties()` + `tx.commit_transaction()`
- Checkpoint properties per batch: `dbport.upload.v2.<version>.batches_appended`, `rows_appended`
- On commit conflict (`CommitFailedException`): break batch loop, reload table metadata, resume from remote checkpoint (max 5 retries)
- Batch-enumeration resume: on retry, skip N already-committed batches (works for tables and views)
- Peak memory: ~50K rows × avg_row_bytes per batch (bounded, scales to billion rows)

**Session caching**: `_duckdb_writes_supported` flag on the adapter. Once DuckDB fails with 404, all subsequent writes use the fallback without retrying DuckDB.

---

## Testing

```bash
uv run pytest                              # full suite (418 tests)
uv run pytest tests/test_dbport/adapters/       # adapter tests only
uv run pytest -x -q                        # stop on first failure
```

Tests use `_Fake*` doubles (not mocks) and `tmp_path` fixtures. Each adapter and service has a dedicated test file mirroring the source layout.

---

## See Also

- `docs/client.md` — user-facing usage guide (no internals)
- `docs/dbport.md` — product concept and positioning
- `examples/minimal/main.py` — full Python client API example
- `examples/minimal_cli/run.sh` — full CLI-driven workflow example
- `.claude/.env` — credentials (git-ignored)
