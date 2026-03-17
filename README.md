# dbport

DuckDB-native runtime for building reproducible warehouse datasets. `DBPort` is the single entry point for loading Iceberg tables into DuckDB, running SQL transforms, and publishing outputs back to the warehouse with automatic metadata and codelist management.

```python
from dbport import DBPort

with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
    port.schema("sql/create_output.sql")
    port.columns.nuts2024.meta(codelist_id="NUTS2024", codelist_kind="hierarchical")
    port.columns.nuts2024.attach(table="wifor.cl_nuts2024")
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
    port.load("wifor.cl_nuts2024")
    port.execute("sql/staging.sql")
    port.execute("sql/final_output.sql")
    port.publish(version="2026-03-09", params={"wstatus": "EMP"})
```

---

## Key Features

- **One import** — `from dbport import DBPort`. No other public symbols.
- **DuckDB-first** — all data operations (ingest + publish) go through the DuckDB `iceberg` extension. No pyarrow batch loops; tables up to 1 billion rows stream directly in SQL.
- **Snapshot-cached ingest** — `port.load()` skips a table if its Iceberg snapshot is unchanged since the last run.
- **Idempotent publish** — re-running the same version is safe; interrupted runs resume from a checkpoint.
- **Schema drift protection** — `publish()` compares local schema to the warehouse before writing anything and raises a clear diff if they diverge.
- **Automatic metadata** — `created_at`, `last_updated_at`, `last_fetched_at`, `inputs`, `codelists`, `versions` are all managed hands-free. No `metadata.json` to write manually.
- **Committable lock file** — `dbport.lock` is TOML, credential-free, and tracks schema, ingest state, and version history. Safe to commit.
- **Multi-model repos** — a single `dbport.lock` at the repo root handles multiple models in namespaced sections.

---

## Requirements

- Python 3.11 – 3.12
- DuckDB `iceberg` extension (installed automatically at runtime)
- Iceberg REST catalog with S3-compatible object store

---

## Installation

```bash
pip install dbport
```

For development:

```bash
uv sync
```

---

## Credentials

Set these environment variables (or pass as constructor kwargs):

| Variable | Required | Description |
|---|---|---|
| `ICEBERG_REST_URI` | Yes | Iceberg REST catalog URL |
| `ICEBERG_CATALOG_TOKEN` | Yes | Bearer token |
| `ICEBERG_WAREHOUSE` | Yes | Warehouse name |
| `S3_ENDPOINT` | No | S3-compatible endpoint |
| `AWS_ACCESS_KEY_ID` | No | S3 access key ID |
| `AWS_SECRET_ACCESS_KEY` | No | S3 secret key |

Credentials are never written to disk.

---

## Documentation

Full API reference and guides: [knifflig.github.io/dbport](https://knifflig.github.io/dbport)

- [Python API](https://knifflig.github.io/dbport/latest/api/python/) — `DBPort` class reference
- [CLI Reference](https://knifflig.github.io/dbport/latest/api/cli/) — `dbp` command reference
- [Getting Started](https://knifflig.github.io/dbport/latest/getting-started/) — installation, credentials, quickstart

---

## The `dbport.lock` File

`dbport.lock` lives at the repo root (next to `pyproject.toml`), is TOML-formatted, contains no secrets, and is safe to commit. It serves three purposes:

1. **Schema registry** — stores the DDL and per-column codelist configuration
2. **Ingest cache** — tracks Iceberg snapshot IDs so unchanged tables are skipped
3. **Version history** — append-only log of completed publishes

Multiple models in the same repo each get their own `[models."agency.dataset_id"]` section.

---

## Project Structure

```
src/dbport/              # source (imported as dbport)
  adapters/primary/      # DBPort, ColumnRegistry
  adapters/secondary/    # DuckDB, Iceberg, TOML, metadata adapters
  application/services/  # ingest, transform, schema, publish use cases
  domain/                # value objects (frozen Pydantic) + port interfaces
  infrastructure/        # credentials, logging

examples/
  minimal/               # minimal load → transform → publish
  minimal_cli/           # CLI-driven workflow

tests/test_dbport/       # tests mirroring src/ structure
docs/                    # documentation source
```
