# Changelog

All notable changes to DBPort are documented here. Every published version receives a changelog entry.

Each entry includes: version number, release date, and a summary of changes grouped by category.

---

## 0.0.3 — 2026-03-17

Version policy and release planning language.

### Added

- **Release versioning policy** — new `docs/release-versioning.md` page documenting the project's `X.Y.Z` numbering convention (major / normal / minor), the predevelopment milestone path from `0.0.1` to `0.1.0`, the single source of truth in `pyproject.toml`, and the per-version release checklist
- **Release Versioning** added to docs site navigation

### Fixed

- **CLI version fallback** — `dbp --version` no longer reports a hard-coded `"0.1.0"` when package metadata is unavailable; now reports `"unknown"` instead of a misleading future version

---

## 0.0.2 — 2026-03-17

Release history and roadmap foundations.

### Added

- **Changelog page** — first-class changelog in the docs site, structured around versions
- **Roadmap page** — milestone-based roadmap covering package, CLI, runtime, docs, release, and testing

---

## 0.0.1 — 2026-03-16

Foundation release. Establishes the full runtime, architecture, CLI, documentation site, and CI pipeline.

### Core runtime

- **Single public import** — `from dbport import DBPort` is the only supported entry point
- **Hexagonal architecture** — domain (entities + ports), application (services), adapters (concrete implementations), wired in `DBPort.__init__`
- **Context manager** — `with DBPort(...) as port:` for automatic resource cleanup
- **Constructor** with optional kwargs for all credentials and paths; falls back to `.env` then environment variables
- **Config-only mode** — `DBPort(config_only=True)` for lightweight initialization without warehouse connection

### Data operations

- **`port.schema(ddl_or_path)`** — declare output table from inline DDL or `.sql` file; creates table in DuckDB and persists columns to lock
- **`port.load(table_address, filters=...)`** — load Iceberg tables into DuckDB via Arrow C++ multi-threaded parquet reader; snapshot-cached (skips unchanged tables)
- **`port.configure_input(table_address, ...)`** — validate and persist input declaration without loading data
- **`port.execute(sql_or_path)`** — run inline SQL or `.sql` files in DuckDB
- **`port.run()`** — execute configured run hooks (Python or SQL)
- **`port.publish(version, params, mode)`** — write output to Iceberg with full metadata and codelist attachment; supports idempotent, dry, and refresh modes

### Column metadata

- **`port.columns.<name>.meta(...)`** — override codelist metadata per column (codelist ID, type, kind, labels); persists immediately to lock
- **`port.columns.<name>.attach(table=...)`** — use a loaded DuckDB table as the codelist source for a column

### Write strategy

- **DuckDB-first publish** — uses the DuckDB `iceberg` extension as the primary write path
- **Streaming Arrow fallback** — auto-switches to 50K-row Arrow batches when the catalog lacks multi-table commit support; per-batch checkpoints, conflict retry with resume
- **Schema drift protection** — fail-fast comparison of local vs warehouse schema before any write
- **Idempotent publish** — checkpoint tracking via Iceberg table properties (`dbport.upload.v2.<version>.completed`)

### Metadata lifecycle

- Fully automatic metadata management: `created_at`, `last_updated_at`, `last_fetched_at`, inputs, codelists, versions
- In-memory `metadata.json` generation (gzip+base64, embedded in Iceberg table properties)
- In-memory codelist CSV generation from DuckDB (embedded in Iceberg column docs)
- No intermediate files written to disk

### Domain model

- **Immutable value objects** — all Pydantic frozen models: `Dataset`, `DatasetKey`, `DatasetSchema`, `SqlDdl`, `ColumnDef`, `DatasetVersion`, `VersionRecord`, `InputDeclaration`, `IngestRecord`, `CodelistEntry`, `ColumnCodelist`
- **Port protocols** — `ICatalog`, `ICompute`, `ILockStore`, `IMetadataStore`

### Adapters

- **IcebergCatalogAdapter** — DuckDB-first data ops with pyiceberg for metadata; S3-compatible object stores
- **DuckDBComputeAdapter** — file-backed DuckDB with auto-extension loading (`iceberg`, `httpfs`, `avro`)
- **TomlLockAdapter** — `dbport.lock` TOML file at repo root; multi-model, credential-free, committable
- **MetadataAdapter** — in-memory metadata JSON builder with version-aware `created_at` preservation
- **Codelist adapters** — CSV generation from output columns and attached tables
- **Attach adapter** — gzip+base64 metadata and codelist embedding into Iceberg table properties
- **Schema drift checker** — PyArrow schema comparison with detailed diff reporting
- **Ingest cache** — snapshot ID comparison to skip unchanged tables

### Application services

- `IngestService` — snapshot resolution, cache check, Arrow-streamed load
- `TransformService` — SQL execution from strings or files
- `DefineSchemaService` — DDL parsing, DuckDB table creation, lock persistence
- `PublishService` — schema validation, idempotent write, metadata+codelist attachment
- `FetchService` — `last_fetched_at` update on init
- `AutoSchemaService` — warehouse schema auto-detection with Arrow-to-DuckDB type mapping
- `SyncService` — local DuckDB sync from lock file state
- `RunService` — hook execution (Python and SQL) with auto-detection

### CLI

- **`dbp` command** with global options: `--version`, `--project`, `--lockfile`, `--model`, `--verbose`, `--quiet`, `--json`, `--no-color`
- **`dbp init`** — scaffold a new model with template files
- **`dbp status`** — show project state, inputs, versions, lock content
- **`dbp model sync|load|execute|publish`** — full model lifecycle commands
- **`dbp config`** — environment and credential management
- **`dbp schema`** — output schema management
- **`dbp check`** — configuration validation
- **Rich console output** — tables, trees, progress rendering, JSON mode

### Infrastructure

- **`WarehouseCreds`** — pydantic-settings credential resolution (kwargs → `.env` → env vars)
- **`setup_logging()`** — Rich logging with stdlib fallback; silences noisy third-party loggers
- **Progress callbacks** — context-variable-based progress protocol for CLI integration

### Documentation site

- **Zensical-powered docs** with deep purple + amber theme, light/dark mode toggle
- **Getting Started** — installation, credentials, quickstart
- **Concepts** — inputs, outputs, metadata, lock file, versioning & publish
- **API Reference** — Python client API, CLI commands
- **Examples** — Python workflow, CLI workflow
- **Versioned deployment** — mike-compatible directory structure (`/<version>/`, `/latest/`, `versions.json`)
- **Local preview** — `scripts/preview_docs.sh` builds a real versioned tree in `_preview/`

### Testing

- **920+ tests** across 41 test modules mirroring the source layout
- **95% coverage requirement** enforced in `pyproject.toml`
- Uses `_Fake*` test doubles and `tmp_path` fixtures
- Covers domain entities, all adapters, all services, CLI commands, and infrastructure

### CI/CD

- **CI workflow** — pytest on Python 3.11 and 3.12, ruff lint + format check, docs build verification
- **Docs workflow** — tag-triggered GitHub Pages deployment with version validation against `pyproject.toml`
- **`scripts/update_versions.py`** — manages multi-version `versions.json` during deployment

### Examples

- **`examples/minimal/main.py`** — full Python client API usage (schema, meta, attach, load, execute, publish modes)
- **`examples/minimal_cli/run.sh`** — full CLI-driven workflow with all commands
