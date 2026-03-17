# Changelog

All notable changes to DBPort are documented here. Every published version receives a changelog entry.

Each entry includes: version number, release date, and a summary of changes grouped by category.

---

## 0.0.5 — 2026-03-17

CLI reference and executable workflows.

### Added

- **CLI contract tests** — 18 tests in `test_cli_contract.py` that lock the `dbp` command tree: top-level commands, global options, status/model/config subcommands, all command flags, and absence of stale references
- **Model and version resolution contract tests** — 22 tests in `test_resolution_contract.py` that lock the 5-step model resolution precedence, 3-step version resolution for `run`, 2-step version resolution for `publish`, and the intentional difference between the two strategies
- **Exit code contract** — exit code 0 for success, 1 for user/validation errors, 2 for internal/unexpected errors, 130 for interrupts; `CliUserError` exception class for explicit validation failures
- **JSON error typing** — `--json` error output now includes an `error_type` field (`runtime_error`, `file_not_found`, `validation_error`, `internal_error`, `interrupted`) for automation
- **Exit codes documented** — CLI reference now includes an exit code table

### Changed

- **Stale `dbp project sync` removed** — `dbp init` no longer references the removed `dbp project sync` command; guidance now points to `dbp model sync`
- **Stale `dbp load` / `dbp run` removed** — `dbp init` output now shows correct `dbp model load` and `dbp model run` commands
- **Generic errors now exit 2** — unexpected/internal errors now use exit code 2 instead of 1, distinguishing them from user errors

### Fixed

- **CLI reference rebuilt** — `docs/api/cli.md` now documents the actual nested `config default model/folder/hook` and `config model MODEL_KEY version/input/schema/columns` hierarchy; added missing `--message` flag on publish, `--timing` on exec/run, `dbp status check` section, version resolution documentation, and exit code table
- **CLI example fixed** — `examples/minimal_cli/run.sh` removed stale `dbp project sync` and `dbp status --show-history`; all commands verified against the current CLI
- **Stale `dbp config check` references fixed** — `docs/examples/cli-workflow.md` and `docs/getting-started/credentials.md` now correctly reference `dbp status check`
- **Stale `dbp config default` reference fixed** — `docs/examples/cli-workflow.md` now uses the correct `dbp config default model` syntax

---

## 0.0.4 — 2026-03-17

Python API reference correctness.

### Added

- **Complete Python API reference** — documented `model_root`, `load_inputs_on_init`, `config_only` constructor parameters; documented `port.configure_input()`, `port.run()`, and `port.run_hook`; added sections for initialization behavior, full mode vs. `config_only`, hook resolution, and the relationship between `load()` and `configure_input()`
- **Python client contract tests** — 33 tests in `test_contract.py` that lock the public `DBPort` surface: module exports, constructor signature, public method/property inventory, method signatures, `config_only` guards, return types, and initialization behavior
- **Initialization behavior documentation** — documented the four init phases (path resolution, credential resolution, adapter wiring, state sync) and the error resilience guarantees for each sync step

### Changed

- **Sync output warning level** — `_sync_output_state()` errors now log at warning level instead of debug, since a failed output table creation is user-relevant
- **FetchService error logging** — `FetchService.execute()` now logs failed `last_fetched_at` updates at debug level instead of silently swallowing exceptions

### Improved

- **Init method docstrings** — added detailed docstrings to `__init__`, `_auto_detect_schema`, `_sync_output_state`, `_load_inputs`, and `_update_last_fetched` documenting their behavior, error handling, and guarantees

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
