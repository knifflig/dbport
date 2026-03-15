DBPort CLI Product Story

Overview

The DBPort CLI is the default way to use DBPort.

DBPort is not meant to feel like a Python library that users wire together manually for routine work. It is meant to feel like a production-ready command-line product for building, recomputing, validating, and publishing DuckDB-based data products.

The Python client remains the runtime engine, but the CLI is the primary user interface.

That means:
	•	users should be able to initialize and operate DBPort projects from the CLI
	•	users should configure the project primarily through CLI actions
	•	configuration changes should persist automatically
	•	operational state should be visible and inspectable
	•	recompute should be a first-class production workflow
	•	Python usage should remain available, but as an advanced or embedded interface

⸻

Core Product Positioning

One-sentence positioning

DBPort CLI is the operational interface for building and running reproducible DuckDB data products in production.

What it does

The CLI gives users a single surface to:
	•	initialize a DBPort project
	•	define and persist dataset configuration
	•	resolve and load inputs
	•	run model logic
	•	validate outputs
	•	publish datasets
	•	inspect and manage project configuration
	•	perform conditional recomputes

What it does not try to be

The CLI is not:
	•	a generic ETL shell
	•	a workflow orchestrator
	•	a replacement for dbt
	•	a replacement for Python for model logic
	•	a general configuration framework

Instead, it is the default operational shell around the DBPort runtime.

⸻

Default Usage Model

Default mode: CLI-first

The default intended experience is:
	1.	install DBPort
	2.	create a model/project with the CLI
	3.	define configuration through CLI commands
	4.	persist that configuration automatically
	5.	run and publish through CLI commands
	6.	use Python only when writing custom model logic or embedding DBPort programmatically

Runtime model: client underneath

The Python client remains responsible for the core lifecycle:
	•	schema handling
	•	input loading
	•	execution
	•	validation support
	•	publish semantics
	•	lock/state handling
	•	recompute planning

So the correct mental model is:

CLI is the product surface. Client is the engine.

⸻

Configuration Model

Primary configuration path

By default, configuration should be made via the CLI.

Users should not be expected to hand-edit configuration files for normal operation.

Examples:
	•	adding or changing a dataset binding
	•	setting the output dataset ID
	•	selecting a default model
	•	setting publish defaults
	•	configuring input aliases
	•	storing resolved runtime state

All of these should be done through CLI commands and then persisted automatically.

Persistence target

The primary persistence mechanism is:

dbport.lock

This file is the canonical persisted state of the project and model workflow.

It contains:
	•	default model selection
	•	per-model identity (agency, dataset_id, model_root, duckdb_path)
	•	resolved inputs with snapshot IDs and timestamps
	•	output schema state (DDL, columns, codelist metadata)
	•	publish history (versions, timestamps, row counts)
	•	recompute state (future)

The lock file is TOML, safe to commit, and contains no secrets.

Optional static configuration

Users may optionally place static configuration in:
	•	pyproject.toml
	•	dbport.toml

These are optional sources for defaults and declarative project setup.

Intended role of pyproject.toml

Use for:
	•	package metadata
	•	dependency declarations
	•	console script registration
	•	repo-level DBPort defaults if the project already uses pyproject.toml

Intended role of dbport.toml

Use for:
	•	explicit DBPort project/model configuration in TOML form
	•	teams who want a dedicated config file
	•	static checked-in defaults

Important rule

By default:
	•	CLI writes config/state
	•	lock file persists runtime truth
	•	TOML config is optional, not required

Configuration precedence

	1.	CLI flags
	2.	persisted runtime state in dbport.lock
	3.	dbport.toml
	4.	pyproject.toml
	5.	built-in defaults

That gives the CLI clear authority while still allowing static project defaults.

⸻

Why the CLI is the default

DBPort is a product for:
	•	governed input/output handling
	•	reproducible dataset production
	•	safe publication
	•	inspectable runtime state
	•	periodic recomputes

Those are operational concerns, and operational concerns are best expressed through a CLI.

A CLI-first interface makes DBPort feel like:
	•	a coherent product
	•	a repeatable workflow
	•	a standardizable production tool

⸻

CLI Product Principles

1. Lean by default

The CLI should stay small and memorable.

2. Mirrors the runtime

Command names should align closely with core client concepts.

3. Safe and inspectable

Every important state transition should be visible.

4. Configuration through commands

Users should configure the system by using it, not by editing files.

5. Recompute is first-class

Periodic reruns are a core production behavior, not an afterthought.

6. Human-first, scriptable second

Use Rich for interactive clarity and --json for automation.

⸻

Complete Command Interface

Command surface

dbp init
dbp status
dbp check
dbp schema
dbp load
dbp run
dbp publish
dbp config default
dbp config info
dbp validate          (planned)
dbp recompute         (planned)

The interface is deliberately compact and closely aligned with the product lifecycle.

⸻

Global Options

Supported across all commands:

--help                  Show command help
--version               Show CLI/package version
--verbose, -v           Increase output verbosity
--quiet, -q             Reduce output
--json                  Structured JSON output
--no-color              Disable Rich styling
--project PATH          Explicitly set project root
--model PATH            Explicitly set model directory
--lockfile PATH         Explicitly point to dbport.lock

⸻

Model Resolution

When a command needs to resolve which model to operate on, the following precedence applies:

	1.	Positional MODEL argument — explicit model key (agency.dataset_id), supported by dbp run, dbp publish, and dbp init
	2.	--model flag — explicit model directory relative to project root
	3.	CWD matching — current working directory relative to repo root, matched against model_root entries in the lock
	4.	default_model — persisted in dbport.lock, set automatically on dbp init and changeable via dbp config default
	5.	First model — fallback for single-model repos or when running from the repo root

⸻

Command-by-command reference

dbp init

Initialize or sync models.

This command has two modes: it either **syncs existing models** from the lock file or **scaffolds a new model** and registers it. The mode is determined automatically based on whether the given name or agency/dataset combination matches an existing model in `dbport.lock`.

Interface

dbp init [NAME]

Arguments

NAME              Model key (agency.dataset_id) to sync, or new project name to scaffold

Flags

--template TEXT   Template type: sql, python, or hybrid (default: sql)
--dataset TEXT    Output dataset ID (default: NAME)
--agency TEXT     Agency identifier (default: "default")
--path PATH       Target directory (default: ./<NAME>)
--force           Overwrite existing files

Behavior: sync vs scaffold

The command resolves what to do based on the arguments:

	•	No arguments — syncs all models in the lock file, running them in parallel
	•	Model key that exists in lock (e.g. `test.table1`) — syncs that specific model
	•	`--agency` + `--dataset` matching an existing model — syncs that model
	•	Name or agency/dataset not in lock — scaffolds a new model

Sync mode
	•	Creates a DBPort instance for each model, triggering the init-time sync (schema detection, local state reconciliation)
	•	Multiple models run in parallel using a thread pool (up to 4 concurrent workers)
	•	Progress is displayed as a Rich tree where each model is a parent node and sync steps are children (see Progress Display below)

Scaffold mode (new models)
	•	Creates a directory scaffold with starter SQL templates
	•	Registers the model in the repo-root dbport.lock under [models."agency.dataset_id"]
	•	Sets the newly initialized model as the default (every init updates the default)
	•	model_root and duckdb_path are stored as paths relative to the repo root
	•	Does NOT connect to the warehouse (offline operation)

What scaffold creates

<target>/
  sql/
    create_output.sql     DDL template
    main.sql              Transform template
  data/                   DuckDB data directory
  run.py                  Only for python/hybrid templates

Example usage

dbp init                                    # sync all models in lock file
dbp init test.table1                        # sync a specific existing model
dbp init --agency test --dataset table1     # sync by agency + dataset
dbp init regional_trends --template hybrid --dataset emp__regional_trends --agency wifor  # scaffold new
dbp init --agency wifor --dataset emp_test --path examples/emp_test                       # scaffold new

⸻

dbp status

Show resolved project and runtime state across all models.

This is the dashboard view — a quick operational summary.

Interface

dbp status

Flags

--show-history    Show version publish history table

What it shows
	•	project root and lockfile path
	•	for each model in the lock file:
	  •	agency, dataset ID, model root, DuckDB path
	  •	schema state (defined or not, column count)
	  •	loaded inputs (table addresses, row counts)
	  •	published versions (count, latest version)
	  •	optional version history table (with --show-history)

Example usage

dbp status
dbp status --json
dbp status --show-history

⸻

dbp check

Check whether the project is operationally healthy.

Interface

dbp check

Flags

--strict          Fail on warnings

Checks performed
	•	lockfile — dbport.lock exists
	•	lockfile_readable — valid TOML
	•	duckdb — DuckDB can be imported and run
	•	credentials — required env vars present (ICEBERG_REST_URI, ICEBERG_CATALOG_TOKEN, ICEBERG_WAREHOUSE)
	•	dependencies — pyarrow, pyiceberg, pydantic installed

Each check produces pass, warn, or fail. In --strict mode, any warning becomes a failure.

Example usage

dbp check
dbp check --strict

⸻

dbp schema

Show or apply the output schema contract.

Interface

dbp schema [SOURCE]

Arguments

SOURCE            Path to .sql DDL file to apply (relative to model root)

Flags

--diff            Show schema diff between lock and DuckDB

Behavior
	•	no argument: show current schema from lock file (columns table + DDL)
	•	with path argument: apply schema from SQL file via port.schema()
	•	source path is resolved relative to the model root directory

Example usage

dbp schema
dbp schema sql/create_output.sql
dbp schema --diff
dbp schema --json

⸻

dbp load

Load configured or explicit inputs into DuckDB.

Interface

dbp load [DATASET]

Arguments

DATASET           Table address to load (e.g. estat.table_name)

Flags

--refresh         Force re-load even if snapshot unchanged

Behavior
	•	with argument: load one explicit input table
	•	without argument: load all configured inputs from lock file (reads table_address, filters, and version from each input record)

Example usage

dbp load
dbp load estat.nama_10r_3empers
dbp load --refresh

⸻

dbp run

Run the complete model workflow: sync, execute, and optionally publish.

This is the primary command for running a model end-to-end. It performs the full lifecycle in a single invocation: initializes (syncs) the model, executes the configured run hook, and optionally publishes the result.

Interface

dbp run [MODEL]

Arguments

MODEL             Model key (agency.dataset_id) to run. Optional — falls back to model resolution (see Model Resolution above).

Flags

--version TEXT     Version to publish after execution
--timing          Print execution duration
--dry-run         Validate only; do not publish
--refresh         Overwrite existing version

Behavior
	•	Resolves the model from the positional argument or default model resolution
	•	Creates a DBPort instance, triggering init-time sync (schema detection, input loading)
	•	Reads the run_hook from the lock file and executes it via port.execute()
	•	If --version is provided, publishes after execution
	•	If --refresh or --dry-run is used without --version, falls back to the latest completed version from the lock file
	•	Progress is displayed as a Rich tree (same renderer as dbp init) with sync steps, execution, and publish as child nodes

Example usage

dbp run                                       # run default model
dbp run test.table1                           # run specific model
dbp run --version 2026-03-15                  # run and publish
dbp run test.table1 --version 2026-03-15      # run specific model and publish
dbp run --refresh                             # run and overwrite latest version
dbp run --timing                              # show execution duration

⸻

dbp publish

Publish the current output dataset to the warehouse.

Interface

dbp publish [MODEL]

Arguments

MODEL             Model key (agency.dataset_id) to publish. Optional — falls back to model resolution (see Model Resolution above).

Flags

--version TEXT     Version identifier (e.g. "2026-03-15"). Optional — falls back to the latest completed version in the lock file.
--dry-run          Validate only; do not write data (mode="dry")
--refresh          Overwrite existing version (mode="refresh")
--message TEXT     Publish note for history
--yes, -y          Skip confirmation prompt
--strict           Fail on warnings

Publish modes
	•	default (no flag): idempotent — skip if version already completed
	•	--dry-run: schema validation only, no data written
	•	--refresh: overwrite existing version unconditionally

Version resolution

When --version is not provided, the command reads the lock file and uses the latest completed version. This enables patterns like `dbp publish --refresh` to overwrite the latest version of the default model without specifying the version explicitly. If no completed versions exist in the lock, the command fails with a clear error.

Progress display

Publish uses the same Rich tree renderer as dbp init, showing sync steps and publish progress (including streaming Arrow row counts and ETA for large datasets) as child nodes under the model.

Example usage

dbp publish --version 2026-03-15
dbp publish test.table1 --version 2026-03-15
dbp publish --refresh                             # overwrite latest version of default model
dbp publish test.table1 --refresh                 # overwrite latest version of specific model
dbp publish --version 2026-03-15 --dry-run
dbp publish --version 2026-03-15 --message "Quarterly recompute"

⸻

dbp config

Repo-level control plane for inspecting and managing project configuration.

The config command is a subcommand group. It replaces the earlier dbp lock concept by unifying configuration management and lock file inspection into a single surface.

Design: dbp status is the dashboard (quick operational summary across all models). dbp config is the control plane (deep inspection and configuration of persisted state for the resolved model).

dbp config default

Show or set the default model for the project.

Interface

dbp config default [MODEL_KEY]

Arguments

MODEL_KEY         Model key (agency.dataset_id) to set as default

Behavior
	•	without argument: show current default model
	•	with argument: validate the model exists in the lock, then set it as default

The default model is persisted as a top-level default_model key in dbport.lock:

default_model = "wifor.emp__regional_trends"

[models."wifor.emp__regional_trends"]
...

The default is set automatically on every dbp init. It can be changed deliberately via this command.

Example usage

dbp config default
dbp config default wifor.emp__regional_trends
dbp config default --json

dbp config info

Inspect persisted lock file state for the resolved model.

This is the deep dive into persisted state — input snapshots, version history, raw TOML.

Interface

dbp config info

Flags

--inputs          Show detailed input table
--history         Show detailed version history table
--raw             Dump raw lock file TOML to stdout

Behavior
	•	default (no flags): summary for the resolved model — default model, identity, schema state, input count, version count
	•	--inputs: detailed input table with table address, rows loaded, snapshot ID, snapshot timestamp, and filters
	•	--history: detailed version history table with version, published_at, snapshot ID, rows, and completion status
	•	--raw: raw TOML dump of the entire lock file (useful for debugging and piping)
	•	flags can be combined: --inputs --history shows both tables

Example usage

dbp config info
dbp config info --inputs
dbp config info --history
dbp config info --raw
dbp config info --inputs --history
dbp config info --json

⸻

dbp validate (planned)

Validate whether the current output is ready for publish.

Purpose

Separate validation from publishing so users can explicitly inspect readiness.

Checks
	•	output exists
	•	schema contract is valid
	•	required metadata is present
	•	loaded inputs satisfy requirements
	•	publish target is resolvable

Interface

dbp validate

Flags

--strict

Example usage

dbp validate
dbp validate --strict

⸻

dbp recompute (planned)

Run a full production recompute workflow.

Purpose

This is the first-class production command for periodic reruns.

Why it exists

A production recompute is more than run + publish.

It should:
	•	resolve current inputs
	•	compare input versions/snapshots with lock state
	•	detect whether anything changed
	•	skip unnecessary work when nothing changed
	•	rerun and publish when changes exist
	•	support forcing a rerun when needed

Default behavior

Without flags, dbp recompute should:
	1.	resolve configured inputs
	2.	compare current input state to dbport.lock
	3.	if no inputs changed:
	•	skip rerun
	•	skip publish
	•	update recompute check state only
	4.	if any inputs changed:
	•	load changed inputs
	•	run model
	•	validate
	•	publish
	•	persist recompute outcome

Interface

dbp recompute

Flags

--version TEXT
--check
--force
--refresh
--message TEXT
--yes
--strict

Flag meaning
	•	--check — decide whether recompute is needed, but do not run it
	•	--force — rerun even if no input changes are detected
	•	--refresh — overwrite/rebuild publish target if allowed
	•	--version TEXT — version to publish if recompute proceeds
	•	--message TEXT — optional note for history/logging
	•	--yes — skip prompts
	•	--strict — fail on warnings or ambiguous state

Example usage

dbp recompute
dbp recompute --check
dbp recompute --force --version 2026-03-15
dbp recompute --version 2026-03-15 --message "Scheduled monthly refresh"

⸻

Recompute as a First-Class Product Capability (planned)

This is one of the most important parts of the CLI story.

DBPort is explicitly intended for datasets that are rerun periodically in production.

That means recompute must not be treated as:
	•	an external scripting convention
	•	an orchestration detail only
	•	a manual combination of load, run, and publish

It should be a built-in package concept.

First-class recompute design

Domain idea

A recompute is a decisioned workflow.

The package should compute a plan before work starts.

That plan should answer:
	•	which inputs are configured
	•	which versions/snapshots they currently resolve to
	•	which changed since last successful recompute
	•	whether rerun is required
	•	whether publish is required
	•	why the decision was made

Recommended internal concept

A RecomputePlan object in the runtime layer.

Lock file should track recompute state

The lock file should persist:
	•	last checked time
	•	last recomputed time
	•	last successful publish version
	•	last decision
	•	last reason
	•	per-input last seen vs last loaded versions/snapshots

This makes recompute visible and inspectable.

Skip semantics

If no relevant input changed:
	•	no rerun
	•	no publish
	•	update recompute check state only

Force semantics

A recompute should still be possible with --force for:
	•	code changes
	•	schema changes
	•	metadata changes
	•	operational refreshes

⸻

Installation

Tooling choice

Use uv for dependency management and installation.

uv add typer rich

Dev dependencies:

uv add --dev pytest ruff mypy

Why Typer + Rich
	•	Typer gives a clean, modern command model with type hints
	•	Rich gives readable interactive output, tables, panels, and status display
	•	together they support both:
	•	human-friendly local usage
	•	scriptable machine output via --json

⸻

Repo Layout and File Positions

pyproject.toml
README.md
dbport.lock

src/
  dbport/
    __init__.py
    cli/
      __init__.py          Thin entrypoint: from .main import app; def main(): app()
      main.py              Root Typer app, global options, command registration
      context.py           CliContext dataclass, model resolution, lock helpers
      options.py           Shared Typer option definitions
      render.py            Rich tables, panels, JSON output helpers
      errors.py            cli_error_handler context manager
      commands/
        __init__.py
        init.py            dbp init
        status.py          dbp status
        check.py           dbp check
        schema.py          dbp schema
        load.py            dbp load
        run.py             dbp run
        execute.py         dbp execute
        publish.py         dbp publish
        config.py          dbp config (default, info)
        validate.py        dbp validate (planned)
        recompute.py       dbp recompute (planned)

    adapters/
      primary/
        client.py          DBPort — public entrypoint
        columns.py         ColumnRegistry + ColumnConfig
      secondary/
        catalog/
        compute/
        lock/
          toml.py          TomlLockAdapter — dbport.lock TOML read/write
        metadata/

    application/
      services/

    domain/
      entities/
      ports/

    infrastructure/
      credentials.py
      logging.py

tests/
  test_dbport/
    cli/
      test_init.py
      test_status.py
      test_check.py
      test_schema.py
      test_load.py
      test_run.py
      test_publish.py
      test_config.py
      test_context.py
      test_main.py

⸻

File responsibilities

pyproject.toml

	•	package metadata and dependency declarations
	•	console script registration: dbp = "dbport.cli:main"

dbport.lock

Primary persisted operational state (TOML, committable, no secrets):
	•	default_model — active default for model resolution
	•	per-model identity, schema, inputs, and versions under [models."agency.dataset_id"]

src/dbport/cli/main.py

Root Typer app. Registers all commands and the config sub-app. Handles global options via @app.callback().

src/dbport/cli/context.py

Resolved CLI runtime context:
	•	CliContext dataclass (project_path, lockfile_path, model_dir, output modes)
	•	Model resolution logic (positional MODEL arg > --model flag > CWD match > default_model > first model)
	•	Lock file read/write helpers (read_lock_models, read_default_model, write_default_model, read_lock_versions)
	•	Model-key-based resolution: resolve_model_key(ctx, model_arg) returns (key, data) tuple
	•	Path resolution from model data: resolve_model_paths_from_data(ctx, model_data) returns ModelPaths

src/dbport/cli/options.py

Shared Typer Option definitions reused across commands.

src/dbport/cli/render.py

Rich output helpers:
	•	Tables, panels, summaries, and JSON serialization helpers
	•	Module-level console respects --no-color
	•	`RichProgressAdapter` — flat spinner/progress bar for single-task progress (used by `dbp execute`, scaffold operations)
	•	`ModelNode` — per-model tree progress callback with animated spinners for indeterminate tasks and text-based progress bars (row counts, ETA) for determinate tasks like streaming Arrow publish
	•	`cli_progress()` — context manager wiring Rich progress to the `progress_callback` contextvar (flat display)
	•	`cli_tree_progress()` — context manager rendering a Rich tree with per-model branches; yields a `model_context` factory for thread-safe per-model progress. Used by `dbp init`, `dbp run`, and `dbp publish`

src/dbport/cli/errors.py

cli_error_handler context manager. Catches RuntimeError, FileNotFoundError, KeyboardInterrupt. Renders as CLI error or JSON depending on mode.

⸻

Console entrypoint

In pyproject.toml:

[project.scripts]
dbp = "dbport.cli:main"

⸻

Output model

Human output

Default output uses Rich for:
	•	tables
	•	panels
	•	status summaries
	•	warnings
	•	success/failure states

Progress display

Commands that perform long-running operations show real-time progress using Rich.

Single-task commands (load, execute) use a flat spinner/progress bar:
	•	Indeterminate tasks show an animated spinner with a description
	•	Determinate tasks (e.g. streaming Arrow publish) show a progress bar with row counts, elapsed time, and ETA

Model-level commands (init, run, publish) use a Rich tree layout:
	•	Each model is a parent node with an animated spinner while in progress, then ✓ or ✗ when done
	•	Steps (sync, execute, publish) appear as child nodes under each model
	•	Indeterminate steps show animated spinners with live elapsed time (e.g. schema detection, loading, executing)
	•	Determinate steps show text-based progress bars: `━━━━━━━━━━ 500,000 / 1,485,615 rows  ETA 0:02:45`
	•	For `dbp init` with multiple models, models run in parallel; the tree updates thread-safely from concurrent workers

All tree-based commands (`dbp init`, `dbp run`, `dbp publish`) use the same `cli_tree_progress` renderer for a consistent experience.

Machine output

Every command supports --json.

JSON mode emits structured output:

{
  "ok": true,
  "command": "status",
  "data": {}
}

⸻

Lock File Format

Multi-model TOML with a top-level default_model key and namespaced model sections:

default_model = "wifor.emp__regional_trends"

[models."wifor.emp__regional_trends"]
agency = "wifor"
dataset_id = "emp__regional_trends"
model_root = "models/emp_regional_trends"
duckdb_path = "models/emp_regional_trends/data/emp__regional_trends.duckdb"

[models."wifor.emp__regional_trends".schema]
ddl = "CREATE OR REPLACE TABLE ..."
source = "local"

[[models."wifor.emp__regional_trends".schema.columns]]
column_name = "geo"
column_pos = 0
sql_type = "VARCHAR"
codelist_id = "geo"

[[models."wifor.emp__regional_trends".inputs]]
table_address = "estat.nama_10r_3empers"
last_snapshot_id = 3372712588430296313
last_snapshot_timestamp_ms = 1773053637272
rows_loaded = 1485951

[[models."wifor.emp__regional_trends".versions]]
version = "2026-03-14"
published_at = 2026-03-14T15:07:02Z
iceberg_snapshot_id = 4427638619268409134
rows = 1485615
completed = true

Key properties:
	•	single file per repo, at the repo root next to pyproject.toml
	•	multi-model: each model namespaced under [models."agency.dataset_id"]
	•	model_root and duckdb_path are relative to repo root
	•	no secrets — safe to commit
	•	managed automatically by CLI commands — not meant for hand-editing

⸻

Example user workflow

dbp init regional_trends --template hybrid --dataset emp__regional_trends --agency wifor
cd regional_trends

dbp check
dbp status

dbp schema sql/create_output.sql
dbp load
dbp run --version 2026-03-15 --timing            # full lifecycle: sync, execute, publish
dbp publish --refresh                             # re-publish latest version

dbp run wifor.emp__regional_trends --timing       # address specific model
dbp publish wifor.emp__regional_trends --refresh  # re-publish specific model

dbp config info --inputs
dbp config info --history
dbp config default

⸻

Final CLI Positioning Statement

DBPort CLI is the default operational interface for DBPort.
It lets users configure, run, validate, publish, and recompute DuckDB-based data products through a lean command set, with runtime truth persisted in dbport.lock and optional static defaults in pyproject.toml or dbport.toml.

⸻

Short product pitch

DBPort CLI gives teams a lean, inspectable, production-ready way to operate DuckDB data products.
Configuration happens through commands, state persists in the lock file, recomputes are first-class, and the Python client remains available as the engine underneath.
