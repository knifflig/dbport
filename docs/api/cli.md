# CLI Reference

The `dbp` command is the default operational interface for DBPort. It covers project initialization, configuration, execution, publication, and status inspection.

## Quick reference

| Command | Purpose |
|---|---|
| [`dbp init`](#dbp-init) | Scaffold a new model |
| [`dbp status`](#dbp-status) | Inspect project and model state |
| [`dbp status check`](#dbp-status-check) | Run health checks |
| [`dbp config default ...`](#project-defaults) | Set default model, folder, or hook |
| [`dbp config model ... version`](#dbp-config-model-model-key-version-version) | Set publish version |
| [`dbp config model ... schema`](#dbp-config-model-model-key-schema-source) | Define output schema |
| [`dbp config model ... input`](#dbp-config-model-model-key-input-dataset) | Manage inputs |
| [`dbp config model ... columns`](#dbp-config-model-model-key-columns) | Manage column metadata |
| [`dbp model sync`](#dbp-model-sync-model) | Sync model from catalog |
| [`dbp model load`](#dbp-model-load-model) | Load inputs into DuckDB |
| [`dbp model exec`](#dbp-model-exec-model) | Run transforms |
| [`dbp model publish`](#dbp-model-publish-model) | Publish output to warehouse |
| [`dbp model run`](#dbp-model-run-model) | Full lifecycle in one command |

## Global options

```
--help                  Show command help
--version               Show CLI/package version
--verbose, -v           Increase output verbosity
--quiet, -q             Reduce output
--json                  Structured JSON output
--no-color              Disable Rich styling
--project PATH          Explicitly set project root
--model PATH            Explicitly set model directory
--lockfile PATH         Explicitly point to dbport.lock
```

## Model resolution

When a command needs to resolve which model to operate on:

1. **Positional MODEL argument** — explicit model key (`agency.dataset_id`)
2. **`--model` flag** — explicit model directory relative to project root
3. **CWD matching** — current directory matched against `model_root` entries in the lock
4. **`default_model`** — persisted in `dbport.lock` (set via `dbp config default model`)
5. **First model** — fallback for single-model repos

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | User or validation error (bad input, missing config, schema drift) |
| 2 | Internal or unexpected error |
| 130 | Interrupted (Ctrl+C) |

In `--json` mode, errors include an `error_type` field for automation:

```json
{
  "ok": false,
  "command": "model publish",
  "data": {
    "error": "No completed versions found in lock file. Specify --version explicitly.",
    "error_type": "runtime_error"
  }
}
```

---

## Project setup

### `dbp init`

Initialize a new model scaffold.

```bash
dbp init NAME [--template TEXT] [--dataset TEXT] [--agency TEXT] [--path PATH] [--force]
```

| Option | Default | Description |
|---|---|---|
| `--template` | `sql` | Template type: `sql`, `python`, or `hybrid` |
| `--dataset` | same as NAME | Output dataset ID |
| `--agency` | `"default"` | Agency identifier |
| `--path` | `./<NAME>` | Target directory |
| `--force` | — | Overwrite existing files |

Creates a directory scaffold, registers the model in `dbport.lock`, and sets it as the default model.

```bash
dbp init regional_trends --agency wifor --dataset emp__regional_trends
```

See also: [`dbp status`](#dbp-status), [`dbp config default model`](#dbp-config-default-model-model-key)

### `dbp status`

Show resolved project and runtime state.

```bash
dbp status [--inputs] [--history] [--raw]
```

| Option | Description |
|---|---|
| `--inputs` | Show detailed input information |
| `--history` | Show version publish history |
| `--raw` | Show raw lock file content |

Shows project root, lockfile path, and for each model: agency, dataset ID, schema state, loaded inputs, and published versions.

### `dbp status check`

Run health checks on the project.

```bash
dbp status check [--strict]
```

| Option | Description |
|---|---|
| `--strict` | Fail on warnings (not just failures) |

Verifies: lockfile exists and is valid TOML, DuckDB is available, credentials are set, and Python dependencies are installed.

---

## Configuration

Manage project configuration via `dbp config`. Two sub-groups: `default` (repo-wide settings) and `model` (per-model settings).

### Project defaults

#### `dbp config default model [MODEL_KEY]`

Show or set the default model for the project.

```bash
# Show current default
dbp config default model

# Set the default model
dbp config default model wifor.emp__regional_trends
```

#### `dbp config default folder [FOLDER]`

Show or set the models folder for new models created with `dbp init`.

```bash
# Show current folder
dbp config default folder

# Set the models folder
dbp config default folder examples
```

#### `dbp config default hook [HOOK_PATH]`

Show or set the run hook for the resolved model.

```bash
# Show current hook
dbp config default hook

# Set the run hook
dbp config default hook main.py
```

### Model settings

#### `dbp config model MODEL_KEY version [VERSION]`

Show or set the configured publish version for a model.

```bash
# Show current version
dbp config model wifor.emp__regional_trends version

# Set the version
dbp config model wifor.emp__regional_trends version 2026-03-17
```

#### `dbp config model MODEL_KEY schema [SOURCE]`

Show or apply the output schema for a model.

```bash
# Show current schema
dbp config model wifor.emp__regional_trends schema

# Apply schema from SQL file
dbp config model wifor.emp__regional_trends schema sql/create_output.sql
```

| Option | Description |
|---|---|
| `--diff` | Show schema diff between lock and DuckDB |

See also: [`port.schema()`](python.md#schema) (Python API equivalent)

#### `dbp config model MODEL_KEY input [DATASET]`

Show configured inputs or add one.

```bash
# Show configured inputs
dbp config model wifor.emp__regional_trends input

# Add an input
dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers

# Add an input with filters and load immediately
dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers \
    --filter wstatus=EMP --load
```

| Option | Description |
|---|---|
| `--filter KEY=VALUE` | Equality filter (repeatable) |
| `--version TEXT` | Pinned dataset version to load |
| `--load` | Load the input immediately after configuring it |

See also: [`port.load()`](python.md#load) (Python API equivalent)

#### `dbp config model MODEL_KEY columns`

Show all column metadata for a model.

```bash
dbp config model wifor.emp__regional_trends columns
```

#### `dbp config model MODEL_KEY columns set COLUMN`

Set codelist metadata for a column.

```bash
dbp config model wifor.emp__regional_trends columns set geo --id GEO --kind reference
dbp config model wifor.emp__regional_trends columns set year --type categorical
```

| Option | Description |
|---|---|
| `--id TEXT` | Codelist identifier |
| `--type TEXT` | Codelist type |
| `--kind TEXT` | Codelist kind |
| `--labels JSON` | JSON labels |

See also: [`port.columns.<name>.meta()`](python.md#meta) (Python API equivalent)

#### `dbp config model MODEL_KEY columns attach COLUMN TABLE`

Attach a DuckDB table as the codelist source for a column.

```bash
dbp config model wifor.emp__regional_trends columns attach geo wifor.cl_nuts2024
```

See also: [`port.columns.<name>.attach()`](python.md#attach) (Python API equivalent)

---

## Model operations

### `dbp model sync [MODEL]`

Sync a model from the catalog. Opens the model via DBPort and performs init-time sync (schema auto-detection, local state sync, `last_fetched_at` update).

```bash
dbp model sync
dbp model sync wifor.emp__regional_trends
```

### `dbp model load [MODEL]`

Load configured inputs into DuckDB.

```bash
dbp model load
dbp model load --update    # resolve newest snapshots
```

| Option | Description |
|---|---|
| `--update` | Resolve the newest available snapshot for each configured input |

See also: [`port.load()`](python.md#load) (Python API equivalent)

### `dbp model exec [MODEL]`

Execute model transforms (the configured run hook or an explicit target).

```bash
dbp model exec
dbp model exec --target sql/transform.sql --timing
```

| Option | Description |
|---|---|
| `--target PATH` | Execute this `.sql` or `.py` file instead of the configured hook |
| `--timing` | Show execution duration |

See also: [`port.execute()`](python.md#execute) (Python API equivalent)

### `dbp model publish [MODEL]`

Publish the output to the warehouse.

```bash
dbp model publish --version 2026-03-15
dbp model publish --dry-run
dbp model publish --refresh
dbp model publish --version 2026-03-15 --message "Initial release"
```

| Option | Description |
|---|---|
| `--version TEXT` | Version string for the publish |
| `--dry-run` | Schema validation only, no data written |
| `--refresh` | Overwrite existing version |
| `--message, -m TEXT` | Publish note for history |

**Version resolution** (when `--version` is omitted): uses the latest completed version from the lock file. Does not fall back to the configured `version` field — use `dbp model run` for that.

See also: [`port.publish()`](python.md#publish) (Python API equivalent)

### `dbp model run [MODEL]`

Full lifecycle: sync, execute, and publish in one command.

```bash
dbp model run --version 2026-03-15 --timing
dbp model run wifor.emp__regional_trends --dry-run
```

| Option | Description |
|---|---|
| `--version TEXT` | Version to publish after execution |
| `--target PATH` | Execute this file instead of the configured hook |
| `--timing` | Show execution duration |
| `--dry-run` | Validate only, do not publish |
| `--refresh` | Overwrite existing version |

**Version resolution** (when `--version` is omitted): first checks the configured `version` field in the lock (set via `dbp config model … version`), then falls back to the latest completed version.

See also: [`port.run()`](python.md#run) (Python API equivalent)

---

## Output modes

All commands support `--json` for structured machine output:

```json
{
  "ok": true,
  "command": "status",
  "data": {}
}
```

Default output uses Rich for readable tables, panels, and progress indicators.
