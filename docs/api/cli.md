# CLI Reference

The `dbp` command is the default operational interface for DBPort. It covers project initialization, configuration, execution, publication, and status inspection.

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
2. **`--model` flag** — explicit model directory
3. **CWD matching** — current directory matched against `model_root` entries in the lock
4. **`default_model`** — persisted in `dbport.lock`
5. **First model** — fallback for single-model repos

---

## `dbp init`

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

---

## `dbp status`

Show resolved project and runtime state.

```bash
dbp status [--inputs] [--history] [--raw] [--json]
```

Shows project root, lockfile path, and for each model: agency, dataset ID, schema state, loaded inputs, and published versions.

---

## `dbp config`

Manage project configuration.

### `dbp config KEY [VALUE]`

Get or set a configuration value. Valid keys: `default`, `folder`, `run-hook`, `check`.

```bash
# Show current default model
dbp config default

# Set the default model
dbp config default wifor.emp__regional_trends

# Run health checks
dbp config check
```

### `dbp config model MODEL_KEY schema [SOURCE]`

Show or apply the output schema for a model.

```bash
# Show current schema
dbp config model wifor.emp__regional_trends schema

# Apply schema from SQL file
dbp config model wifor.emp__regional_trends schema sql/create_output.sql
```

### `dbp config model MODEL_KEY input [DATASET]`

Show or configure model inputs.

```bash
# Show configured inputs
dbp config model wifor.emp__regional_trends input

# Add an input
dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers
```

### `dbp config model MODEL_KEY columns`

Manage column metadata.

```bash
# Show all columns
dbp config model wifor.emp__regional_trends columns

# Set codelist metadata
dbp config model wifor.emp__regional_trends columns set geo --id GEO --kind reference

# Attach a codelist table
dbp config model wifor.emp__regional_trends columns attach geo wifor.cl_nuts2024
```

---

## `dbp model sync [MODEL]`

Sync a model from the catalog. Opens the model via DBPort and performs init-time sync.

```bash
dbp model sync
dbp model sync wifor.emp__regional_trends
```

---

## `dbp model load [MODEL]`

Load configured inputs into DuckDB.

```bash
dbp model load
dbp model load --update    # resolve newest snapshots
```

---

## `dbp model exec [MODEL]`

Execute model transforms.

```bash
dbp model exec --target sql/transform.sql --timing
```

| Option | Description |
|---|---|
| `--target PATH` | Override the default execution target |
| `--timing` | Show execution timing |

---

## `dbp model publish [MODEL]`

Publish the output to the warehouse.

```bash
dbp model publish --version 2026-03-15
dbp model publish --dry-run
dbp model publish --refresh
```

| Option | Description |
|---|---|
| `--version TEXT` | Version string for the publish |
| `--dry-run` | Schema validation only, no data written |
| `--refresh` | Overwrite existing version |

---

## `dbp model run [MODEL]`

Full lifecycle: sync, execute, and publish in one command.

```bash
dbp model run --version 2026-03-15 --timing
dbp model run wifor.emp__regional_trends --dry-run
```

Combines `sync`, `exec`, and `publish` into a single operation with all their respective options.

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
