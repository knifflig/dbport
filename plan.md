# CLI Improvement Plan

## Current Commands

| Command | Purpose |
|---|---|
| `dbp init [NAME]` | Scaffold new model or sync existing models |
| `dbp status` | Show project/model state summary |
| `dbp check` | Verify project health (lockfile, DuckDB, creds, deps) |
| `dbp schema [SOURCE]` | Show or apply output schema |
| `dbp load [DATASET]` | Load inputs into DuckDB |
| `dbp execute TARGET` | Run a SQL file in DuckDB |
| `dbp run [MODEL]` | Full workflow: sync + execute + publish |
| `dbp publish [MODEL]` | Publish output to warehouse |
| `dbp config default` | Show/set default model |
| `dbp config folder` | Show/set models folder |
| `dbp config run-hook` | Show/set run hook |
| `dbp config info` | Inspect lock file state for a model |

---

## Issues Found

### 1. `dbp status` vs `dbp config info` — Heavy Redundancy

Both commands display nearly identical information:
- Agency, dataset_id, model_root, DuckDB path
- Schema status (defined, column count)
- Input count/list
- Version count/history

`dbp config info` adds: default model, schema source, `--inputs` detail table, `--history` detail table, `--raw` dump.
`dbp status` adds: project path display, multi-model summary (iterates all models).

**Problem**: A user running `dbp status` and then `dbp config info` sees mostly the same output. The distinction ("project state" vs "lock file state") is unclear.

### 2. `dbp execute` — Name Too Long

At 7 characters, `execute` is verbose for the most frequently typed command during development. Every competing tool uses a shorter name:
- dbt: `dbt run`
- make: `make`
- psql: `\i`

### 3. `dbp init` Does Too Many Things

`dbp init` has three distinct behaviors:
1. No args → sync all models (parallel)
2. Existing model key → sync one model
3. New name → scaffold new model

The "sync" behavior is unintuitive under `init`. Users expect `init` to create something, not refresh existing state. The sync operation is really a "fetch latest catalog state" action.

### 4. `dbp config` Subcommands — Fragmented

Four subcommands (`default`, `folder`, `run-hook`, `info`) under `config` feel scattered:
- `config info` overlaps with `status`
- `config default/folder/run-hook` are getter/setters that could use a simpler `key=value` pattern

### 5. `dbp check` — Niche, Rarely Used

Health checks (lockfile valid, DuckDB works, deps installed) are mainly useful in CI or after initial setup. Not a daily command, but occupies top-level namespace.

### 6. `--show-history` on `status` vs `--history` on `config info`

Two different flags on two different commands that show the same version history table. Pure duplication.

### 7. Unused Options on `publish`

`--message` / `-m` and `--yes` / `-y` and `--strict` are accepted but `--message` is only displayed in output (never persisted), `--yes` never triggers a confirmation prompt, and `--strict` does nothing in the publish flow.

---

## Proposed Changes

### A. Merge `status` and `config info` → unified `dbp status`

Combine into a single `dbp status` that:
- Shows project + model summary (current behavior of both)
- `--inputs` flag for detailed input table (from `config info`)
- `--history` flag for version history table (from `config info`)
- `--raw` flag for raw TOML dump (from `config info`)
- Remove `--show-history` (replaced by `--history`)

Delete `config info` subcommand entirely.

### B. Add `dbp sql` as alias for `dbp execute`

Register `execute` under the shorter name `sql`:
```
dbp sql staging.sql          # short form
dbp sql sql/main.sql --timing
```

Keep `execute` as a hidden alias for backwards compatibility.

### C. Split `dbp init` — remove sync behavior

- `dbp init` → only scaffolds new models (its intuitive purpose)
- `dbp sync [MODEL]` → new command for syncing existing models from lock/catalog

This makes both commands do exactly one thing.

### D. Flatten `dbp config` to `dbp config KEY [VALUE]`

Replace the subcommands with a single `get/set` pattern:
```
dbp config default                    # get
dbp config default wifor.employment   # set
dbp config folder                     # get
dbp config folder models              # set
dbp config run-hook                   # get
dbp config run-hook sql/main.sql      # set
```

This is a single command with KEY as required arg and VALUE as optional. Simpler mental model, fewer subcommands.

### E. Remove dead options from `publish`

- Remove `--message` / `-m` (not persisted anywhere)
- Remove `--yes` / `-y` (no confirmation prompt exists)
- Remove `--strict` (no warning checks in publish flow)

### F. Move `dbp check` under `dbp config check`

Health checks are a configuration/setup concern, not a daily workflow command. Moving it under `config` declutters the top-level namespace while keeping it accessible.

---

## Summary of Changes

| Before | After | Type |
|---|---|---|
| `dbp status` + `dbp config info` | `dbp status` (merged, with `--inputs`, `--history`, `--raw`) | Merge |
| `dbp execute` | `dbp sql` (primary), `dbp execute` (hidden alias) | Rename |
| `dbp init` (scaffold + sync) | `dbp init` (scaffold only) + `dbp sync` (new) | Split |
| `dbp config default/folder/run-hook/info` | `dbp config KEY [VALUE]` + `dbp config check` | Flatten |
| `dbp check` | `dbp config check` | Move |
| `dbp publish --message/--yes/--strict` | Remove dead flags | Cleanup |

### Final command tree after changes:

```
dbp init NAME                     # scaffold new model
dbp sync [MODEL]                  # sync model(s) from catalog
dbp status                        # show project state (--inputs, --history, --raw)
dbp schema [SOURCE]               # show/apply schema (unchanged)
dbp load [DATASET]                # load inputs (unchanged)
dbp sql TARGET                    # execute SQL (renamed from execute)
dbp run [MODEL]                   # full workflow (unchanged)
dbp publish [MODEL]               # publish output (cleaned up flags)
dbp config KEY [VALUE]            # get/set config values
dbp config check                  # verify project health
```

10 top-level entries → 10 top-level entries, but clearer, shorter, no redundancy.
