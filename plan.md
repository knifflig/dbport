# CLI Improvement Plan — Implemented

## Changes Made

### 1. `execute` → `exec` (with hidden `execute` alias)
- Renamed primary command from `execute` to `exec` for brevity
- `execute` kept as hidden alias for backwards compatibility
- Renamed `execute_cmd` → `exec_cmd` in source

### 2. Split `init` into `init` + `sync`
- `dbp init` now **only scaffolds** new models
- `dbp init` with no args errors out (suggests `dbp sync`)
- `dbp init` errors if model already exists in lock file (suggests `dbp sync` or `--force`)
- New `dbp sync [MODEL]` command handles syncing existing models
- Parallel multi-model sync preserved in `sync`

### 3. Merged `status` + `config info` → unified `status`
- `dbp status` now shows full model detail (project path, lockfile, default model, model info)
- Added `--inputs` flag for detailed input table
- Added `--history` flag for version history table
- Added `--raw` flag for raw TOML dump
- Removed redundant `--show-history` flag
- Deleted `config info` subcommand entirely

### 4. Flattened `config` subcommands → `config KEY [VALUE]`
- Replaced `config default`, `config folder`, `config run-hook`, `config info` subcommands
- New pattern: `dbp config KEY [VALUE]` (get if VALUE omitted, set if provided)
- Valid keys: `default`, `folder`, `run-hook`, `check`
- `dbp config check` replaces top-level `dbp check`

### 5. Removed dead flags from `publish`
- Removed `--message` / `-m` (was only displayed, never persisted)
- Removed `--yes` / `-y` (no confirmation prompt existed)
- Removed `--strict` (no warning checks in publish flow)

### 6. Moved `check` under `config`
- `dbp check` → `dbp config check`
- Health checks are a setup/configuration concern, not a daily workflow command
- Deleted standalone `check.py` command file

## Final Command Tree

```
dbp init NAME                     # scaffold new model
dbp sync [MODEL]                  # sync model(s) from catalog
dbp status                        # show project state (--inputs, --history, --raw)
dbp schema [SOURCE]               # show/apply schema
dbp load [DATASET]                # load inputs
dbp exec TARGET                   # execute transforms (alias: execute)
dbp run [MODEL]                   # full workflow
dbp publish [MODEL]               # publish output
dbp config KEY [VALUE]            # get/set config (keys: default, folder, run-hook, check)
```

## Test Results
- 813 tests passing (all CLI + domain + adapter + service tests)
- 287 CLI-specific tests covering all new commands and changes
