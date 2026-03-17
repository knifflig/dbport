# CLI Workflow

A complete CLI example demonstrating the full `dbp` command surface.

## Prerequisites

- DBPort installed (`pip install dbport`)
- Credentials set in the environment (`ICEBERG_REST_URI`, `ICEBERG_CATALOG_TOKEN`, `ICEBERG_WAREHOUSE`)

## Step-by-step

### 1. Initialize a model

```bash
dbp init minimal_cli --agency test --dataset cli_table1 --path examples/minimal_cli --force
```

This creates a scaffold directory and registers the model in `dbport.lock`.

### 2. Configure defaults

```bash
dbp config default test.cli_table1
dbp status
```

### 3. Check project health

```bash
dbp config check
```

Verifies lockfile, DuckDB, credentials, and dependencies.

### 4. Apply the output schema

```bash
cd examples/minimal_cli
dbp config model test.cli_table1 schema sql/create_output.sql
```

### 5. Configure column metadata

```bash
dbp config model test.cli_table1 columns set geo --id GEO --kind reference
dbp config model test.cli_table1 columns set year --type categorical
dbp config model test.cli_table1 columns    # show all columns
```

### 6. Configure and load inputs

```bash
dbp config model test.cli_table1 input estat.nama_10r_3empers
dbp config model test.cli_table1 input wifor.cl_nuts2024
dbp model load test.cli_table1
```

### 7. Attach codelist table

```bash
dbp config model test.cli_table1 columns attach geo wifor.cl_nuts2024
```

### 8. Execute transforms

```bash
dbp model exec test.cli_table1 --target sql/staging.sql --timing
dbp model exec test.cli_table1 --target sql/transform.sql --timing
```

### 9. Publish

```bash
# Dry run — validate schemas only
dbp model publish test.cli_table1 --version 2026-03-16 --dry-run

# Normal publish
dbp model publish test.cli_table1 --version 2026-03-16

# Refresh (overwrite)
dbp model publish test.cli_table1 --version 2026-03-16 --refresh
```

### 10. Full lifecycle in one command

```bash
dbp model run test.cli_table1 --timing --refresh
```

This runs sync, execute, and publish in sequence.

### 11. Inspect state

```bash
dbp status
dbp status --history
dbp status --json
```

## Key points

- **CWD-based resolution** — after `cd`-ing into the model directory, commands auto-resolve the model
- **Configuration through commands** — no manual file editing needed
- **`dbp model run`** — combines sync, execute, and publish into a single operation
- **`--json`** — every command supports structured JSON output for automation
