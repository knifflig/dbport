#!/usr/bin/env bash
# Minimal CLI example — demonstrates the full `dbp` command surface.
#
# Covers: init, config (default, schema, input, meta, attach),
# status check, model exec/publish/run, project sync, and status.
#
# Uses CWD-based model resolution — no --model flag needed.
#
# Prerequisites:
#   - Credentials set in the environment (ICEBERG_REST_URI, etc.)
#
# Usage (from repo root):
#   bash examples/minimal_cli/run.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

VERSION="$(date +%Y-%m-%d)"

echo "=== 1. Init: register model in lock file ==="
dbp init minimal_cli --agency test --dataset cli_table1 --path examples/minimal_cli --force

echo ""
echo "=== 2. Config: set defaults ==="
dbp config default model test.cli_table1
dbp config default hook main.py
dbp config model test.cli_table1 version "$VERSION"
dbp status

echo ""
echo "=== 2b. Project-wide sync ==="
dbp project sync

echo ""
echo "=== 3. Check project health ==="
dbp status check

# cd into model directory — all subsequent commands resolve model from CWD
cd examples/minimal_cli

echo ""
echo "=== 4. Apply the output schema ==="
dbp config model test.cli_table1 schema sql/create_output.sql

echo ""
echo "=== 5. Column metadata ==="
dbp config model test.cli_table1 columns set geo --id GEO --kind reference
dbp config model test.cli_table1 columns set year --type categorical
dbp config model test.cli_table1 columns               # show all column metadata

echo ""
echo "=== 6. Load inputs ==="
dbp config model test.cli_table1 input estat.nama_10r_3empers
dbp config model test.cli_table1 input wifor.cl_nuts2024
dbp model load test.cli_table1

echo ""
echo "=== 7. Attach codelist table to column ==="
dbp config model test.cli_table1 columns attach geo wifor.cl_nuts2024

echo ""
echo "=== 8. Execute multi-step transforms ==="
dbp model exec test.cli_table1 --target sql/staging.sql --timing
dbp model exec test.cli_table1 --target sql/transform.sql --timing

echo ""
echo "=== 9. Publish: dry-run ==="
dbp model publish test.cli_table1 --version "$VERSION" --dry-run

echo ""
echo "=== 10. Publish: normal ==="
dbp model publish test.cli_table1 --version "$VERSION"

echo ""
echo "=== 11. Publish: refresh (overwrite) ==="
dbp model publish test.cli_table1 --version "$VERSION" --refresh

echo ""
echo "=== 12. Run (hook-based workflow, auto-publishes with configured version) ==="
dbp model run test.cli_table1 --timing --refresh

echo ""
echo "=== 13. Status ==="
dbp status
dbp status --show-history

echo ""
echo "=== 14. JSON output mode ==="
dbp status --json

echo ""
echo "=== Done ==="
