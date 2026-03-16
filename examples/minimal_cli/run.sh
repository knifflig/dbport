#!/usr/bin/env bash
# Minimal CLI example — demonstrates the full `dbp` command surface.
#
# Covers: init, config (default, run-hook, meta, attach, info),
# check, schema, load, execute, publish (dry/normal/refresh),
# run (hook-based), and status.
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
dbp config default test.cli_table1
dbp config run-hook sql/main.sql
dbp config version "$VERSION"
dbp config info

echo ""
echo "=== 3. Check project health ==="
dbp check

# cd into model directory — all subsequent commands resolve model from CWD
cd examples/minimal_cli

echo ""
echo "=== 4. Apply the output schema ==="
dbp schema sql/create_output.sql

echo ""
echo "=== 5. Column metadata ==="
dbp config meta geo --id GEO --kind reference
dbp config meta year --type categorical
dbp config meta                  # show all column metadata

echo ""
echo "=== 6. Load inputs ==="
dbp load estat.nama_10r_3empers
dbp load wifor.cl_nuts2024

echo ""
echo "=== 7. Attach codelist table to column ==="
dbp config attach geo --table wifor.cl_nuts2024

echo ""
echo "=== 8. Execute multi-step transforms ==="
dbp execute sql/staging.sql --timing
dbp execute sql/transform.sql --timing

echo ""
echo "=== 9. Publish: dry-run ==="
dbp publish --version "$VERSION" --dry-run --yes

echo ""
echo "=== 10. Publish: normal ==="
dbp publish --version "$VERSION" --yes

echo ""
echo "=== 11. Publish: refresh (overwrite) ==="
dbp publish --version "$VERSION" --refresh --yes

echo ""
echo "=== 12. Run (hook-based workflow, auto-publishes with configured version) ==="
dbp run --timing --refresh

echo ""
echo "=== 13. Status ==="
dbp status
dbp status --show-history

echo ""
echo "=== 14. JSON output mode ==="
dbp status --json

echo ""
echo "=== Done ==="
