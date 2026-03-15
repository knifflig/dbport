#!/usr/bin/env bash
# Minimal CLI example — equivalent to examples/minimal/main.py but
# driven entirely through the `dbp` command line.
#
# This script runs from the repo root and addresses the model by its path.
# You can also cd into the model directory and run without --model.
#
# Prerequisites:
#   - Credentials set in the environment (ICEBERG_REST_URI, etc.)
#   - DuckDB extensions installed (run .claude/setup.sh from repo root)
#
# Usage (from repo root):
#   bash examples/minimal_cli/run.sh
#
# Or step-by-step from the model directory:
#   cd examples/minimal_cli
#   dbp init --agency test --dataset cli_table1 --path . --force
#   dbp schema sql/create_output.sql
#   dbp load estat.nama_10r_3empers
#   dbp run sql/transform.sql
#   dbp publish --version "2026-03-15" --yes
set -euo pipefail

# Run from the repo root
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

MODEL=examples/minimal_cli

echo "=== Step 1: Register model in repo-root lock file ==="
dbp init minimal_cli --agency test --dataset cli_table1 --path "$MODEL" --force

echo ""
echo "=== Step 2: Apply the output schema ==="
dbp --model "$MODEL" schema sql/create_output.sql

echo ""
echo "=== Step 3: Load input data ==="
dbp --model "$MODEL" load estat.nama_10r_3empers

echo ""
echo "=== Step 4: Run the transform ==="
dbp --model "$MODEL" run sql/transform.sql --timing

echo ""
echo "=== Step 5: Publish ==="
dbp --model "$MODEL" publish --version "$(date +%Y-%m-%d)" --refresh --yes

echo ""
echo "=== Step 6: Status ==="
dbp status --show-history

echo ""
echo "=== Done ==="
