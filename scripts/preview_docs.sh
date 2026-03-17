#!/usr/bin/env bash
# Local docs preview with a mock version selector.
#
# Usage:
#   ./scripts/preview_docs.sh          # build + serve
#   ./scripts/preview_docs.sh --serve  # serve only (skip build)
#
# This generates a local versions.json so the version selector renders
# during local preview. The mock versions.json is written into site/ after
# the build and is git-ignored.
#
# Limitation: only the current build is available locally. Switching
# versions in the selector will 404 — this is expected. The selector
# itself is visible so you can verify its appearance and behavior.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Extract version from pyproject.toml
VERSION=$(python3 -c "
import re, pathlib
text = pathlib.Path('pyproject.toml').read_text()
m = re.search(r'^version\s*=\s*\"([^\"]+)\"', text, re.MULTILINE)
print(m.group(1))
")

if [ "${1:-}" != "--serve" ]; then
    echo "Building docs..."
    uv run zensical build --clean
fi

# Write mock versions.json into the built site
cat > site/versions.json <<EOF
[
  {
    "version": "$VERSION",
    "title": "$VERSION (local preview)",
    "aliases": ["latest"]
  }
]
EOF

echo "Serving docs for version $VERSION at http://localhost:8000"
echo "Note: version selector is visible but switching versions will 404 locally."
cd site && python3 -m http.server 8000
