#!/usr/bin/env bash
# Build and serve a real versioned docs preview locally.
#
# Usage:
#   ./scripts/preview_docs.sh          # build + serve
#   ./scripts/preview_docs.sh --serve  # serve only (skip build)
#
# This produces the same versioned directory structure used in deployment:
#
#   _preview/
#     index.html          → redirect to latest/
#     versions.json       → mike-compatible version metadata
#     <version>/          → full built site
#     latest/             → full built site (copy)
#
# The version selector reads versions.json and navigates between real
# versioned paths. This matches what GitHub Pages serves in production.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

PREVIEW_DIR="$PROJECT_ROOT/_preview"

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

echo "Assembling versioned preview for $VERSION..."

# Clean previous preview
rm -rf "$PREVIEW_DIR"
mkdir -p "$PREVIEW_DIR"

# Deploy built site to versioned subdirectory
cp -r site "$PREVIEW_DIR/$VERSION"

# Create latest alias (full copy, matching GitHub Pages deployment)
cp -r site "$PREVIEW_DIR/latest"

# Write mike-compatible versions.json
cat > "$PREVIEW_DIR/versions.json" <<EOF
[
  {
    "version": "$VERSION",
    "title": "$VERSION",
    "aliases": ["latest"]
  }
]
EOF

# Root redirect to latest
cat > "$PREVIEW_DIR/index.html" <<'REDIRECT'
<!DOCTYPE html>
<html>
  <head><meta http-equiv="refresh" content="0; url=latest/"></head>
  <body><a href="latest/">Redirecting to latest docs...</a></body>
</html>
REDIRECT

echo "Serving versioned docs at http://localhost:8000"
echo "  /$VERSION/  — versioned path"
echo "  /latest/    — latest alias"
echo "  /           — redirects to /latest/"
cd "$PREVIEW_DIR" && python3 -m http.server 8000
