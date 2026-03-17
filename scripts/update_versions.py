#!/usr/bin/env python3
"""Update versions.json for the documentation version selector.

Usage:
    python scripts/update_versions.py <version>

Reads versions.json from the current directory (if it exists), adds or updates
the given version, marks it as "latest", and writes the result back.

The output format is compatible with Material for MkDocs / Zensical's
version selector (extra.version.provider = "mike").
"""

import json
import sys
from pathlib import Path


def update_versions(version: str) -> None:
    path = Path("versions.json")

    if path.exists():
        versions = json.loads(path.read_text())
    else:
        versions = []

    # Remove "latest" alias from all existing entries
    for entry in versions:
        entry["aliases"] = [a for a in entry.get("aliases", []) if a != "latest"]

    # Remove existing entry for this version (if re-deploying)
    versions = [v for v in versions if v["version"] != version]

    # Add the new version at the front with "latest" alias
    versions.insert(0, {
        "version": version,
        "title": version,
        "aliases": ["latest"],
    })

    path.write_text(json.dumps(versions, indent=2) + "\n")
    print(f"Updated versions.json: {version} (latest)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <version>", file=sys.stderr)
        sys.exit(1)
    update_versions(sys.argv[1])
