# Zensical Documentation Setup — Implementation Plan

## Overview

Set up Zensical documentation for dbport with versioned GitHub Pages deployment on release tags and a version selector for browsing different releases.

---

## 1. Project Setup

### 1.1 Add Zensical dependency

Add `zensical` to dev dependencies in `pyproject.toml`.

Run `uv sync` to install.

### 1.2 Create `zensical.toml`

Native TOML configuration with:
- Site metadata (name, URL, description)
- Modern theme variant with navigation features (tabs, sections, instant, sticky tabs, back-to-top)
- Light/dark mode toggle
- GitHub repo link
- Version selector via `extra.version.provider = "mike"` (reads `versions.json` — we generate it ourselves, mike CLI is not used)
- Full navigation tree

---

## 2. Documentation Pages

All files under `docs/`. Content adapted from existing `client.md`, `dbport.md`, `dbport_cli.md`, `README.md`, and `examples/`.

### Navigation structure

```
Home                              → docs/index.md
Getting Started/
  Overview                        → docs/getting-started/index.md
  Installation                    → docs/getting-started/installation.md
  Credentials                     → docs/getting-started/credentials.md
  Quickstart                      → docs/getting-started/quickstart.md
Concepts/
  Overview                        → docs/concepts/index.md
  Inputs & Loading                → docs/concepts/inputs.md
  Outputs & Schemas               → docs/concepts/outputs.md
  Metadata & Codelists            → docs/concepts/metadata.md
  Lock File                       → docs/concepts/lock-file.md
  Versioning & Publish            → docs/concepts/versioning.md
API Reference/
  Overview                        → docs/api/index.md
  Python API                      → docs/api/python.md  (from client.md)
  CLI Reference                   → docs/api/cli.md     (from dbport_cli.md)
Examples/
  Overview                        → docs/examples/index.md
  Python Workflow                  → docs/examples/python-workflow.md
  CLI Workflow                     → docs/examples/cli-workflow.md
```

### Existing docs disposition
- `docs/client.md` → content migrated to `docs/api/python.md` + getting-started; **removed**
- `docs/dbport_cli.md` → content migrated to `docs/api/cli.md`; **removed**
- `docs/dbport.md` → internal product strategy doc; stays in place, **not in nav**

---

## 3. Versioned GitHub Pages Deployment

### 3.1 GitHub Actions Workflow: `.github/workflows/docs.yml`

Triggered on push of `v*` tags only.

Steps:
1. Checkout the tagged commit
2. Install uv + dependencies
3. Extract version from tag (`v0.1.0` → `0.1.0`)
4. Build docs with `uv run zensical build --clean`
5. Checkout or create `gh-pages` branch
6. Copy built site to `<version>/` subdirectory
7. Copy same build to `latest/` directory
8. Run `scripts/update_versions.py` to update `versions.json`
9. Write root `index.html` that redirects to `latest/`
10. Commit and push to `gh-pages`

### 3.2 Version management script: `scripts/update_versions.py`

Small Python script that:
1. Reads existing `versions.json` (or creates empty list)
2. Adds new version entry in mike-compatible format:
   ```json
   [
     {"version": "0.2.0", "title": "0.2.0", "aliases": ["latest"]},
     {"version": "0.1.0", "title": "0.1.0", "aliases": []}
   ]
   ```
3. Moves `"latest"` alias to newest version
4. Sorts versions descending (newest first)
5. Writes updated `versions.json`

The theme's version selector reads this JSON and renders the dropdown.

---

## 4. Files Summary

### New files (22)

| File | Purpose |
|------|---------|
| `zensical.toml` | Zensical configuration |
| `docs/index.md` | Landing page |
| `docs/getting-started/index.md` | Getting started overview |
| `docs/getting-started/installation.md` | Installation guide |
| `docs/getting-started/credentials.md` | Credentials setup |
| `docs/getting-started/quickstart.md` | Quickstart tutorial |
| `docs/concepts/index.md` | Concepts overview |
| `docs/concepts/inputs.md` | Inputs & loading |
| `docs/concepts/outputs.md` | Outputs & schemas |
| `docs/concepts/metadata.md` | Metadata & codelists |
| `docs/concepts/lock-file.md` | Lock file |
| `docs/concepts/versioning.md` | Versioning & publish |
| `docs/api/index.md` | API reference overview |
| `docs/api/python.md` | Python API reference |
| `docs/api/cli.md` | CLI reference |
| `docs/examples/index.md` | Examples overview |
| `docs/examples/python-workflow.md` | Python example |
| `docs/examples/cli-workflow.md` | CLI example |
| `.github/workflows/docs.yml` | Docs deployment workflow |
| `scripts/update_versions.py` | Version JSON updater |

### Modified files (1)

| File | Change |
|------|--------|
| `pyproject.toml` | Add `zensical` to dev deps |

### Removed files (2)

| File | Reason |
|------|--------|
| `docs/client.md` | Content migrated to new structure |
| `docs/dbport_cli.md` | Content migrated to new structure |

---

## 5. Implementation Order

1. Add `zensical` dependency + `uv sync`
2. Create `zensical.toml`
3. Create all 18 documentation pages
4. Create `scripts/update_versions.py`
5. Create `.github/workflows/docs.yml`
6. Remove migrated source files
7. Verify build with `uv run zensical build --clean`
