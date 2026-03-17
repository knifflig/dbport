# Implementation Plan: Work Package 0.0.6

**Theme:** Public package surface, repository trustworthiness, and docs trustworthiness.

**Version:** 0.0.5 → 0.0.6

---

## Overview

Six backlog items, plus version bump and changelog/roadmap updates. Items 1–6 are independent and can be implemented in any order. The version bump is last.

---

## Item 1: DBP-DOC-005 — Clean up README drift (P0)

**File:** `README.md`

### Changes

1. **Requirements section (line 37):** Replace "DuckDB `iceberg` extension (pre-installed by `setup.sh`)" with "DuckDB `iceberg` extension (installed automatically at runtime)". The `.claude/setup.sh` file does not exist; extensions are installed via HTTPS at runtime.

2. **Installation section (lines 48–52):** Remove the "For development" block referencing `bash .claude/setup.sh`. Replace with:
   ```
   For development:
   ```bash
   uv sync
   ```
   ```

3. **API Overview section (lines 73–123):** Remove the entire duplicated API overview (methods schema, load, columns, execute, publish with code examples). Replace with a short paragraph linking to the docs site:
   ```markdown
   ## Documentation

   Full API reference and guides: [knifflig.github.io/dbport](https://knifflig.github.io/dbport)

   - [Python API](https://knifflig.github.io/dbport/latest/api/python/) — `DBPort` class reference
   - [CLI Reference](https://knifflig.github.io/dbport/latest/api/cli/) — `dbp` command reference
   - [Getting Started](https://knifflig.github.io/dbport/latest/getting-started/) — installation, credentials, quickstart
   ```

4. **Lock file section (lines 127–136):** Keep as-is (concise, accurate).

5. **Project Structure section (lines 139–155):**
   - Line 151: Change `regional_trends/       # full WiFOR employment model` to `minimal_cli/          # CLI-driven workflow`
   - Line 154: Change `docs/client.md           # user guide` to `docs/                # documentation source`

6. **Documentation section (lines 159–161):** Remove this section entirely — it's superseded by the new Documentation section added in step 3.

---

## Item 2: DBP-IA-001 — Make section index pages serve distinct roles (P1)

### `docs/api/index.md`

Add a framing sentence after the title and a "See also" section at the bottom:

```markdown
# API Reference

Complete specification of every parameter, method, and command.

[... existing content ...]

---

See also: [Getting Started](../getting-started/index.md) for first-time setup · [Examples](../examples/index.md) for runnable workflows
```

### `docs/concepts/index.md`

Add framing sentence after the title and "See also" at bottom:

```markdown
# Concepts

Understand the design decisions behind DBPort before diving into the API.

[... existing content ...]

---

See also: [API Reference](../api/index.md) for exact signatures · [Examples](../examples/index.md) for applied usage
```

### `docs/examples/index.md`

Expand the thin page with framing and cross-links:

```markdown
# Examples

Copy-paste-ready workflows that demonstrate DBPort end to end.

The **Python workflow** shows programmatic control with the `DBPort` class. The **CLI workflow** shows the same pipeline driven entirely from the terminal.

- **[Python Workflow](python-workflow.md)** — full Python API example: load, transform, publish
- **[CLI Workflow](cli-workflow.md)** — full CLI example: init, configure, run, publish

---

See also: [Getting Started](../getting-started/index.md) for setup prerequisites · [API Reference](../api/index.md) for parameter details
```

### `docs/getting-started/index.md`

Add "See also" after the card grid:

```markdown
[... existing card grid ...]

---

See also: [Examples](../examples/index.md) for complete workflows · [Concepts](../concepts/index.md) for understanding how things work
```

---

## Item 3: DBP-PKG-001 — Lock down public Python package surface (P1)

### Decision: `__version__` is NOT part of the public contract

The CLI already uses `importlib.metadata.version("dbport")`. Adding `dbport.__version__` would create a second source of truth. This absence is now a deliberate, tested decision.

### `tests/test_dbport/adapters/primary/test_contract.py`

Add 3 tests to the `TestModuleExports` class:

```python
def test_no_version_attribute(self):
    """__version__ is deliberately absent — use importlib.metadata instead."""
    import dbport
    assert not hasattr(dbport, "__version__")

def test_version_via_importlib(self):
    """The approved way to get the version."""
    from importlib.metadata import version
    v = version("dbport")
    assert isinstance(v, str)
    assert re.match(r"\d+\.\d+\.\d+", v)

def test_no_internal_symbols_leak(self):
    """Only DBPort should be accessible as a public name."""
    import dbport
    public = [name for name in dir(dbport) if not name.startswith("_")]
    assert public == ["DBPort"]
```

(Add `import re` to the imports at the top of the file.)

### `src/dbport/__init__.py`

No changes. Current single-export is correct.

---

## Item 4: DBP-PKG-002 — Complete PyPI-facing metadata (P1)

### `pyproject.toml`

Add these fields to the `[project]` section (after `requires-python`, before `dependencies`):

```toml
license = "Apache-2.0"
authors = [
    { name = "Henry Zehe", email = "knifflig.utopisch-0u@icloud.com" },
]
keywords = ["duckdb", "iceberg", "data-warehouse", "etl", "data-pipeline"]
classifiers = [
    "Development Status :: 2 - Pre-Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Topic :: Database",
    "Topic :: Scientific/Engineering",
    "Typing :: Typed",
]
```

Add a new `[project.urls]` section (after `[project.scripts]`):

```toml
[project.urls]
Homepage = "https://github.com/knifflig/dbport"
Documentation = "https://knifflig.github.io/dbport"
Repository = "https://github.com/knifflig/dbport"
Changelog = "https://knifflig.github.io/dbport/latest/changelog/"
```

---

## Item 5: DBP-REL-004 — Decide docs artifact policy (P1)

### `docs/release-versioning.md`

Add a new section after "Source of truth" (before "Release checklist"):

```markdown
## Docs artifact policy

Generated documentation (the `site/` directory) is **never committed to `main`**. It belongs exclusively on the `gh-pages` branch, deployed automatically by the docs workflow on tag push.

| Path | Branch | Purpose |
|---|---|---|
| `site/` | `gh-pages` only | Built docs (deployed by CI) |
| `_preview/` | never committed | Local preview (git-ignored) |
| `docs/` | `main` | Source markdown (committed) |

Enforcement:

- `.gitignore` excludes `/site` and `/_preview`
- CI (`ci.yml`) verifies the docs build on every push and PR but does not deploy
- The docs workflow (`docs.yml`) builds and deploys to `gh-pages` only on tag push
- The `gh-pages` branch is not merged back to `main`
```

---

## Item 6: DBP-REPO-001 — Add governance files (P1)

### `CONTRIBUTING.md` (new file)

Concise contributing guide (~60 lines) covering:
- Prerequisites (Python 3.11–3.12, uv)
- Development setup (`uv sync`)
- Running tests (`uv run pytest`)
- Code style (ruff, enforced by CI)
- Architecture pointer (hexagonal, single-import rule)
- Pull request expectations (one topic per PR, tests required)
- License (Apache 2.0)

### `SECURITY.md` (new file)

Minimal security policy (~30 lines) covering:
- Supported versions (latest 0.0.x)
- How to report vulnerabilities (GitHub Security Advisories)
- Response timeline (48h acknowledgment)
- Scope (dbport package only)

### `CODE_OF_CONDUCT.md` (new file)

Contributor Covenant v2.1. Standard text with enforcement contact pointing to repository maintainers via GitHub.

---

## Final Step: Version bump + changelog + roadmap

### `pyproject.toml`

`version = "0.0.5"` → `version = "0.0.6"`

### `docs/changelog.md`

New entry at the top (before the 0.0.5 entry):

```markdown
## 0.0.6 — 2026-03-17

Public package surface, repository trustworthiness, and docs trustworthiness.

### Added

- **Governance files** — `CONTRIBUTING.md`, `SECURITY.md`, and `CODE_OF_CONDUCT.md` at the repository root
- **PyPI metadata** — `pyproject.toml` now declares license, authors, keywords, classifiers, and project URLs
- **Docs artifact policy** — generated docs belong on `gh-pages` only; policy documented in release-versioning.md
- **Package surface tests** — contract tests verify no `__version__` attribute, `importlib.metadata` access, and no internal symbol leakage

### Fixed

- **README drift** — removed stale `.claude/setup.sh` references, fixed example directory paths, corrected docs links
- **README duplication** — replaced duplicated API overview with links to the docs site

### Improved

- **Section index pages** — each docs section now states its purpose and cross-links to related sections
```

### `docs/roadmap.md`

- Milestone table: change 0.0.6 status from `Planned` to `Done`
- Detail section: strike through all 0.0.6 items

---

## Verification

After all changes:

```bash
uv run pytest           # all existing tests + 3 new contract tests must pass
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```
