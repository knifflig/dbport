# Release Versioning

This page documents the release numbering policy for the DBPort project.

---

## Numbering scheme

DBPort uses a three-part version number: **`X.Y.Z`**

| Position | Name | Meaning |
|---|---|---|
| `X` | Major | Breaking changes or major release step |
| `Y` | Normal | Feature additions or normal release step |
| `Z` | Minor | Fixes, documentation, or small improvements |

Examples:

- `0.0.1` → `0.0.2`: minor release step (docs, polish, small changes)
- `0.0.9` → `0.1.0`: normal release step (first public release)
- `0.1.0` → `1.0.0`: major release step (would signal a breaking or significant milestone)

---

## Initial release path

Development starts at `0.0.1` and proceeds through ten predevelopment work packages:

| Version | Milestone |
|---|---|
| `0.0.1` | Foundation: versioning mechanism online |
| `0.0.2` | Release history and roadmap |
| `0.0.3` | Version policy and release planning language |
| `0.0.4` | Python API reference correctness |
| `0.0.5` | CLI reference and executable workflows |
| `0.0.6` | Public package surface and repository trustworthiness |
| `0.0.7` | Execution model and conceptual docs depth |
| `0.0.8` | Zensical navigation model |
| `0.0.9` | Homepage UX and publication-facing polish |
| **`0.1.0`** | **First public release** (PyPI + GitHub Pages) |

The `0.0.x` series is predevelopment. No stability guarantees apply until `0.1.0`.

**`0.1.0`** is the first version published to PyPI and GitHub Pages. From that point forward, the versioning policy applies to all public releases.

---

## Source of truth

The **single source of truth** for the package version is the `version` field in `pyproject.toml`:

```toml
[project]
version = "0.0.3"
```

All version-bearing surfaces derive from this value:

| Surface | How it reads the version |
|---|---|
| `dbp --version` | `importlib.metadata.version("dbport")` (reads installed package metadata, which comes from `pyproject.toml`) |
| Docs deployment | Extracts version from `pyproject.toml` directly; validates against git tag |
| Git tags | Must match `v{version}` (e.g., `v0.1.0` for version `0.1.0`) |
| Release workflow | Fails if the git tag does not match `pyproject.toml` |

No hard-coded version strings exist in source code. If `importlib.metadata` cannot resolve the version (e.g., broken install), the CLI reports `"unknown"` rather than guessing.

---

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

---

## Release checklist (per version)

1. Update `version` in `pyproject.toml`
2. Add a changelog entry in `docs/changelog.md`
3. Update `docs/roadmap.md` status
4. Tag the commit as `v{version}`
5. Push the tag to trigger the docs deployment workflow
