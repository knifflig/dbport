# Roadmap

This roadmap tracks the planned work from predevelopment (`0.0.1`) through the first public release (`0.1.0`). Each milestone is a work package with a clear theme.

For completed work, see the [Changelog](changelog.md).

---

## Milestone overview

| Version | Theme | Status |
|---|---|---|
| 0.0.1 | Foundation: runtime, architecture, CLI, docs site, CI | Done |
| 0.0.2 | Release history and roadmap foundations | Done |
| 0.0.3 | Version policy and release planning language | Done |
| 0.0.4 | Python API reference correctness | Done |
| 0.0.5 | CLI reference and executable workflows | Done |
| 0.0.6 | Public package surface and repository trustworthiness | Done |
| 0.0.7 | Execution model and conceptual docs depth | Planned |
| 0.0.8 | Zensical navigation model | Planned |
| 0.0.9 | Homepage UX and publication-facing polish | Planned |
| 0.1.0 | First public release (PyPI + GitHub Pages) | Planned |

---

## 0.0.3 — Version policy and release planning language

- ~~Document and enforce the project's release numbering policy~~
- ~~Encode the initial release milestones (`0.0.1` → `0.1.0`) in docs and release workflow~~
- ~~Make runtime and CLI version reporting derive from `pyproject.toml` consistently~~

## 0.0.4 — Python API reference correctness

- ~~Complete the Python API reference to match the full `DBPort` surface~~
- ~~Freeze the supported Python client contract for `0.1.0`~~
- ~~Make `DBPort(...)` startup semantics explicit and stable~~

## 0.0.5 — CLI reference and executable workflows

- ~~Rebuild CLI reference around the real command hierarchy (`status`, `config`, `model`)~~
- ~~Fix broken CLI example script~~
- ~~Remove stale command references from workflow pages~~
- ~~Freeze the CLI command taxonomy, model selection, and error behavior~~

## 0.0.6 — Public package surface and repository trustworthiness

- ~~Clean up README drift~~
- ~~Make docs sections (API, concepts, examples) serve distinct roles~~
- ~~Lock down the public Python package surface~~
- ~~Complete PyPI-facing package metadata~~
- ~~Decide policy for generated docs artifacts in the repository~~
- ~~Add baseline governance files (`CONTRIBUTING.md`, `SECURITY.md`)~~

## 0.0.7 — Execution model and conceptual docs depth

- Turn the lock file page into an operator guide with annotated examples and recovery guidance
- Add dedicated documentation for hook-based workflows
- Strengthen section index pages
- Tighten hook execution and publish safety semantics

## 0.0.8 — Zensical navigation model

- Restore the left sidebar on the homepage
- Expand left navigation by default
- Keep the right sidebar TOC separate from left nav

## 0.0.9 — Homepage UX and publication-facing polish

- Apply stronger Zensical teaching patterns to core concept pages
- Redesign the homepage card grid for balanced layout
- Improve card interaction using supported patterns
- Make the homepage more documentation-first
- Review homepage TOC behavior

## 0.1.0 — First public release

- Publish the package to PyPI
- Publish the docs to GitHub Pages
- Tie release publishing, docs versioning, and changelog into one release checklist
- Make credential resolution deterministic across environments
- Verify the built package artifact before publishing
- Make CI enforce the declared quality bar
- Promote runnable examples to release-gated smoke tests
