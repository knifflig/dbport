# Project Backlog

## Purpose

This backlog organizes the remaining repository work into release work packages.

It is the planning file for still-open backlog items across the project, including package behavior, CLI, runtime, documentation, release workflow, testing, and repository maintenance.

Implemented capabilities should only be mentioned here when they give useful context for the remaining work. This file is not meant to preserve a done-history.

Rules for this file:

- All work required before the first public release is grouped into exactly ten work packages:
  - `0.0.1`
  - `0.0.2`
  - `0.0.3`
  - `0.0.4`
  - `0.0.5`
  - `0.0.6`
  - `0.0.7`
  - `0.0.8`
  - `0.0.9`
  - `0.1.0`
- `0.0.1` includes bringing the versioning mechanism online
- `0.1.0` includes the publishing mechanism to PyPI and GitHub Pages
- Work that is out of scope until after `0.1.0` stays in an unordered backlog with priority only

---

## Conventions

### Priority levels

- `P0` Critical correctness issue or release blocker
- `P1` Important structural or UX improvement
- `P2` Valuable enhancement with lower urgency

### Status values

- `todo`
- `doing`
- `done`
- `blocked`

---

## Zensical Reference Links

Use these links when planning or implementing docs UX, theme, navigation, and authoring changes.

### Core setup and customization

- [Customization](https://zensical.org/docs/customization/)
- [Basics](https://zensical.org/docs/setup/basics/)
- [Colors](https://zensical.org/docs/setup/colors/)
- [Fonts](https://zensical.org/docs/setup/fonts/)
- [Logo and icons](https://zensical.org/docs/setup/logo-and-icons/)
- [Data privacy](https://zensical.org/docs/setup/data-privacy/)
- [Navigation](https://zensical.org/docs/setup/navigation/)
- [Tags](https://zensical.org/docs/setup/tags/)
- [Header](https://zensical.org/docs/setup/header/)
- [Footer](https://zensical.org/docs/setup/footer/)
- [Repository](https://zensical.org/docs/setup/repository/)
- [Comment system](https://zensical.org/docs/setup/comment-system/)
- [Offline usage](https://zensical.org/docs/setup/offline/)

### Markdown and extension setup

- [Python Markdown](https://zensical.org/docs/setup/extensions/python-markdown/)
- [Python Markdown Extensions](https://zensical.org/docs/setup/extensions/python-markdown-extensions/)
- [mkdocstrings](https://zensical.org/docs/setup/extensions/mkdocstrings/)

### Authoring patterns

- [Admonitions](https://zensical.org/docs/authoring/admonitions/)
- [Buttons](https://zensical.org/docs/authoring/buttons/)
- [Code blocks](https://zensical.org/docs/authoring/code-blocks/)
- [Content tabs](https://zensical.org/docs/authoring/content-tabs/)
- [Data tables](https://zensical.org/docs/authoring/data-tables/)
- [Diagrams](https://zensical.org/docs/authoring/diagrams/)
- [Footnotes](https://zensical.org/docs/authoring/footnotes/)
- [Formatting](https://zensical.org/docs/authoring/formatting/)
- [Grids](https://zensical.org/docs/authoring/grids/)
- [Icons and emojis](https://zensical.org/docs/authoring/icons-emojis/)
- [Images](https://zensical.org/docs/authoring/images/)
- [Lists](https://zensical.org/docs/authoring/lists/)
- [Tooltips](https://zensical.org/docs/authoring/tooltips/)

---

## Current Baseline

Use these facts as context when working the remaining backlog items.

- The docs site structure, Zensical config, and versioned docs deployment assets already exist.
- `uv run zensical build --clean` currently succeeds locally, so the remaining docs-build work is about enforcement, preview behavior, and release integration rather than basic site setup.
- The Python client already exposes advanced surface that now needs documentation and contract hardening, including `model_root`, `load_inputs_on_init`, `config_only`, `configure_input()`, `run()`, and `run_hook`.
- The CLI already has a nested command tree under `dbp status`, `dbp config`, and `dbp model`; the remaining work is to freeze semantics, remove drift in docs/examples, and harden behavior.
- The test suite is broadly healthy, but credential precedence is still not deterministic across environments and currently causes test failures when ambient credentials are present.

---

## Work Package 0.0.1

Theme: versioning mechanism online.

Status: `done`

### What was implemented

- **Real versioned docs structure** — both local preview and deployment produce the same mike-compatible layout: `/<version>/`, `/latest/`, root `versions.json`, and root `index.html` redirect to `/latest/`
- **pyproject.toml as version source of truth** — the docs deployment workflow extracts the version from `pyproject.toml` and validates it against the git tag; a mismatch fails the workflow
- **Fixed deployment ordering bug** — the original workflow ran `update_versions.py` on the `gh-pages` branch before the script existed there (it was checked out from the tag *after* execution); now both the built site and the script are saved before the branch switch
- **Fixed malformed root redirect** — the original heredoc produced indented HTML due to shell indentation; replaced with a properly formatted heredoc
- **Local preview with real versioned paths** — `scripts/preview_docs.sh` builds a versioned tree in `_preview/` (git-ignored) with real `/<version>/` and `/latest/` directories that the version selector can navigate between
- **Docs build verification in CI** — added a `docs` job to `.github/workflows/ci.yml` that runs `uv run zensical build --clean` on every push and PR to main
- **`scripts/update_versions.py` retained** — it manages multi-version history in `versions.json` during deployment (adding versions, moving the `latest` alias, handling re-deploys)

### Challenges encountered

- **mike cannot be used directly with Zensical** — mike's `deploy` command calls `mkdocs build` internally and expects `mkdocs.yml`; since Zensical uses its own build system and `zensical.toml`, there is no bridge; the versioned directory structure is mike-compatible but assembled manually
- **Zensical does not wire `extra.version` into the JS config** — the base template reads `config.extra.version` for the outdated-version banner but never assigns it to the `_.version` namespace variable that the `__config` JSON block uses; a template override (`overrides/main.html`) was created to fix this, but the resulting version selector rendered as broken unstyled text in the header and nav tabs because Zensical does not include the CSS for the mike version selector dropdown
- **Version selector UI removed** — the client-side version selector was removed entirely because Zensical cannot render it correctly; versioned browsing works through real directory paths and `versions.json`, not through the client-side widget; the selector can be re-evaluated when Zensical adds proper support

### Items delivered

| Item | Priority | Summary |
|---|---|---|
| DBP-VERS-001 | `P0` | Versioned docs structure works end-to-end for deployment; client-side selector deferred (Zensical limitation) |
| DBP-VERS-002 | `P0` | `pyproject.toml` is the version source of truth; tag validation enforced in workflow |
| DBP-VERS-007 | `P1` | Local preview builds a real versioned tree; documented in `CLAUDE.md` |
| DBP-FOUND-006 | `P1` | Docs build verification added to CI as a separate job |

---

## Work Package 0.0.2

Theme: release history and roadmap foundations.

Status: `done`

### What was implemented

- **Changelog page** — added `docs/changelog.md` with version-structured entries; the 0.0.1 entry comprehensively documents all foundational capabilities (core runtime, data operations, column metadata, write strategy, domain model, adapters, services, CLI, infrastructure, docs site, testing, CI/CD, and examples); the changelog convention requires every released version to have an entry with version number, date, and categorized changes
- **Roadmap page** — added `docs/roadmap.md` with a milestone overview table and per-version summaries covering the full repository scope (package, CLI, runtime, docs, release, testing); roadmap is kept distinct from changelog (planned future vs completed past)
- **Docs navigation** — both pages added as top-level nav items in `zensical.toml` (alongside Home, Getting Started, Concepts, API Reference, Examples)

### Items delivered

| Item | Priority | Summary |
|---|---|---|
| DBP-VERS-003 | `P0` | Changelog page added to docs site, structured around versions with categorized changes |
| DBP-VERS-004 | `P0` | Changelog convention established; minimum content defined (version, date, grouped changes) |
| DBP-VERS-008 | `P0` | Roadmap page added covering all repository work from 0.0.1 through 0.1.0 |

---

## Work Package 0.0.3

Theme: version policy and release planning language.

Status: `done`

### What was implemented

- **Release versioning policy** — added `docs/release-versioning.md` documenting the project's `X.Y.Z` numbering convention (major / normal / minor), the predevelopment milestone path from `0.0.1` to `0.1.0`, the single source of truth in `pyproject.toml`, and a per-version release checklist
- **CLI version fallback fixed** — removed the hard-coded `"0.1.0"` fallback in `dbp --version`; now reports `"unknown"` when `importlib.metadata` cannot resolve the package version
- **Version surfaces documented** — `pyproject.toml` is the declared single source of truth; `dbp --version`, docs deployment, git tags, and release automation all derive from it consistently

### Items delivered

| Item | Priority | Summary |
|---|---|---|
| DBP-VERS-005 | `P0` | Release numbering policy documented in `docs/release-versioning.md` |
| DBP-VERS-006 | `P1` | Initial release milestones (`0.0.1` → `0.1.0`) encoded in versioning docs |
| DBP-VERS-009 | `P1` | CLI version fallback fixed; all version surfaces derive from `pyproject.toml` |

---

## Work Package 0.0.4

Theme: Python API reference correctness.

Status: `done`

### DBP-DOC-001 - Complete Python API reference

- Priority: `P0`
- Status: `done`
- Files:
  - `docs/api/python.md`
- Required changes:
  - Document the already-implemented constructor parameters `model_root`, `load_inputs_on_init`, and `config_only`
  - Document the already-implemented `port.configure_input(...)`, `port.run(...)`, and `port.run_hook`
  - Explain full mode vs `config_only=True`
  - Clarify the difference between `load()` and `configure_input()`
  - Clarify how `run()` relates to `execute()` and publish flow
- Acceptance criteria:
  - The Python API reference matches the public `DBPort` surface

### DBP-API-001 - Freeze the supported Python client contract before public release

- Priority: `P0`
- Status: `done`
- Files:
  - `src/dbport/__init__.py`
  - `src/dbport/adapters/primary/client.py`
  - `docs/api/python.md`
  - Python client contract tests
- Required changes:
  - Freeze the current `DBPort` constructor options, methods, and properties intentionally instead of leaving them as accidental surface area
  - Explicitly confirm or demote advanced surface such as `configure_input()`, `run()`, `run_hook`, `load_inputs_on_init`, and `config_only`
  - Make the single-public-import product story match the actual supported Python client surface
  - Add tests that lock the intended Python client contract so it does not drift accidentally before or after release
- Acceptance criteria:
  - The Python client has an intentional and test-protected `0.1.0` contract rather than an incidental one

### DBP-API-002 - Make `DBPort(...)` startup semantics explicit and stable

- Priority: `P0`
- Status: `done`
- Files:
  - `src/dbport/adapters/primary/client.py`
  - `src/dbport/application/services/sync.py`
  - `src/dbport/application/services/fetch.py`
  - `docs/api/python.md`
  - client integration tests
- Required changes:
  - Build on the current initialization flow instead of redesigning it from scratch
  - Define exactly what happens during client initialization in full mode versus `config_only=True`
  - Decide which startup behaviors are guaranteed, including schema auto-detection, local sync, input loading, and `last_fetched_at` updates
  - Remove or tighten silent startup fallbacks where correctness-relevant failures are currently skipped without surfacing to the caller
  - Ensure constructor behavior is predictable enough to freeze as part of the public client contract
- Acceptance criteria:
  - Creating a `DBPort` instance has deliberate, documented, and testable semantics suitable for a stable public release

### Items delivered

| Item | Priority | Summary |
|---|---|---|
| DBP-DOC-001 | `P0` | Python API reference rewritten for completeness and readability |
| DBP-API-001 | `P0` | 33 contract tests lock the public `DBPort` surface |
| DBP-API-002 | `P0` | Init semantics documented and stabilized; silent fallbacks tightened |

#### DBP-DOC-001 — Python API reference

- Documented all 13 constructor parameters including `model_root`, `load_inputs_on_init`, and `config_only`
- Documented `configure_input()`, `run()`, and `run_hook` property
- Added "Initialization behavior" section with sync phase table and error guarantees
- Added "Full mode vs. config_only" comparison table
- Added `load()` vs. `configure_input()` comparison table
- Documented hook resolution order and dispatch-by-extension rules
- Restructured page for readability: Quick Reference table at top, H2 for Constructor/Methods/Example/Errors, H3 for individual methods (TOC from 13 flat entries to 5 top-level)
- Standardized every method section to consistent template: signature → parameters → returns → raises → example
- Merged `run_hook` property into `run()` section (reduces visual noise)
- Added "Complete example" section at the bottom showing full workflow
- Added anchor IDs on all method headings for cross-linking from Quick Reference

#### DBP-API-001 — Contract tests

- Created `tests/test_dbport/adapters/primary/test_contract.py` (33 tests)
- `TestModuleExports` — `__all__ == ["DBPort"]`, importability
- `TestConstructorSignature` — 2 positional + 11 keyword-only params with correct defaults, total count
- `TestPublicSurface` — 7 methods, 1 property, context manager protocol, `columns` attribute
- `TestMethodSignatures` — parameter names and defaults for all 6 public methods
- `TestConfigOnlyContract` — 6 guarded methods raise `RuntimeError`; `columns.meta()`, `columns.attach()`, `close()`, context manager, and `run_hook` work
- `TestReturnTypes` — `load()` and `configure_input()` return `IngestRecord`
- `TestInitBehavior` — full mode calls all 4 sync phases; `load_inputs_on_init=False` skips input loading; `config_only` skips all sync; sync errors do not fail init

#### DBP-API-002 — Init semantics

- Added detailed docstring to `__init__` documenting all 4 initialization phases and `config_only` behavior
- Added detailed docstrings to `_auto_detect_schema`, `_sync_output_state`, `_load_inputs`, `_update_last_fetched`
- Promoted `_sync_output_state` error logging from `debug` to `warning` (user-relevant failure)
- Added `logger` import to `FetchService` and replaced bare `pass` with `logger.debug(...)` in exception handler
- Version bumped to `0.0.4` in `pyproject.toml`
- Updated `docs/changelog.md`, `docs/roadmap.md`

---

## Work Package 0.0.5

Theme: CLI reference and executable workflows.

Status: `done`

### What was implemented

- **CLI contract tests** — 18 tests in `test_cli_contract.py` lock the full command tree: top-level commands (`init`, `status`, `model`, `config`), global options, subcommand names under `status`, `model`, and `config`, all command flags, and the absence of stale references
- **Model and version resolution contract tests** — 22 tests in `test_resolution_contract.py` lock the 5-step model resolution precedence (positional → `--model` → CWD → `default_model` → first), the 3-step version resolution for `run` (flag → configured → latest completed), the 2-step version resolution for `publish` (flag → latest completed), and the intentional difference between the two strategies
- **Exit code contract** — exit 0 (success), exit 1 (user/validation error), exit 2 (internal/unexpected error), exit 130 (interrupted); `CliUserError` exception class added for explicit validation failures; JSON errors now include `error_type` field for automation
- **CLI reference rebuilt** — `docs/api/cli.md` rewritten to match the actual nested command hierarchy including `config default model/folder/hook`, `config model MODEL_KEY version/input/schema/columns`, `status check`, all command flags, version resolution rules, and exit codes
- **CLI example fixed** — `examples/minimal_cli/run.sh` cleaned up: removed stale `dbp project sync`, fixed `dbp status --show-history` to `--history`, all commands verified against the current CLI
- **Stale docs references fixed** — `docs/examples/cli-workflow.md`, `docs/getting-started/credentials.md` updated from `dbp config check` to `dbp status check`, from `dbp config default` to `dbp config default model`
- **Stale init output fixed** — `dbp init` no longer references removed `dbp project sync`; next-steps output now shows correct `dbp model load` and `dbp model run`
- **Version resolution documented** — both `resolve_publish_version` (for `run`) and `resolve_publish_version_for_publish` (for `publish`) have detailed docstrings explaining the intentional difference
- **CLI docs readability** — `docs/api/cli.md` restructured for usability: quick-reference table at top (matching the Python API page pattern), commands grouped into three logical sections (Project Setup, Configuration, Model Operations), Configuration split into Project Defaults and Model Settings sub-groups, heading hierarchy demoted so the right-side TOC has three scannable levels instead of a flat list, and "See also" cross-links added from each model operation to its Python API equivalent

### Items delivered

| Item | Priority | Summary |
|---|---|---|
| DBP-DOC-002 | `P0` | CLI reference rebuilt to match actual nested command hierarchy |
| DBP-DOC-003 | `P0` | CLI example script fixed; all commands valid |
| DBP-DOC-004 | `P0` | Stale `dbp config check` and `dbp config default` references fixed in 3 docs |
| DBP-DOC-005 | `P1` | CLI docs restructured with quick-reference table, logical groups, and Python API cross-links |
| DBP-CLI-001 | `P0` | CLI command tree frozen with 18 contract tests |
| DBP-CLI-002 | `P0` | Model and version resolution locked with 22 precedence tests |
| DBP-CLI-003 | `P1` | Exit codes normalized (1=user, 2=internal); JSON errors include `error_type` |

---

## Work Package 0.0.6

Theme: public package surface, repository trustworthiness, and docs trustworthiness.

### DBP-DOC-005 - Clean up README drift

- Priority: `P0`
- Status: `todo`
- Files:
  - `README.md`
- Required changes:
  - Remove stale file references and setup references, including obsolete `.claude/setup.sh` and retired docs paths
  - Replace doc links with current docs entry points
  - Reduce duplicated operational content
- Acceptance criteria:
  - The README is accurate and no longer acts as a stale second docs system

### DBP-IA-001 - Make API, concepts, and examples serve distinct roles

- Priority: `P1`
- Status: `todo`
- Files:
  - `docs/api/index.md`
  - `docs/concepts/index.md`
  - `docs/examples/index.md`
  - `docs/getting-started/index.md`
- Required changes:
  - Tighten section intros so each section serves a distinct user intent
  - Cross-link sections more intentionally
- Acceptance criteria:
  - Users can clearly distinguish tutorial, concept, and reference entry points

### DBP-PKG-001 - Lock down the public Python package surface before the first release

- Priority: `P1`
- Status: `todo`
- Files:
  - `src/dbport/__init__.py`
  - `README.md`
  - `docs/api/python.md`
  - package surface tests
- Required changes:
  - Make the supported public import surface explicit and test it
  - Decide whether runtime version metadata such as `dbport.__version__` is part of the supported package contract
  - Remove or document any ambiguity between the single-import product story and the broader implementation surface
  - Ensure package metadata, README usage, and API docs all describe the same supported interface
- Acceptance criteria:
  - Users can tell exactly which Python entry points are public and supported in `0.1.0`

### DBP-PKG-002 - Complete the PyPI-facing package metadata before first publication

- Priority: `P1`
- Status: `todo`
- Files:
  - `pyproject.toml`
  - `README.md`
  - release verification notes
- Required changes:
  - Add the package metadata needed for a credible first public release on PyPI, including authorship, project URLs, classifiers, and other release-facing fields that materially affect package presentation
  - Ensure the declared package metadata matches the public product story and published docs entry points
  - Verify that release automation and artifact smoke tests exercise the packaged metadata rather than relying only on editable installs
- Acceptance criteria:
  - The published package metadata is complete enough that the first PyPI release does not look incomplete or internally inconsistent

### DBP-REL-004 - Decide and enforce the policy for generated docs artifacts in the repository

- Priority: `P1`
- Status: `todo`
- Files:
  - `site/`
  - `.gitignore`
  - docs and release workflow notes
- Required changes:
  - Decide explicitly whether generated site output belongs in the main branch, only on `gh-pages`, or nowhere in version control
  - Remove ambiguity between repository contents, ignore rules, and the GitHub Pages deployment flow
  - Ensure maintainers do not need to infer from the current repo state whether generated docs artifacts should be committed
- Acceptance criteria:
  - The repository has one clear and documented policy for generated docs artifacts, and the release workflow matches it

### DBP-REPO-001 - Add baseline governance files for the first public release

- Priority: `P1`
- Status: `todo`
- Files:
  - `CONTRIBUTING.md`
  - `SECURITY.md`
  - `CODE_OF_CONDUCT.md`
  - release/repository documentation
- Required changes:
  - Add the minimal repository governance and contribution files expected for an externally facing public project
  - Document how contributors should propose changes, how users should report vulnerabilities, and the baseline expectations for public collaboration
  - Ensure these files align with the release process and public support posture for `0.1.0`
- Acceptance criteria:
  - The repository includes the core governance and contribution guidance needed for a first public release

---

## Work Package 0.0.7

Theme: execution model and conceptual docs depth.

Status: `done`

### DBP-LOCK-001 - Turn the lock file page into an operator guide

- Priority: `P1`
- Status: `done`
- Files:
  - `docs/concepts/lock-file.md`
- Required changes:
  - Expand the page beyond format description into workflow guidance
  - Explain when `dbport.lock` changes during `schema`, column metadata, `load`, and `publish`
  - Clarify what the lock file is for in normal development and code review
  - Make the page clearly answer why the file exists, why it is safe to commit, and how it relates to warehouse state
- Acceptance criteria:
  - The lock file page explains not only structure but also how users are expected to work with it

### DBP-LOCK-002 - Add annotated lock file examples and diff walkthroughs

- Priority: `P1`
- Status: `done`
- Files:
  - `docs/concepts/lock-file.md`
- Required changes:
  - Replace the single static TOML example with a more teachable annotated example
  - Show the main sections of `dbport.lock` with clear explanation of `default_model`, `schema`, `inputs`, and `versions`
  - Add realistic diff-style examples for schema changes, snapshot updates, and appended publish history
  - Use supported Zensical authoring patterns such as code annotations and tabs where they improve clarity
- Acceptance criteria:
  - Readers can understand a real lock file and recognize normal diffs in pull requests

### DBP-LOCK-003 - Document lock file recovery and merge conflict handling

- Priority: `P1`
- Status: `done`
- Files:
  - `docs/concepts/lock-file.md`
  - related workflow docs if needed
- Required changes:
  - Add guidance for resolving merge conflicts in `dbport.lock`
  - Explain what to do when the file is stale, missing, or points at the wrong default model
  - Clarify when manual edits are acceptable and when regeneration through DBPort is the safer path
  - Document the main failure modes users are likely to hit before and after first publish
- Acceptance criteria:
  - The docs provide a clear recovery path for common lock file problems without forcing users to infer behavior from source code

### DBP-IA-002 - Add dedicated documentation for hook-based workflows

- Priority: `P1`
- Status: `done`
- Files:
  - `docs/concepts/hooks.md` or equivalent new page
  - `docs/examples/python-workflow.md`
  - `docs/api/python.md`
  - `docs/api/cli.md`
- Required changes:
  - Add one clear home for the already-implemented hook-based execution flow
  - Cover run hooks, resolution, SQL vs Python hooks, and `exec` vs `run`
- Acceptance criteria:
  - Hook behavior is discoverable without reverse-engineering examples

### DBP-IA-003 - Use section index pages more deliberately

- Priority: `P1`
- Status: `done`
- Files:
  - `docs/getting-started/index.md`
  - `docs/concepts/index.md`
  - `docs/api/index.md`
  - `docs/examples/index.md`
- Required changes:
  - Turn section index pages into stronger section landing pages
  - Add concise orientation copy and `start here` guidance
- Acceptance criteria:
  - Section landing pages feel intentional rather than placeholder-like

### DBP-RUN-001 - Tighten hook execution and publish safety semantics before release

- Priority: `P0`
- Status: `done`
- Files:
  - `src/dbport/application/services/run.py`
  - `src/dbport/application/services/publish.py`
  - `src/dbport/application/services/schema.py`
  - hook and publish tests
  - conceptual docs for execution and publish behavior
- Required changes:
  - Build on the existing `run`, `exec`, and publish services rather than introducing a second execution model
  - Define the supported hook execution model for `0.1.0`, including hook resolution order and supported hook types
  - Decide which validation failures must block publish rather than being logged and skipped
  - Tighten the fail-fast contract around schema and publish safety so the release behavior matches the product promise of safe publication
  - Ensure `exec`, `run`, `publish`, and `dry` mode semantics are stable enough to document as long-lived behavior
- Acceptance criteria:
  - Execution and publish semantics are deliberate, safety-oriented, and stable enough to freeze for `0.1.0`

### Items delivered

| Item | Priority | Summary |
|---|---|---|
| DBP-LOCK-001 | `P1` | Lock file page expanded into operator guide with mutation table and workflow guidance |
| DBP-LOCK-002 | `P1` | Annotated TOML examples with code annotations; three diff walkthroughs |
| DBP-LOCK-003 | `P1` | Recovery guidance for merge conflicts, stale files, manual edits, and regeneration |
| DBP-IA-002 | `P1` | New `docs/concepts/hooks.md` with resolution order, dispatch, trust model; cross-links from API and example pages |
| DBP-IA-003 | `P1` | All four section index pages strengthened with "start here" guidance |
| DBP-RUN-001 | `P0` | Publish blocks on catalog connection failures; hook execution raises clear error on missing files |

---

## Work Package 0.0.8

Theme: Zensical navigation model.

### DBP-ZEN-001 - Restore the intended left sidebar on the homepage

- Priority: `P1`
- Status: `todo`
- Files:
  - `docs/index.md`
- Required changes:
  - Remove `hide: navigation` from the homepage
  - Reconsider whether `hide: toc` should remain
- Relevant Zensical docs:
  - [Navigation](https://zensical.org/docs/setup/navigation/)
- Acceptance criteria:
  - The homepage participates in the main docs navigation model

### DBP-ZEN-002 - Expand left navigation by default

- Priority: `P1`
- Status: `todo`
- Files:
  - `zensical.toml`
- Required changes:
  - Add `navigation.expand`
- Relevant Zensical docs:
  - [Navigation](https://zensical.org/docs/setup/navigation/)
- Acceptance criteria:
  - Section subpages are visible by default in the left sidebar on desktop

### DBP-ZEN-004 - Keep right sidebar TOC instead of integrating it into the left nav

- Priority: `P1`
- Status: `todo`
- Files:
  - `zensical.toml`
- Required changes:
  - Explicitly avoid `toc.integrate`
  - Preserve `toc.follow`
- Relevant Zensical docs:
  - [Navigation](https://zensical.org/docs/setup/navigation/)
- Acceptance criteria:
  - The docs keep the intended tabs + left nav + right TOC layout

---

## Work Package 0.0.9

Theme: homepage UX and publication-facing docs polish.

### DBP-ZEN-010 - Apply stronger Zensical teaching patterns to core concept pages

- Priority: `P2`
- Status: `todo`
- Files:
  - `docs/concepts/lock-file.md`
  - other concept pages as needed
- Required changes:
  - Adopt supported Zensical patterns that improve comprehension without custom hacks
  - Prefer annotated code blocks, tabs, and clearer intra-page structure where the content benefits from them
  - Use the lock file page as the first publication-quality example of those patterns
- Relevant Zensical docs:
  - [Code blocks](https://zensical.org/docs/authoring/code-blocks/)
  - [Content tabs](https://zensical.org/docs/authoring/content-tabs/)
  - [Admonitions](https://zensical.org/docs/authoring/admonitions/)
  - [Tooltips](https://zensical.org/docs/authoring/tooltips/)
  - [Formatting](https://zensical.org/docs/authoring/formatting/)
  - [Grids](https://zensical.org/docs/authoring/grids/)
  - [Diagrams](https://zensical.org/docs/authoring/diagrams/)
- Acceptance criteria:
  - At least one core concept page uses supported Zensical teaching patterns in a way that materially improves readability

### DBP-UI-001 - Redesign the homepage card grid to fit clean rows

- Priority: `P1`
- Status: `todo`
- Files:
  - `docs/index.md`
- Required changes:
  - Replace the 4-card layout with either 3 or 6 cards
  - Preferred direction: 6 cards aligned to the actual docs IA
- Relevant Zensical docs:
  - [Grids](https://zensical.org/docs/authoring/grids/)
  - [Navigation](https://zensical.org/docs/setup/navigation/)
- Acceptance criteria:
  - The homepage grid feels balanced on large screens

### DBP-UI-002 - Improve card interaction without unsupported hacks

- Priority: `P1`
- Status: `todo`
- Files:
  - `docs/index.md`
- Required changes:
  - Make cards feel more obviously actionable using supported Zensical patterns
  - Avoid custom hacks for fully clickable cards unless Zensical officially supports them
- Relevant Zensical docs:
  - [Buttons](https://zensical.org/docs/authoring/buttons/)
  - [Grids](https://zensical.org/docs/authoring/grids/)
  - [Customization](https://zensical.org/docs/customization/)
- Acceptance criteria:
  - Homepage cards are clearer and more usable without relying on unsupported behavior

### DBP-UI-003 - Make the homepage more documentation-first

- Priority: `P1`
- Status: `todo`
- Files:
  - `docs/index.md`
- Required changes:
  - Reduce generic marketing copy
  - Emphasize the actual documentation entry points
- Relevant Zensical docs:
  - [Navigation](https://zensical.org/docs/setup/navigation/)
  - [Buttons](https://zensical.org/docs/authoring/buttons/)
  - [Grids](https://zensical.org/docs/authoring/grids/)
- Acceptance criteria:
  - The homepage behaves more like a documentation hub than a product landing page

### DBP-UI-004 - Review homepage TOC behavior

- Priority: `P2`
- Status: `todo`
- Files:
  - `docs/index.md`
- Required changes:
  - Decide deliberately whether the homepage should keep `hide: toc`
- Relevant Zensical docs:
  - [Navigation](https://zensical.org/docs/setup/navigation/)
- Acceptance criteria:
  - Homepage sidebar behavior is intentional and consistent with the page content

---

## Work Package 0.1.0

Theme: first public release with publishing to PyPI and GitHub Pages.

### DBP-REL-001 - Publish the package to PyPI as part of the 0.1.0 release workflow

- Priority: `P0`
- Status: `todo`
- Files:
  - packaging/release workflow
  - `pyproject.toml`
  - release documentation
- Required changes:
  - Define the PyPI publishing workflow for the package
  - Make sure the package version used for release matches `pyproject.toml`
  - Build and smoke-test the distributable artifact as part of the same release path, not as an afterthought
  - Document the release steps needed for the first public package release
- Acceptance criteria:
  - Version `0.1.0` can be published to PyPI through a documented, repeatable process

### DBP-REL-002 - Publish the docs to GitHub Pages as part of the 0.1.0 release workflow

- Priority: `P0`
- Status: `todo`
- Files:
  - `.github/workflows/docs.yml`
  - GitHub Pages deployment configuration
  - release documentation
- Required changes:
  - Make the GitHub Pages publishing workflow reliable for the first public release
  - Ensure the deployed docs expose `latest` and version-specific paths
  - Ensure the changelog is included in the published site
- Relevant Zensical docs:
  - [Basics](https://zensical.org/docs/setup/basics/)
  - [Navigation](https://zensical.org/docs/setup/navigation/)
  - [Offline usage](https://zensical.org/docs/setup/offline/)
- Acceptance criteria:
  - Version `0.1.0` docs are published to GitHub Pages with working version navigation

### DBP-REL-003 - Tie release publishing, docs versioning, and changelog updates into one release checklist

- Priority: `P0`
- Status: `todo`
- Files:
  - release/versioning docs
  - changelog workflow
  - package/docs publishing workflow
- Required changes:
  - Define one release checklist covering:
    - package version update
    - changelog entry
    - PyPI publication
    - GitHub Pages publication
    - `latest` update
  - Ensure the first public release path is documented around `0.1.0`
- Acceptance criteria:
  - The first public release can be executed from one coherent release process

### DBP-QA-001 - Make credential resolution and release checks deterministic across environments

- Priority: `P0`
- Status: `todo`
- Files:
  - `src/dbport/infrastructure/credentials.py`
  - `src/dbport/cli/commands/check.py`
  - `tests/test_dbport/infrastructure/test_credentials.py`
  - `tests/test_dbport/cli/test_check.py`
- Required changes:
  - Define and document the exact precedence between constructor kwargs, project `.env`, and shell environment variables
  - Fix the currently observed ambient-credential leakage in tests and `dbp status check`
  - Remove ambient maintainer credentials from influencing tests and `dbp status check` behavior unexpectedly
  - Make the credential test suite pass reliably even when a real `.env` file or real environment variables are present
  - Ensure release verification does not silently depend on workstation-specific secrets
- Acceptance criteria:
  - Credential behavior is deterministic in local development, CI, and release verification
  - The credential-related test suite passes without requiring a clean shell session

### DBP-QA-002 - Verify the built package artifact before publishing 0.1.0

- Priority: `P0`
- Status: `todo`
- Files:
  - `pyproject.toml`
  - packaging/release workflow
  - package smoke-test commands
- Required changes:
  - Build both sdist and wheel as part of the release path
  - Install the built artifact in a clean environment before publishing to PyPI
  - Smoke-test the shipped entry points from the built artifact, including `from dbport import DBPort` and `dbp --version`
  - Fail the release workflow if the packaged artifact differs materially from editable/development behavior
- Acceptance criteria:
  - The exact artifact intended for PyPI is built, installable, and minimally exercised before publication

### DBP-QA-003 - Make CI enforce the declared quality bar for the first public release

- Priority: `P0`
- Status: `todo`
- Files:
  - `.github/workflows/ci.yml`
  - `pyproject.toml`
  - release verification notes
- Required changes:
  - Make CI run the same coverage gate that is declared in `pyproject.toml`
  - Add explicit build verification for the package, not only source-tree tests
  - Record which operating systems and Python versions are officially verified for `0.1.0`
  - Ensure the release checklist references these CI gates as mandatory, not optional
- Acceptance criteria:
  - CI reflects the actual release bar for `0.1.0` and fails when coverage or packaging checks regress

### DBP-QA-004 - Promote runnable examples to release-gated smoke tests

- Priority: `P1`
- Status: `todo`
- Files:
  - `examples/minimal/main.py`
  - `examples/minimal_cli/run.sh`
  - `.github/workflows/ci.yml`
  - example-oriented docs pages
- Required changes:
  - Treat the shipped Python and CLI examples as compatibility assets, not illustrative prose only
  - Add smoke verification so example commands stay aligned with the actual CLI and Python API
  - Prevent example drift between docs, README snippets, and the implemented command surface
  - Decide which example checks run in normal CI and which run only in release verification
- Acceptance criteria:
  - The examples shipped for `0.1.0` are intentionally maintained and verified against the package behavior

---

## Unscheduled Backlog

These items are out of scope until after `0.1.0`.

### DBP-ZEN-003 - Add progress feedback for instant navigation

- Priority: `P2`
- Status: `todo`
- Relevant Zensical docs:
  - [Navigation](https://zensical.org/docs/setup/navigation/)

### DBP-ZEN-005 - Improve section orientation with breadcrumbs and tabs

- Priority: `P2`
- Status: `todo`
- Relevant Zensical docs:
  - [Navigation](https://zensical.org/docs/setup/navigation/)
  - [Content tabs](https://zensical.org/docs/authoring/content-tabs/)

### DBP-ZEN-006 - Enable repository content actions

- Priority: `P1`
- Status: `todo`
- Relevant Zensical docs:
  - [Repository](https://zensical.org/docs/setup/repository/)
  - [Customization](https://zensical.org/docs/customization/)

### DBP-ZEN-007 - Review repository integration polish

- Priority: `P2`
- Status: `todo`
- Relevant Zensical docs:
  - [Repository](https://zensical.org/docs/setup/repository/)
  - [Data privacy](https://zensical.org/docs/setup/data-privacy/)

### DBP-ZEN-008 - Consider header autohide

- Priority: `P2`
- Status: `todo`
- Relevant Zensical docs:
  - [Header](https://zensical.org/docs/setup/header/)
  - [Navigation](https://zensical.org/docs/setup/navigation/)

### DBP-ZEN-009 - Review search experience and exclusions

- Priority: `P2`
- Status: `todo`
- Relevant Zensical docs:
  - [Basics](https://zensical.org/docs/setup/basics/)
  - [Tags](https://zensical.org/docs/setup/tags/)

### DBP-UI-005 - Review palette choices against product tone

- Priority: `P2`
- Status: `todo`
- Files:
  - `docs/brand.md`
- Required changes:
  - Use `docs/brand.md` as the source of truth for brand colors and visual tone
- Relevant Zensical docs:
  - [Colors](https://zensical.org/docs/setup/colors/)
  - [Fonts](https://zensical.org/docs/setup/fonts/)
  - [Customization](https://zensical.org/docs/customization/)

### DBP-UI-006 - Review icon and brand consistency

- Priority: `P2`
- Status: `todo`
- Files:
  - `docs/brand.md`
- Required changes:
  - Use `docs/brand.md` as the source of truth for logo, icon usage, typography, and brand consistency
- Relevant Zensical docs:
  - [Logo and icons](https://zensical.org/docs/setup/logo-and-icons/)
  - [Fonts](https://zensical.org/docs/setup/fonts/)
  - [Header](https://zensical.org/docs/setup/header/)

### DBP-UI-007 - Keep UI changes within documented Zensical capabilities

- Priority: `P1`
- Status: `todo`
- Relevant Zensical docs:
  - [Customization](https://zensical.org/docs/customization/)
  - [Basics](https://zensical.org/docs/setup/basics/)

---

## Definition Of Done

This roadmap is complete when the repository is ready for its first public release:

- `0.0.1` delivers a working versioning mechanism and changelog/roadmap foundation
- `0.0.2` establishes changelog and roadmap foundations for a release-facing repository
- `0.0.3` defines the release language and versioning policy clearly, including consistent runtime version reporting
- `0.0.4` freezes the supported Python client contract and makes client startup semantics stable enough to document and support
- `0.0.5` freezes the CLI contract, including command taxonomy, model/version resolution, and operational error behavior
- `0.0.6` locks down the public package surface, completes PyPI-facing package metadata, establishes the public repository posture, and makes public-facing documentation trustworthy
- `0.0.7` makes execution, hook, publish, and lock-file semantics stable enough for a safety-oriented first release
- `0.0.8` implements the intended Zensical navigation model
- `0.0.9` makes the homepage publication-ready
- `0.1.0` delivers the first public release to PyPI and GitHub Pages with deterministic credentials, verified package artifacts, enforced CI quality gates, and release-gated examples
