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

### DBP-VERS-001 - Make version-specific docs browsing actually work end-to-end

- Priority: `P0`
- Status: `done`
- Files:
  - `zensical.toml`
  - `.github/workflows/docs.yml`
  - `scripts/update_versions.py`
  - deployed `gh-pages` output
- Required changes:
  - Build on the existing docs deployment workflow and `update_versions.py` script rather than replacing them
  - Ensure `versions.json` is reliably generated during deployment
  - Ensure the version selector is visible in deployed docs
  - Ensure `latest/` always points to the newest published docs
  - Ensure specific published versions remain browsable under stable version paths
  - Fix workflow ordering issues that can break version metadata generation
- Relevant Zensical docs:
  - [Basics](https://zensical.org/docs/setup/basics/)
  - [Navigation](https://zensical.org/docs/setup/navigation/)
- Acceptance criteria:
  - Users can browse `latest` and switch to specific published versions from the docs UI

### DBP-VERS-002 - Define the project version source of truth for docs builds

- Priority: `P0`
- Status: `done`
- Files:
  - `pyproject.toml`
  - `.github/workflows/docs.yml`
  - versioning scripts and docs build workflow
- Required changes:
  - Treat this backlog as the source of truth for the pre-release milestone plan and `pyproject.toml` as the package version source used by automation
  - Make docs versioning derive primarily from `pyproject.toml`
  - Treat Git tags as deployment triggers, not the sole version source
  - Validate consistency between the declared package version and the release trigger
- Acceptance criteria:
  - Docs version labels and versioned output are driven primarily by `pyproject.toml`

### DBP-VERS-007 - Clarify local preview behavior for versioned docs

- Priority: `P1`
- Status: `done`
- Files:
  - docs maintenance notes
  - local docs workflow
  - optional dev helper scripts
- Required changes:
  - Decide whether local preview should support a mock/generated `versions.json`
  - If not, document the limitation clearly for maintainers
  - Optionally add a helper for local preview of version-selector behavior
- Relevant Zensical docs:
  - [Basics](https://zensical.org/docs/setup/basics/)
  - [Navigation](https://zensical.org/docs/setup/navigation/)
- Acceptance criteria:
  - Maintainers know how to test version browsing before release

### DBP-FOUND-006 - Keep local docs build verification in the delivery workflow

- Priority: `P1`
- Status: `done`
- Files:
  - local docs workflow
  - CI docs workflow
- Required changes:
  - Keep the already-working local build path lightweight; do not turn docs verification into a fragile custom toolchain
  - Keep `uv run zensical build --clean` as a required verification step for substantial docs changes
  - Optionally enforce docs build validation in CI
- Relevant Zensical docs:
  - [Basics](https://zensical.org/docs/setup/basics/)
- Acceptance criteria:
  - Docs work includes an explicit build verification step

---

## Work Package 0.0.2

Theme: release history and roadmap foundations.

### DBP-VERS-003 - Add a first-class changelog page to the docs site

- Priority: `P0`
- Status: `todo`
- Files:
  - `docs/changelog.md`
  - `zensical.toml`
- Required changes:
  - Add a dedicated changelog page
  - Add it to the docs navigation
  - Structure it around versions
- Relevant Zensical docs:
  - [Navigation](https://zensical.org/docs/setup/navigation/)
  - [Lists](https://zensical.org/docs/authoring/lists/)
  - [Formatting](https://zensical.org/docs/authoring/formatting/)
- Acceptance criteria:
  - The docs site exposes a visible changelog page

### DBP-VERS-004 - Ensure every published version receives a changelog entry

- Priority: `P0`
- Status: `todo`
- Files:
  - `docs/changelog.md`
  - release/versioning process
- Required changes:
  - Require a changelog entry for every released version
  - Make changelog updates part of the release workflow
  - Define the minimum expected content for each entry
- Acceptance criteria:
  - No published version exists without a corresponding changelog entry

### DBP-VERS-008 - Add a first-class roadmap page for the full repository

- Priority: `P0`
- Status: `todo`
- Files:
  - `docs/roadmap.md`
  - `zensical.toml`
  - broader repo planning workflow
- Required changes:
  - Add a dedicated roadmap page to the docs site
  - Use the same milestone-based structure used in this backlog
  - Expand the scope beyond docs to include package, CLI, runtime, release, testing, and repository-wide work
  - Keep roadmap distinct from changelog:
    - roadmap = planned future work
    - changelog = completed released work
  - Add the roadmap page to the docs navigation
- Relevant Zensical docs:
  - [Navigation](https://zensical.org/docs/setup/navigation/)
  - [Lists](https://zensical.org/docs/authoring/lists/)
  - [Formatting](https://zensical.org/docs/authoring/formatting/)
  - [Data tables](https://zensical.org/docs/authoring/data-tables/)
- Acceptance criteria:
  - The docs site contains a roadmap page for the full repository, not only documentation tasks

---

## Work Package 0.0.3

Theme: version policy and release planning language.

### DBP-VERS-005 - Document and enforce the project's release numbering policy

- Priority: `P0`
- Status: `todo`
- Files:
  - `docs/changelog.md`
  - release/versioning docs
  - packaging/release workflow
- Required changes:
  - Document the project's chosen `x.x.x` policy
  - Capture the project-specific convention:
    - first number bump = major release step
    - second number bump = normal release step
    - third number bump = minor release step
  - Use that policy consistently in changelog and release docs
- Acceptance criteria:
  - Contributors have one clear versioning policy to follow

### DBP-VERS-006 - Encode the initial release milestones in docs and release workflow

- Priority: `P1`
- Status: `todo`
- Files:
  - changelog and release/versioning docs
  - packaging/release workflow
- Required changes:
  - Record that predevelopment starts at `0.0.1`
  - Record that the first version published to PyPI and GitHub Pages will be `0.1.0`
- Acceptance criteria:
  - The initial release path is explicitly documented

### DBP-VERS-009 - Make runtime and CLI version reporting follow the declared source of truth

- Priority: `P1`
- Status: `todo`
- Files:
  - `pyproject.toml`
  - `src/dbport/cli/main.py`
  - package/version tests
- Required changes:
  - Make runtime-facing version reporting derive from the same package version source used by release automation
  - Remove hard-coded fallback behavior that can report a release version unrelated to the installed package
  - Decide whether package-level runtime version metadata is part of the supported public contract and test it accordingly
  - Ensure `dbp --version`, package metadata, docs version labels, and release automation all describe the same version coherently
- Acceptance criteria:
  - Runtime and CLI version reporting are consistent with the declared package version source of truth

---

## Work Package 0.0.4

Theme: Python API reference correctness.

### DBP-DOC-001 - Complete Python API reference

- Priority: `P0`
- Status: `todo`
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
- Status: `todo`
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
- Status: `todo`
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

---

## Work Package 0.0.5

Theme: CLI reference and executable workflows.

### DBP-DOC-002 - Rebuild CLI reference around real command hierarchy

- Priority: `P0`
- Status: `todo`
- Files:
  - `docs/api/cli.md`
- Required changes:
  - Build on the implemented nested command tree under `status`, `config`, and `model`
  - Replace stale flat config framing with the actual nested CLI structure
  - Document `dbp status check`
  - Correct flags and examples to match implementation
  - Clarify configuration commands vs lifecycle commands
- Acceptance criteria:
  - The CLI reference matches the current CLI help output

### DBP-DOC-003 - Fix broken runnable CLI example

- Priority: `P0`
- Status: `todo`
- Files:
  - `examples/minimal_cli/run.sh`
- Required changes:
  - Remove stale commands that no longer exist, including `dbp project sync`
  - Replace stale status flags
  - Verify the full script against the current CLI
- Acceptance criteria:
  - The example script contains only valid commands and flags

### DBP-DOC-004 - Remove stale command references from workflow pages

- Priority: `P0`
- Status: `todo`
- Files:
  - `docs/examples/cli-workflow.md`
  - `docs/getting-started/quickstart.md`
  - `docs/getting-started/credentials.md`
- Required changes:
  - Replace stale config/check examples
  - Align workflow snippets with the CLI reference
- Acceptance criteria:
  - Workflow pages no longer teach commands that do not exist

### DBP-CLI-001 - Freeze the CLI command taxonomy before `0.1.0`

- Priority: `P0`
- Status: `todo`
- Files:
  - `src/dbport/cli/main.py`
  - `src/dbport/cli/commands/config.py`
  - `src/dbport/cli/commands/model.py`
  - `src/dbport/cli/commands/init.py`
  - CLI contract tests
- Required changes:
  - Freeze the currently implemented top-level CLI shape and subcommand taxonomy for `0.1.0`
  - Resolve naming and placement mismatches such as model-scoped behavior exposed under `config default ...`
  - Remove stale command language from scaffold output, examples, help text, and user-facing error guidance
  - Treat command names, core flags, and command grouping as a compatibility contract rather than a moving target
- Acceptance criteria:
  - The CLI tree is coherent enough to freeze without expecting renames or reshuffles immediately after release

### DBP-CLI-002 - Make CLI model selection and version resolution deterministic

- Priority: `P0`
- Status: `todo`
- Files:
  - `src/dbport/cli/context.py`
  - `src/dbport/cli/commands/lifecycle.py`
  - `src/dbport/cli/commands/config.py`
  - CLI lifecycle tests
- Required changes:
  - Define and test the precedence between positional model key, `--model`, current working directory, and `default_model`
  - Define and test the precedence for publish version resolution across explicit flags, configured model version, and version history
  - Ensure CLI defaults are unsurprising enough to become part of the stable `0.1.0` UX contract
  - Align JSON output and human-readable output around the same resolved behavior
- Acceptance criteria:
  - Model targeting and version selection behave predictably across CLI commands and are protected by tests

### DBP-CLI-003 - Normalize CLI errors, exit codes, and machine-readable output

- Priority: `P1`
- Status: `todo`
- Files:
  - `src/dbport/cli/errors.py`
  - `src/dbport/cli/commands/check.py`
  - CLI error tests
- Required changes:
  - Replace broad or inconsistent error handling with an intentional CLI error contract
  - Catch important user-facing validation failures explicitly instead of letting them surface as generic unexpected errors
  - Define stable exit-code expectations for success, user error, interruption, and validation failure
  - Keep JSON output consistent with human-readable failures so automation can rely on it
- Acceptance criteria:
  - The CLI has a stable operational contract for errors and exit behavior suitable for external users and scripts

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

### DBP-LOCK-001 - Turn the lock file page into an operator guide

- Priority: `P1`
- Status: `todo`
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
- Status: `todo`
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
- Status: `todo`
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
- Status: `todo`
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
- Status: `todo`
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
- Status: `todo`
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
