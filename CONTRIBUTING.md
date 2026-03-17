# Contributing to DBPort

Thank you for considering a contribution to DBPort.

## Prerequisites

- Python 3.11 or 3.12
- [uv](https://docs.astral.sh/uv/) for dependency management

## Setup

```bash
git clone https://github.com/knifflig/dbport.git
cd dbport
uv sync
```

## Running tests

```bash
uv run pytest
```

All tests must pass before submitting a pull request. The test suite mirrors the `src/dbport/` layout under `tests/test_dbport/`.

## Code style

The project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting, enforced by CI:

```bash
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

## Architecture

DBPort follows hexagonal architecture with three layers:

1. **Domain** — pure Python value objects and port protocols (no I/O)
2. **Application** — use-case services depending only on ports
3. **Adapters** — concrete implementations wired in `DBPort.__init__`

The single public import is `from dbport import DBPort`. No other symbols are part of the public API.

## Pull requests

- One topic per PR
- Include tests for new behavior
- All existing tests must continue to pass
- Add a changelog entry in `docs/changelog.md` for user-facing changes
- Keep commits focused with clear, imperative-mood messages

## Reporting issues

Open an issue at [github.com/knifflig/dbport/issues](https://github.com/knifflig/dbport/issues).

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
