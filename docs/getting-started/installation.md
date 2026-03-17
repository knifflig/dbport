# Installation

## Install with pip

```bash
pip install dbport
```

## Install with uv

```bash
uv add dbport
```

This installs DBPort and all runtime dependencies:

- **duckdb** — embedded analytical database
- **pyarrow** — Apache Arrow for columnar data
- **pyiceberg** — Iceberg table format client
- **pydantic** — data validation
- **typer** + **rich** — CLI framework

## DuckDB extensions

The DuckDB `iceberg` and `httpfs` extensions are installed automatically at runtime via HTTPS when first needed. No manual pre-download is required.

## Python version

DBPort requires **Python 3.11 or 3.12**.

## Verify installation

After installing, verify the CLI is available:

```bash
dbp --help
```

You should see the `dbp` command help with subcommands like `init`, `status`, `model`, and `config`.

To verify the Python API:

```python
from dbport import DBPort
```
