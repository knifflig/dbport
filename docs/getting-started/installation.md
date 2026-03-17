# Installation

## Install

=== "uv"

    ```bash
    uv add dbport
    ```

=== "pip"

    ```bash
    pip install dbport
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

=== "CLI"

    ```bash
    dbp --help
    ```

    You should see the `dbp` command help with subcommands like `init`, `status`, `model`, and `config`.

=== "Python"

    ```python
    from dbport import DBPort
    ```
