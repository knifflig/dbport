# Metadata & Codelists

DBPort manages metadata automatically. You never write `metadata.json` or codelist CSV files manually.

## Lifecycle fields

| Field | Source | When set |
|---|---|---|
| `agency_id` | Constructor / `dbp init` | Every init |
| `dataset_id` | Constructor / `dbp init` | Every init |
| `created_at` | Auto | First publish only |
| `last_updated_at` | Auto (publish time) | Every publish |
| `last_fetched_at` | Auto | Every initialization |
| `params` | Publish parameters | Every publish |
| `inputs` | Load calls | Accumulated across all loads |
| `versions` | Auto (append) | Every publish |

## Codelists

On publish, a codelist is auto-generated for each output column from its distinct values. You can customize this behavior per column.

### Overriding codelist metadata

=== "CLI"

    ```bash
    dbp config model wifor.emp__regional_trends columns set nuts2024 \
        --id NUTS2024 --kind hierarchical
    ```

=== "Python"

    ```python
    port.columns.nuts2024.meta(
        codelist_id="NUTS2024",
        codelist_kind="hierarchical",
        codelist_type="string",
        codelist_labels={"en": "NUTS 2024 Regions"},
    )
    ```

| Parameter | Default | Description |
|---|---|---|
| `codelist_id` | column name | Identifier for the codelist |
| `codelist_kind` | inferred from SQL type | `"flat"` or `"hierarchical"` |
| `codelist_type` | inferred from SQL type | Value type hint |
| `codelist_labels` | `null` | Human-readable labels per language |

### Attaching an external codelist table

=== "CLI"

    ```bash
    dbp config model wifor.emp__regional_trends columns attach nuts2024 wifor.cl_nuts2024
    ```

=== "Python"

    ```python
    port.columns.nuts2024.attach(table="wifor.cl_nuts2024")
    ```

The referenced table should already be loaded via `dbp model load` or `port.load()`. On publish, the full table is exported as the codelist instead of auto-generating from distinct output values.

### Chaining in Python

In the Python API, `.meta()` returns `self` for chaining:

```python
port.columns.nuts2024.meta(codelist_id="NUTS2024").attach(table="wifor.cl_nuts2024") # (1)!
```

1. `.meta()` returns the `ColumnConfig` instance, so `.attach()` can be called immediately on the result.

## How metadata is stored

!!! info "No files on disk"

    On publish, the finalized `metadata.json` is built in-memory and embedded directly in Iceberg table properties (gzip + base64 compressed). Codelist CSVs are generated in-memory from DuckDB and embedded in Iceberg column docs. No intermediate files are written to disk.
