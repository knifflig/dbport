# Metadata & Codelists

DBPort manages metadata automatically. You never write `metadata.json` or codelist CSV files manually.

## Lifecycle fields

| Field | Source | When set |
|---|---|---|
| `agency_id` | `DBPort(agency=...)` | Every init |
| `dataset_id` | `DBPort(dataset_id=...)` | Every init |
| `created_at` | Auto | First `publish()` only |
| `last_updated_at` | Auto (publish time) | Every `publish()` |
| `last_fetched_at` | Auto | Every `DBPort()` initialization |
| `params` | `publish(params=...)` | Every `publish()` |
| `inputs` | `port.load(...)` | Accumulated across all `load()` calls |
| `versions` | Auto (append) | Every `publish()` |

## Codelists

On `publish()`, a codelist is auto-generated for each output column from its distinct values. You can customize this behavior per column.

### `.meta()` — override codelist metadata

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

`.meta()` returns `self` for chaining:

```python
port.columns.nuts2024.meta(codelist_id="NUTS2024").attach(table="wifor.cl_nuts2024")
```

### `.attach()` — use a table as codelist source

```python
port.columns.nuts2024.attach(table="wifor.cl_nuts2024")
```

The referenced table should already be loaded via `port.load()`. On `publish()`, the full table is exported as the codelist instead of auto-generating from distinct output values.

## How metadata is stored

On `publish()`, the finalized `metadata.json` is built in-memory and embedded directly in Iceberg table properties (gzip + base64 compressed). Codelist CSVs are generated in-memory from DuckDB and embedded in Iceberg column docs. No intermediate files are written to disk.
