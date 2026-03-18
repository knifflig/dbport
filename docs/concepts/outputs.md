# Outputs & Schemas

DBPort uses explicit output contracts. Every dataset must declare its schema before data is written.

## Declaring a schema

=== "CLI"

    ```bash
    # Apply schema from a SQL file
    dbp config model wifor.emp__regional_trends schema sql/create_output.sql
    ```

=== "Python"

    ```python
    # From a SQL file
    port.schema("sql/create_output.sql")

    # Or inline
    port.schema("""
        CREATE OR REPLACE TABLE wifor.emp__regional_trends (
            freq     VARCHAR,
            year     DATE,
            nuts2024 VARCHAR,
            value    DOUBLE
        )
    """)
    ```

The table is created in DuckDB and the schema (DDL + column list) is persisted to `dbport.lock`.

## Schema drift protection

If the output table already exists in the warehouse, DBPort compares the local DDL against the warehouse schema. Incompatible changes raise `SchemaDriftError`:

``` title="SchemaDriftError output"
SchemaDriftError: Schema drift detected:
  + new_column (string)     # (1)!
  - old_column (int32)      # (2)!
  ~ value (int32 → float64) # (3)!
```

1. Column added locally but not present in the warehouse.
2. Column exists in the warehouse but was removed from the local DDL.
3. Column type changed between local and warehouse schema.

This check runs at schema declaration time (early fail-fast) and again at publish time (safety net).

!!! info "Idempotent schema declaration"

    Declaring the same schema repeatedly is safe — the table is created or replaced in DuckDB without error. Only incompatible changes relative to the *warehouse* trigger `SchemaDriftError`.

## Column metadata

After declaring a schema, every column gets a default codelist entry. Override metadata per column:

=== "CLI"

    ```bash
    # Set codelist metadata
    dbp config model wifor.emp__regional_trends columns set nuts2024 \
        --id NUTS2024 --kind hierarchical

    # Attach an external codelist table
    dbp config model wifor.emp__regional_trends columns attach nuts2024 wifor.cl_nuts2024
    ```

=== "Python"

    ```python
    port.columns.nuts2024.meta(
        codelist_id="NUTS2024",
        codelist_kind="hierarchical",
        codelist_labels={"en": "NUTS 2024 Regions"},
    )

    port.columns.nuts2024.attach(table="wifor.cl_nuts2024")
    ```

See [Metadata & Codelists](metadata.md) for full details.
