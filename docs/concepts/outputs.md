# Outputs & Schemas

DBPort uses explicit output contracts. Every dataset must declare its schema before data is written.

## Declaring a schema

Pass an inline DDL string or a path to a `.sql` file:

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

If the output table already exists in the warehouse, `port.schema()` compares the local DDL against the warehouse schema immediately. Incompatible changes raise `SchemaDriftError`:

```
SchemaDriftError: Schema drift detected:
  + new_column (string)     # added locally, not in warehouse
  - old_column (int32)      # in warehouse, removed locally
  ~ value (int32 → float64) # type changed
```

This check runs at both `port.schema()` time (early fail-fast) and `port.publish()` time (safety net).

## Idempotency

Calling `port.schema()` with the same DDL is idempotent — the table is created or replaced in DuckDB without error.

## Column metadata

After declaring a schema, every column gets a default codelist entry. Override metadata per column:

```python
port.columns.nuts2024.meta(
    codelist_id="NUTS2024",
    codelist_kind="hierarchical",
    codelist_labels={"en": "NUTS 2024 Regions"},
)
```

Attach an external codelist table:

```python
port.columns.nuts2024.attach(table="wifor.cl_nuts2024")
```

See [Metadata & Codelists](metadata.md) for full details.
