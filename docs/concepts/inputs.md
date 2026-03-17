# Inputs & Loading

DBPort loads Iceberg tables from the warehouse into a local DuckDB database. Once loaded, tables are available in DuckDB under their exact original address.

## Loading a table

```python
port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
```

The table is available in DuckDB as `estat.nama_10r_3empers` — no aliasing.

## Filters

Filters are equality predicates pushed down to the Iceberg scan:

```python
port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP", "nace_r2": "TOTAL"})
```

Filters reduce the data scanned at the Parquet level. There is no `limit` parameter — always scope data with `filters`.

## Snapshot-based caching

Each Iceberg table has a snapshot ID. If the snapshot has not changed since the last run and the DuckDB table already exists, `load()` skips the table automatically.

This makes repeated runs fast without manual cache management.

## How ingestion works

Under the hood, `load()` uses pyiceberg's Arrow C++ multi-threaded Parquet reader:

1. pyiceberg scans the table with the given filters and snapshot
2. A `RecordBatchReader` streams Arrow batches into DuckDB
3. DuckDB creates (or replaces) the table from the Arrow stream

No full-table Python memory allocation — data streams directly from Parquet to DuckDB.

## Input tracking

Every `load()` call is recorded in `dbport.lock`:

- Table address
- Filters applied
- Snapshot ID and timestamp
- Row count

This state is included in published metadata and enables the snapshot-based skip logic on subsequent runs.
