# Inputs & Loading

DBPort loads Iceberg tables from the warehouse into a local DuckDB database. Once loaded, tables are available in DuckDB under their exact original address.

## Loading a table

=== "CLI"

    ```bash
    # Configure an input and load it immediately
    dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers --load

    # Or configure first, load later
    dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers
    dbp model load
    ```

=== "Python"

    ```python
    port.load("estat.nama_10r_3empers")
    ```

The table is available in DuckDB as `estat.nama_10r_3empers` — no aliasing.

## Filters

Filters are equality predicates pushed down to the Iceberg scan. They reduce the data scanned at the Parquet level. There is no `limit` parameter — always scope data with `filters`.

=== "CLI"

    ```bash
    dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers \
        --filter wstatus=EMP --filter nace_r2=TOTAL --load
    ```

=== "Python"

    ```python
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP", "nace_r2": "TOTAL"})
    ```

## Snapshot-based caching

Each Iceberg table has a snapshot ID. If the snapshot has not changed since the last run and the DuckDB table already exists, loading is skipped automatically.

This makes repeated runs fast without manual cache management. To force re-resolution of the newest snapshot:

=== "CLI"

    ```bash
    dbp model load --update
    ```

=== "Python"

    ```python
    # Snapshot caching is automatic — just call load() again.
    # The table is only re-loaded if the warehouse snapshot changed.
    port.load("estat.nama_10r_3empers")
    ```

## How ingestion works

Under the hood, loading uses pyiceberg's Arrow C++ multi-threaded Parquet reader:

1. pyiceberg scans the table with the given filters and snapshot
2. A `RecordBatchReader` streams Arrow batches into DuckDB
3. DuckDB creates (or replaces) the table from the Arrow stream

No full-table Python memory allocation — data streams directly from Parquet to DuckDB.

## Input tracking

Every load is recorded in `dbport.lock`:

- Table address
- Filters applied
- Snapshot ID and timestamp
- Row count

This state is included in published metadata and enables the snapshot-based skip logic on subsequent runs.
