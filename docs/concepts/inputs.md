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

This makes repeated runs fast without manual cache management.

!!! tip "Forcing a fresh snapshot check"

    Use `dbp model load --update` (CLI) to re-resolve the newest snapshot even when the cached snapshot ID has not changed. In Python, snapshot caching is automatic — calling `port.load()` again only re-loads if the warehouse snapshot actually changed.

## How ingestion works

Under the hood, loading uses pyiceberg's Arrow C++ multi-threaded Parquet reader. No full-table Python memory allocation — data streams directly from Parquet to DuckDB.

``` mermaid
graph LR
    A["Iceberg table<br/>(Parquet files)"] -->|"pyiceberg scan<br/>+ filters"| B["RecordBatchReader<br/>(Arrow batches)"]
    B -->|"streaming insert"| C["DuckDB table<br/>(local file)"]
```

## Input tracking

!!! info "What gets recorded in `dbport.lock`"

    Every load records the following in the lock file:

    - **Table address** — the fully qualified Iceberg table name
    - **Filters applied** — equality predicates used during the scan
    - **Snapshot ID and timestamp** — pins the exact data version loaded
    - **Row count** — number of rows ingested

    This state is included in published metadata and enables the snapshot-based skip logic on subsequent runs.
