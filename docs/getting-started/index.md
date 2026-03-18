# Getting Started

DBPort is the governance layer between your models and your Iceberg warehouse. You bring the warehouse and credentials — DBPort gives you a DuckDB workspace where inputs are tracked, outputs are versioned, and every publish is safe and reproducible.

## Prerequisites

- Python 3.11 or 3.12
- An Iceberg REST catalog with S3-compatible object storage
- Catalog credentials (URI, token, warehouse name)

## 1. Install

```bash
pip install dbport
```

## 2. Set credentials

DBPort needs to know how to reach your warehouse. Export the credentials for your Iceberg catalog:

```bash
export ICEBERG_REST_URI=https://catalog.example.com
export ICEBERG_CATALOG_TOKEN=your-token
export ICEBERG_WAREHOUSE=your-warehouse
```

See [Credentials](credentials.md) for all options including S3 access keys.

## 3. Initialize a model

```bash
dbp init regional_trends --agency wifor --dataset emp__regional_trends
cd regional_trends
```

This creates a project folder with a `dbport.lock` and a `data/` directory. The lock file tracks your schema, inputs, and versions — commit it alongside your code.

## 4. Configure and run

```bash
# Declare the output schema and inputs
dbp config model wifor.emp__regional_trends schema sql/create_output.sql
dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers

# Run the full lifecycle: load inputs → execute transforms → publish
dbp model run --version 2026-03-09 --timing
```

Behind the scenes, DBPort loads your inputs into a local DuckDB file (`data/emp__regional_trends.duckdb`). You can open that file with any DuckDB client to explore data, develop queries, or debug transforms — it's a regular DuckDB database.

## 5. Publish

Publishing writes your output back to the warehouse as a versioned Iceberg table — with schema validation, metadata, and codelists attached automatically. If a publish is interrupted, it resumes from checkpoint. If the version was already completed, it's a safe no-op.

## 6. Build on it

Once your first model is published, its output is a versioned artifact in the warehouse. The next model can declare it as an input:

```bash
dbp init downstream_model --agency wifor --dataset emp__summary
cd downstream_model
dbp config model wifor.emp__summary input wifor.emp__regional_trends
```

That's the core idea: each model's output becomes a governed, versioned input for the next. DBPort tracks these dependencies so recomputes flow through your model graph in the right order, picking up new versions automatically.

## Next steps

<div class="grid cards" markdown>

-   **About**

    ---

    Why DBPort exists, who it's for, and how it fits with DuckDB, dbt, and orchestrators.

    [:octicons-arrow-right-24: Read more](about.md)

-   **Credentials**

    ---

    All credential options including S3 access keys and `.env` files.

    [:octicons-arrow-right-24: Configure](credentials.md)

-   **Quickstart**

    ---

    Walk through a complete project from init to publish.

    [:octicons-arrow-right-24: Start building](quickstart.md)

-   **Concepts**

    ---

    How inputs, schemas, metadata, versioning, and the lock file work together.

    [:octicons-arrow-right-24: Read the concepts](../concepts/index.md)

</div>
