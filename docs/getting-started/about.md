# About DBPort

## What is DBPort?

DBPort is a DuckDB-native runtime for building reproducible warehouse datasets. It manages the full dataset lifecycle — loading inputs from a warehouse, running them through your model logic, and publishing versioned outputs back — so you can focus on the work that actually matters: the model itself.

You bring the model. DBPort manages the dataset lifecycle.

## Who is it for?

**If you periodically recompute datasets from warehouse inputs, DBPort is for you.** Whether that means refreshing a single indicator once a month or rebuilding an entire family of interdependent datasets on a regular cycle, the production concerns are the same: inputs need loading, schemas need enforcing, versions need tracking, and outputs need publishing safely.

DBPort becomes especially valuable when the number of datasets grows. Teams managing dozens of models that depend on each other — where recomputation must be orchestrated, dependencies tracked across projects, and each output governed with clear contracts — find that the lifecycle overhead quickly outweighs the modeling work itself. That is the problem DBPort was built to solve.

### Bridging two worlds

DBPort sits at an intersection that few tools address directly:

- **Data engineers and analysts** think in warehouses, pipelines, and scheduled jobs. They need reliable input loading, schema validation, and safe publication into production systems.
- **Data scientists and statistical modelers** think in versioned artifacts, reproducibility, and dataset governance. They need to know exactly which inputs produced which outputs, with full provenance.

DBPort serves both by handling the production edges — inputs, contracts, metadata, versions, publication — while leaving the modeling layer entirely open. Use SQL, Python, Polars, pandas, or anything else that runs on DuckDB. DBPort does not care how you build your model, only that the dataset around it is governed.

## What it handles

- **Input loading** — pulls Iceberg tables into DuckDB with snapshot caching. Unchanged inputs are skipped automatically.
- **Schema contracts** — declare the output shape upfront. Publishing checks for schema drift before writing anything.
- **Version tracking** — each publish records version, timestamp, parameters, and row count. Re-running a completed version is a no-op.
- **Metadata and codelists** — timestamps, input provenance, codelists, and version history are attached to published tables automatically.
- **Safe publication** — interrupted runs resume from checkpoint. Schema drift blocks the publish rather than corrupting the warehouse.
- **Committable state** — `dbport.lock` is TOML, credential-free, and tracks schema, inputs, and versions for code review and CI.

## Column contracts

Each output column can carry codelist metadata that links it to a reference table — declaring what the column means, what values it can take, and where those values are defined. This is a simplified approach inspired by [SDMX](https://sdmx.org/) (the statistical data exchange standard), adapted for practical warehouse use.

For consumers of your dataset, this means every column is self-documenting: they know not just the data type, but the semantic meaning and the authoritative source of its values.

## How it fits with other tools

| Tool | Relationship |
|---|---|
| **DuckDB** | The execution engine. DBPort adds the production lifecycle around it: governed inputs, output contracts, and publish semantics. |
| **dbt** | Complementary. dbt excels at transformation logic in the middle; DBPort manages dataset lifecycle at the edges — inputs, contracts, versions, and publication. |
| **Orchestrators** (Airflow, Dagster, …) | DBPort is not an orchestrator. It defines what a safe, reproducible run means at the dataset level. Orchestrators decide when to trigger it. |
| **Warehouse platforms** | DBPort is not a warehouse. It is the runtime layer that connects DuckDB-based model execution to warehouse-backed datasets via Iceberg. |

## When DBPort is not the right fit

DBPort is designed for periodic dataset recomputation — workflows where you rebuild outputs from warehouse inputs on a regular cycle. If your primary need is event-driven ingestion, real-time streaming, or storing application data (like tracking new user signups), other tools will serve you better. DBPort shines when the challenge is not "how do I store this data?" but "how do I reliably rebuild, govern, and publish these datasets over time?"
