# Roadmap

This roadmap tracks planned work beyond the current release. Items are ordered by impact and likelihood of implementation. For completed work, see the [Changelog](changelog.md).

---

## Extension system

**Impact:** very high | **Effort:** medium | **Timeline:** next up

The extension system is the foundation for most items below. It introduces a plugin architecture that lets DBPort integrate with external tools without growing the core. Three extension types are planned:

- **Model extensions** — integrate model builders (dbt, sqlmesh) into the lifecycle
- **Runtime extensions** — integrate orchestrators (Prefect, Airflow) into execution
- **Input extensions** — prebuilt import channels for common data sources

The extension system will ship first because it unblocks everything else on this roadmap.

---

## Input extensions

**Impact:** high | **Effort:** low (once the extension system exists) | **Timeline:** after extension system

Prebuilt import channels that let you declare external sources as inputs without writing custom ingestion code.

Planned sources, in priority order:

1. **SDMX** — the primary target; statistical data from Eurostat, ECB, OECD, and other agencies via standardised APIs
2. **Generic HTTP/REST** — fetch and ingest from any URL returning CSV, JSON, or Parquet
3. **Public cloud storage** — direct reads from S3, GCS, or Azure Blob (outside the Iceberg catalog)
4. **Open data portals** — CKAN, Socrata, and similar APIs used by government open data platforms

---

## Model builder integrations

**Impact:** medium | **Effort:** medium | **Timeline:** planned, sooner rather than later

Deeper integration with transformation frameworks via model extensions. This is already possible today — you can run dbt or sqlmesh and publish the result through DBPort — but a native integration would remove the custom glue code.

Planned integrations:

- **dbt** — run dbt models as the transform step, with DBPort managing inputs and publication
- **sqlmesh** — same pattern, leveraging sqlmesh's incremental model support

---

## Orchestrator integrations

**Impact:** medium | **Effort:** medium | **Timeline:** planned

Runtime extensions that embed DBPort into orchestration platforms. Since orchestrators already provide scheduling, retries, and DAG execution, the integration mostly maps DBPort primitives onto their building blocks — which may actually simplify execution compared to standalone use.

Planned integrations:

- **Prefect** — DBPort tasks and flows as native Prefect components
- **Airflow** — DBPort operators for Airflow DAGs

---

## S3 + Parquet as standalone warehouse

**Impact:** high (usability) | **Effort:** high | **Timeline:** worth exploring before other backends

Not every team runs an Iceberg catalog. A plain S3 bucket with Parquet files is the lowest barrier to entry — no catalog server, no REST API, just files on object storage. Supporting this as a first-class backend would make DBPort accessible to teams that don't have (or don't want) a full Iceberg setup.

The trade-off is significant: without Iceberg's native table versioning, snapshot semantics, and schema evolution, DBPort would need to reimplement those guarantees in its own domain layer. Contracts and codelists would be stored as sidecar files next to the Parquet data rather than embedded in table properties. That's a substantial amount of new domain logic — but the usability gain for smaller teams and simpler setups makes this worth investigating even before extending to other catalog backends.

---

## Additional warehouse backends

**Impact:** high | **Effort:** high | **Timeline:** not soon

DBPort currently targets Iceberg REST catalogs exclusively. Extending to other warehouse backends would broaden adoption significantly but requires substantial adapter work.

Candidates, roughly ordered by market adoption:

1. **AWS Glue Catalog** (Iceberg) — most common managed Iceberg catalog
2. **Hive Metastore** (Iceberg) — widely deployed in on-premise Hadoop environments
3. **Unity Catalog** (Databricks) — growing Iceberg support, large user base
4. **Snowflake Iceberg Tables** — Snowflake-managed Iceberg with proprietary catalog
5. **Google BigLake** — GCP-native Iceberg integration
6. **Delta Lake** — different table format, similar lifecycle needs
7. **Apache Hudi** — another open table format with its own catalog model

---

## Additional compute engines

**Impact:** high | **Effort:** very high | **Timeline:** not soon

DuckDB is deeply embedded as DBPort's execution engine. Supporting alternative compute engines would require abstracting the SQL layer, connection management, and data exchange paths.

Candidates, in order of priority:

1. **PostgreSQL** — most widely deployed relational database; natural fit for teams already running Postgres
2. **MongoDB** — document-oriented; would require rethinking the SQL-based transform model
3. **ClickHouse** — column-oriented analytics engine; strong fit for large-scale aggregation workloads
4. **Apache Spark** — distributed compute; relevant for datasets that exceed single-node scale

---

## SDMX Fusion Registry as warehouse backend

**Impact:** unclear | **Effort:** high | **Timeline:** to be determined

An alternative to classical Iceberg warehouses: using an [SDMX Fusion Registry](https://www.sdmx.io/) as both catalog and storage backend. This is an interesting perspective because SDMX brings much stronger contracts than Iceberg table properties — data structure definitions, codelists, concept schemes, and content constraints are first-class citizens.

The trade-off: stronger governance out of the box, but less flexibility to integrate with the broader data ecosystem. DBPort's current strength is that it layers governance onto whatever warehouse you already use. An SDMX backend would deliver deeper contracts at the cost of that flexibility.
