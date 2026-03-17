# DBPort

[![CI](https://github.com/knifflig/dbport/actions/workflows/ci.yml/badge.svg)](https://github.com/knifflig/dbport/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/dbport)](https://pypi.org/project/dbport/)
[![Python 3.11–3.12](https://img.shields.io/pypi/pyversions/dbport)](https://pypi.org/project/dbport/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Versioned dataset recomputation on DuckDB, published to Iceberg.

Analytic workloads often recompute the same large dataset every few weeks or months to produce a new version. That workflow — download inputs, run model logic, upload the result — is simple in concept but tedious to productionize: schema contracts, snapshot caching, version tracking, metadata, and safe publication all need to work every time. DBPort handles that lifecycle so you can focus on the model.

## Quickstart

```bash
pip install dbport
```

```bash
# Initialize a project
dbp init regional_trends --agency wifor --dataset emp__regional_trends
cd regional_trends

# Configure schema, inputs, and columns
dbp config model wifor.emp__regional_trends schema sql/create_output.sql
dbp config model wifor.emp__regional_trends input estat.nama_10r_3empers

# Run the full lifecycle: load inputs → execute model → publish output
dbp model run --version 2026-03-09 --timing
```

For programmatic control, the same workflow in Python:

```python
from dbport import DBPort

with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
    port.schema("sql/create_output.sql")
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
    port.execute("sql/transform.sql")
    port.publish(version="2026-03-09", params={"wstatus": "EMP"})
```

## Why DBPort

The hard part of periodic dataset recomputation is not the model logic — it is everything around it:

- **Loading inputs** — `dbp model load` pulls Iceberg tables into DuckDB with snapshot caching. Unchanged tables are skipped automatically.
- **Schema contracts** — `dbp config model ... schema` declares the output shape. Publishing checks for schema drift before writing anything.
- **Version tracking** — each publish records version, timestamp, parameters, and row count. Re-running a completed version is a no-op.
- **Metadata** — timestamps, input provenance, codelists, and version history are attached to the published table automatically.
- **Safe publication** — interrupted runs resume from checkpoint. Schema drift blocks the publish rather than corrupting the warehouse.
- **Committable state** — `dbport.lock` is TOML, credential-free, and safe to commit. It tracks schema, inputs, and version history for code review and CI.

## Configuration

DBPort reads credentials from environment variables:

```bash
export ICEBERG_REST_URI=https://catalog.example.com
export ICEBERG_CATALOG_TOKEN=your-token
export ICEBERG_WAREHOUSE=your-warehouse
```

See the [credentials guide](https://knifflig.github.io/dbport/latest/getting-started/credentials/) for all options.

## Documentation

Full docs at **[knifflig.github.io/dbport](https://knifflig.github.io/dbport)**

- [Getting Started](https://knifflig.github.io/dbport/latest/getting-started/) — installation, credentials, first run
- [Concepts](https://knifflig.github.io/dbport/latest/concepts/) — inputs, outputs, metadata, lock file, hooks, versioning
- [CLI Reference](https://knifflig.github.io/dbport/latest/api/cli/) — `dbp` command reference
- [Python API](https://knifflig.github.io/dbport/latest/api/python/) — `DBPort` class reference
- [Examples](https://knifflig.github.io/dbport/latest/examples/) — complete CLI and Python workflows

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
