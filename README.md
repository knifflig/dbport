# DBPort

[![CI](https://github.com/knifflig/dbport/actions/workflows/ci.yml/badge.svg)](https://github.com/knifflig/dbport/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/dbport)](https://pypi.org/project/dbport/)
[![Python 3.11–3.12](https://img.shields.io/pypi/pyversions/dbport)](https://pypi.org/project/dbport/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Governance and orchestration for recomputable warehouse datasets.

You build models that produce datasets — and those datasets depend on each other. When external sources update, you need to recompute downstream models in the right order, knowing exactly which input versions went into each output. As the number of models grows, keeping track of dependencies, provenance, and data quality becomes harder than the modeling itself.

DBPort is the orchestration layer on top of your warehouse that enforces governance into recomputable workflows. It tracks dependencies between your models and on external inputs, so you can build with the confidence that future updates will be picked up correctly — and that other models can pick up your results.

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

- **Dependency tracking** — models produce datasets that feed other models. DBPort tracks these dependencies so you always know what depends on what across your organisation.
- **Input provenance** — every publish records exactly which input versions and snapshots were used. Trace any output back to the data that produced it.
- **Recompute on change** — snapshot-cached inputs detect when external sources update. Unchanged tables are skipped — only what's new gets reprocessed.
- **Schema drift detection** — declare the output shape upfront. Drift is caught before anything is written to the warehouse, not after.
- **Versioned, resumable publishes** — each publish records version, parameters, and row count. Interrupted runs resume from checkpoint. Re-running a completed version is a safe no-op.
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

- [About DBPort](https://knifflig.github.io/dbport/latest/getting-started/about/) — why it exists and who it's for
- [Getting Started](https://knifflig.github.io/dbport/latest/getting-started/) — installation, credentials, first run
- [Concepts](https://knifflig.github.io/dbport/latest/concepts/) — inputs, outputs, metadata, lock file, hooks, versioning
- [CLI Reference](https://knifflig.github.io/dbport/latest/api/cli/) — `dbp` command reference
- [Python API](https://knifflig.github.io/dbport/latest/api/python/) — `DBPort` class reference
- [Examples](https://knifflig.github.io/dbport/latest/examples/) — complete CLI and Python workflows

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
