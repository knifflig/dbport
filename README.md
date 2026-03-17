# DBPort

[![CI](https://github.com/knifflig/dbport/actions/workflows/ci.yml/badge.svg)](https://github.com/knifflig/dbport/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/dbport)](https://pypi.org/project/dbport/)
[![Python 3.11–3.12](https://img.shields.io/pypi/pyversions/dbport)](https://pypi.org/project/dbport/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

The production layer for DuckDB data products.

Load governed warehouse inputs into DuckDB, run your own SQL or Python model logic, and publish datasets back to Iceberg — with schema contracts, version tracking, metadata, and codelists managed automatically.

**You bring the model. DBPort manages the dataset lifecycle.**

## Quickstart

```bash
pip install dbport
```

```python
from dbport import DBPort

with DBPort(agency="wifor", dataset_id="emp__regional_trends") as port:
    port.schema("sql/create_output.sql")           # declare output contract
    port.load("estat.nama_10r_3empers",             # load warehouse inputs
              filters={"wstatus": "EMP"})
    port.execute("sql/transform.sql")               # run your model logic
    port.publish(version="2026-03-09",              # publish to warehouse
                 params={"wstatus": "EMP"})
```

Or use the CLI:

```bash
dbp init wifor emp__regional_trends
dbp model load
dbp model execute sql/transform.sql
dbp model publish --version 2026-03-09
```

## Why DBPort

Teams want the speed of DuckDB for real analytical work, but productionizing those workflows is harder than it should be. The friction is not in the model logic — it is in everything around it: loading versioned inputs, enforcing output schemas, preserving metadata, making reruns reproducible, and keeping publishes safe.

DBPort solves that gap:

- **DuckDB-native execution** — tables up to 1 billion rows stream through DuckDB's Iceberg extension. No batch loops, no memory copies.
- **Governed inputs** — `port.load()` pulls from Iceberg with snapshot caching. Unchanged tables are skipped automatically.
- **Output contracts** — `port.schema()` declares the output shape. `publish()` checks for schema drift before writing anything.
- **Automatic metadata** — timestamps, input provenance, codelists, and version history are tracked without manual files.
- **Idempotent publish** — interrupted runs resume from checkpoint. Re-running a completed version is a no-op.
- **Committable state** — `dbport.lock` is TOML, credential-free, and safe to commit. It tracks schema, inputs, and version history.

## Configuration

DBPort reads credentials from environment variables or constructor kwargs:

```bash
export ICEBERG_REST_URI=https://catalog.example.com
export ICEBERG_CATALOG_TOKEN=your-token
export ICEBERG_WAREHOUSE=your-warehouse
```

See the [credentials guide](https://knifflig.github.io/dbport/latest/getting-started/credentials/) for all options.

## Documentation

Full docs at **[knifflig.github.io/dbport](https://knifflig.github.io/dbport)**

- [Getting Started](https://knifflig.github.io/dbport/latest/getting-started/) — installation, credentials, first run
- [Concepts](https://knifflig.github.io/dbport/latest/concepts/) — inputs, outputs, metadata, lock file, versioning
- [Python API](https://knifflig.github.io/dbport/latest/api/python/) — `DBPort` class reference
- [CLI Reference](https://knifflig.github.io/dbport/latest/api/cli/) — `dbp` command reference
- [Examples](https://knifflig.github.io/dbport/latest/examples/) — complete Python and CLI workflows

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
