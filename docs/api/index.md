# API Reference

Complete specification of every command, method, and parameter.

The **CLI** (`dbp`) is the default interface for working with DBPort. Start there for project setup, configuration, and running workflows.

The **Python API** (`DBPort`) is the runtime engine underneath. Use it when you need programmatic control or custom model logic in a hook file.

- **[CLI Reference](cli.md)** — the `dbp` command for initializing, configuring, running, and publishing models
- **[Python API](python.md)** — the `DBPort` class with `schema()`, `load()`, `execute()`, `publish()`, and column metadata configuration

Both interfaces share the same `dbport.lock` state and produce identical results.

---

See also: [Getting Started](../getting-started/index.md) for first-time setup · [Examples](../examples/index.md) for runnable workflows
