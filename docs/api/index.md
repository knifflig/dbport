# API Reference

DBPort provides two interfaces: a Python API and a CLI.

- **[Python API](python.md)** — the `DBPort` class with `schema()`, `load()`, `execute()`, `publish()`, and column metadata configuration
- **[CLI Reference](cli.md)** — the `dbp` command for initializing, configuring, running, and publishing models from the terminal

## Which to use

The **CLI** (`dbp`) is the default operational interface. Use it for initializing projects, running workflows, inspecting state, and publishing.

The **Python API** is the runtime engine underneath. Use it when you need programmatic control, embedding in scripts, or custom model logic beyond what the CLI scaffold provides.

Both interfaces share the same `dbport.lock` state and produce identical results.
