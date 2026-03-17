# Concepts

Understand the design decisions behind DBPort before diving into the API.

DBPort manages the dataset lifecycle around your model logic. It owns the edges — inputs, outputs, metadata, versioning, and publication — while leaving the middle flexible.

## Core ideas

- **[Inputs & Loading](inputs.md)** — how warehouse tables are loaded into DuckDB with snapshot caching and filter pushdown
- **[Outputs & Schemas](outputs.md)** — how output contracts are declared, enforced, and validated against the warehouse
- **[Metadata & Codelists](metadata.md)** — automatic lifecycle fields, codelist generation, and column-level overrides
- **[Lock File](lock-file.md)** — the committable TOML file that tracks schema, ingest state, and version history
- **[Hooks & Execution](hooks.md)** — how run hooks are resolved, dispatched, and trusted
- **[Versioning & Publish](versioning.md)** — idempotent publication, checkpoints, publish modes, and schema drift protection

Start with [Inputs & Loading](inputs.md) if you want to understand data flow, or [Lock File](lock-file.md) if you want to understand project state.

## The mental model

DBPort is the port where warehouse datasets come in, model logic runs, and governed outputs leave.

```
Warehouse ──▶ port.load() ──▶ DuckDB ──▶ port.execute() ──▶ port.publish() ──▶ Warehouse
              (inputs)        (your SQL)    (transforms)      (outputs)
```

The user brings the model. DBPort manages everything else.

---

See also: [API Reference](../api/index.md) for exact signatures · [Examples](../examples/index.md) for applied usage
