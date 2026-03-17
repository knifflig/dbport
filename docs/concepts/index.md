# Concepts

Understand the design decisions behind DBPort before diving into the API.

DBPort handles the edges of periodic dataset recomputation: loading versioned inputs, enforcing output schemas, tracking publish history, and managing metadata. The model logic in between is yours.

## Core ideas

- **[Inputs & Loading](inputs.md)** — how warehouse tables are loaded into DuckDB with snapshot caching and filter pushdown
- **[Outputs & Schemas](outputs.md)** — how output contracts are declared, enforced, and validated against the warehouse
- **[Metadata & Codelists](metadata.md)** — automatic lifecycle fields, codelist generation, and column-level overrides
- **[Lock File](lock-file.md)** — the committable TOML file that tracks schema, ingest state, and version history
- **[Hooks & Execution](hooks.md)** — how run hooks are resolved, dispatched, and trusted
- **[Versioning & Publish](versioning.md)** — idempotent publication, checkpoints, publish modes, and schema drift protection

Start with [Inputs & Loading](inputs.md) if you want to understand data flow, or [Lock File](lock-file.md) if you want to understand project state.

## The mental model

```
Warehouse ──▶ dbp model load ──▶ DuckDB ──▶ dbp model exec ──▶ dbp model publish ──▶ Warehouse
              (inputs)           (your SQL/Python)                (versioned output)
```

You bring the model. DBPort manages the dataset lifecycle.

---

See also: [API Reference](../api/index.md) for exact signatures · [Examples](../examples/index.md) for applied usage
