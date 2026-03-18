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

``` mermaid
graph LR
    A["Warehouse"] -->|"dbp model load"| B["DuckDB"]
    B -->|"dbp model exec"| B
    B -->|"dbp model publish"| C["Warehouse"]
    style A fill:#f5f5f5,stroke:#1F4E79
    style B fill:#f5f5f5,stroke:#E07A5F
    style C fill:#f5f5f5,stroke:#1F4E79
```

You bring the model. DBPort manages the dataset lifecycle.

---

See also: [API Reference](../api/index.md) for exact signatures · [Examples](../examples/index.md) for applied usage
