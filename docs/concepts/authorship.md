# Authorship & Ownership

DBPort needs to answer two different questions about a model:

- Who is responsible for this model by default?
- Who actually performed a specific operation such as a publish?

These are related, but they are not the same thing.

This page describes the intended authorship model for DBPort at a high level. It is a product concept, not yet a fully wired command reference.

## Why authorship exists

DBPort is designed for governed dataset workflows. In practice, that means datasets are not only versioned by data and schema, but also by stewardship:

- a model may have a default maintainer
- one model in a repo may be maintained by a different person than another
- a specific publish may be executed by someone other than the default maintainer
- warehouses may be shared across teams, environments, and contributors

Without explicit authorship, it becomes hard to answer basic operational questions:

- Who owns this model?
- Who published this version?
- Did this publish come from the expected maintainer or from a temporary override?
- When multiple maintainers work in one repository, which model belongs to whom?

## Three separate identities

DBPort should keep three kinds of identity separate.

### Warehouse binding

This answers: which remote warehouse context does this state belong to?

Warehouse binding is about location, not people. It distinguishes one catalog or warehouse from another so that local state from one environment is not mistaken for another.

### Model maintainer

This answers: who is responsible for this model by default?

This is stewardship metadata. It is mutable and can change over time as ownership of a model changes.

### Operation actor

This answers: who actually performed a specific action?

This is event provenance. A publish, sync, or load can be attributed to the actor who ran it, even if that actor is not the long-term maintainer of the model.

## Default and model-specific maintainers

The lightweight rule is:

- a repository can define a default maintainer
- a model can optionally override that default
- if a model has no explicit maintainer, it inherits the default maintainer

This keeps the common case simple while still supporting repositories where different models are stewarded by different people.

### Intended precedence

When DBPort resolves the maintainer for a model, it should use this order:

1. model-specific maintainer
2. default maintainer
3. generated anonymous actor

The anonymous fallback keeps the system usable even when no maintainer metadata has been configured yet.

## Ownership vs. action history

The default or model-specific maintainer should not be treated as the same thing as the actor who publishes a version.

Those concepts should be stored separately:

- model-level maintainer: who is responsible for the model now
- version-level `published_by`: who executed this specific publish

This distinction matters because operational reality is messy. A model might be owned by one person but published by another during debugging, incident response, or CI automation.

Changing model ownership later should not rewrite historical provenance.

## Multiple maintainers in one repository

Multiple maintainers in one repository are normal. DBPort should support that directly without splitting the repository into maintainer-specific state.

The right model is:

- one shared repository
- one or more models
- each model can inherit the default maintainer or declare its own override
- each version records who published it

In other words, maintainers should be modeled per repository and per model, not as separate parallel copies of the same project state.

## Expected Python and CLI behavior

The Python client is the source of truth for this feature. The CLI should be a thin wrapper around the same semantics.

At a high level, the workflow should be:

- configure a default maintainer once for the repository
- optionally set a per-model maintainer override
- let `publish()` resolve the effective maintainer automatically
- record the acting maintainer on the published version

In the CLI, this implies a shape like:

```bash
dbp config default author set --name "..." --email "..." --organisation "..."
dbp config model test.table1 author set --name "..." --email "..." --organisation "..."
```

In Python, this implies matching semantics on `DBPort`, with the same inheritance order.

## Anonymous actors

Authorship should be optional.

If no maintainer information has been configured, DBPort should still be able to operate by generating an anonymous actor identity. That fallback should be stable enough to avoid creating a new identity on every operation.

Anonymous actors are useful for:

- early experimentation
- local development before metadata is configured
- automation contexts where explicit maintainer data is not yet available

But anonymous actors should not silently become project owners. They are only a fallback identity for action provenance.

## Merge and conflict expectations

Authorship metadata should be treated differently depending on its type.

- default maintainer: repository-level configuration
- model maintainer: model-level configuration override
- `published_by`: immutable historical record on a version

That leads to a simple conflict rule:

- current ownership can change
- historical actors should not be rewritten

If ownership changes, future publishes should use the new resolved maintainer. Existing published versions should continue to show who actually published them.

## Relationship to warehouse metadata

When a version is published, DBPort can attach authorship metadata to the warehouse together with the rest of the version metadata.

At minimum, that should allow a published dataset version to answer:

- who published this version
- which model it belonged to
- which warehouse binding it was published into

Project-level ownership may also be reflected in warehouse metadata, but it should remain easy to change without rewriting historical version actors.

## Lightweight design principle

The authorship model should stay lightweight:

- no maintainer setup required to get started
- one default maintainer for the common case
- model-level override only when needed
- per-version actor stamping happens automatically

This keeps the normal user flow simple while still providing enough governance for shared warehouse workflows.
