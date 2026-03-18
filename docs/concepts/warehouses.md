# Warehouses, Bindings & Sync

DBPort operates against a warehouse, but users do not always stay connected to the same one forever.

In practice, teams switch between:

- production and test warehouses
- different catalogs with the same warehouse name
- empty bootstrap environments and already populated environments
- personal, CI, and shared credentials

This page describes the intended warehouse model for DBPort at a high level. It explains how warehouse switching, sync, status, and publish should behave when the local project state and the current warehouse do not match.

## Why warehouse binding matters

The same repository can be pointed at different warehouses over time. If DBPort treats all local state as if it belonged to a single global warehouse context, stale local information can be mistaken for current remote truth.

That causes confusing behavior such as:

- `status` showing published versions that belong to another warehouse context
- `sync` appearing successful even though the current warehouse table is missing
- `publish` skipping because of local idempotency state even when the active warehouse is empty

To avoid that, DBPort needs an explicit notion of warehouse binding.

## Warehouse binding

A warehouse binding answers one question:

- Which remote warehouse context does this local state belong to?

This is not only the warehouse name. The binding should identify the actual remote context with enough precision that two unrelated environments are not merged accidentally.

At a minimum, a binding should be derived from:

- catalog URI
- warehouse name

Additional identifiers may be included later if needed.

The purpose of the binding is simple: local state that belongs to one warehouse should not silently masquerade as state for another.

## Local state vs warehouse state

DBPort needs to handle two kinds of information at once:

- local project state needed to operate naturally
- remote warehouse state that may differ from what is cached locally

The core product problem is not that one of these exists and the other does not. The problem is making the boundary explicit.

Users should be able to tell whether DBPort is showing:

- the current warehouse state
- cached local state for the active warehouse binding
- local configuration that still exists even when the remote table is missing

## What happens when credentials change

Switching credentials is not the same thing as changing the model.

When credentials point to a different warehouse binding, DBPort should treat that as a new remote context rather than assuming the old local state is still authoritative.

That means:

- local state should be resolved relative to the active warehouse binding
- stale state from another binding should not be reused silently
- an empty warehouse should be reported as empty, not presented as if it still contained the previous publish history

The normal credential switch should feel natural, not destructive.

## Sync is a reconcile step, not a reset button

`sync` should stay lightweight.

Its job is to reconcile the local project with the active warehouse binding as cheaply and clearly as possible. It should not default to destructive behavior.

At a high level, sync should:

- resolve the active warehouse binding
- inspect whether the model exists in the current warehouse
- refresh warehouse-derived state for that binding
- update the warehouse-check heartbeat when appropriate
- tell the user clearly when the warehouse is empty, missing, or diverged

What sync should not do by default:

- silently wipe unrelated local state
- imply that local and remote are fully reconciled when the warehouse table is absent
- use a missing remote table as automatic proof that all local state should be destroyed

## Status should reflect the active warehouse context

`status` is where users form their mental model of the current project state.

Because of that, it must be explicit about the warehouse context it is showing.

At a high level, status should make it clear:

- which warehouse binding is active
- whether the output table exists in that warehouse
- whether the displayed state comes from the active binding or from local configuration only

The important rule is that stale local publish history should not be presented as if it were the current warehouse truth.

## Missing warehouse tables are normal

A missing output table is not always an error.

It can mean:

- a brand-new warehouse
- a test environment prepared for first publish
- a model that has never been published in this binding
- a warehouse that was intentionally cleaned

Because of that, missing remote state should be handled as a first-class product scenario, not as an edge case.

DBPort should surface it clearly and then let the user continue naturally.

## Publish must be remote-aware

Publish idempotency is only correct if it is evaluated against the active warehouse binding.

If DBPort skips a publish only because a local lock entry says a version was already completed, that behavior becomes wrong as soon as the user switches to a fresh or empty warehouse.

The intended behavior is:

- if the active warehouse already has the completed version, default publish is a safe no-op
- if the active warehouse is empty, publish should bootstrap it normally
- if the active warehouse contains partial checkpoints, publish should resume from them

In other words, remote checkpoint state should decide remote idempotency.

## Destructive reconciliation should be explicit

Sometimes local and remote state will diverge in a way that cannot be reconciled automatically.

When that happens, DBPort should not guess silently. It should make destructive reconciliation explicit.

That means a separate, clearly communicated step such as:

- confirm wiping binding-specific local cache
- confirm adopting the empty remote state
- confirm discarding stale local reconciliation data for the active warehouse binding

The common path should stay non-destructive. Prompts and wipes should be reserved for unresolvable divergence.

## Lightweight design principle

The warehouse model should stay lightweight for normal users:

- switching credentials should not require manual lock surgery
- sync should remain a cheap check-and-reconcile step
- status should make the current warehouse context obvious
- first publish into a fresh warehouse should work naturally
- destructive steps should happen only with explicit confirmation

This keeps DBPort easy to use in everyday workflows while still handling the reality that one repository may interact with multiple warehouse environments over time.

## Relationship to other concepts

Warehouse binding answers where local state belongs.

This is separate from authorship, which answers who owns a model or who performed an operation. It is also separate from schema and input configuration, which describe what the model is supposed to do.

Together, these concepts let DBPort say three different things clearly:

- where the state belongs
- who is responsible for it
- what the model does

That separation is what makes warehouse switching understandable instead of surprising.

## Scope of this concept

This page describes the intended product behavior only.

The concrete implementation work for warehouse-scoped lock state, warehouse-aware sync and status, and remote-aware publish idempotency is tracked separately in the issue tracker.
