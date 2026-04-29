# ADR 0001: Variable Mutability

## Status

Accepted

## Context

`VariableAssigner` currently mutates any variable selector that happens to exist in
the runtime variable pool. That behavior was tolerable while the Dify workflow UI
prevented users from targeting most selectors, but `graphon` is now a standalone
library and no longer inherits that frontend restriction.

Issue #24 asks us to clarify the language semantics for variable assignment instead
of leaving them implementation-defined.

## Decision

We define mutability as a property of runtime variables:

- Variables are read-only by default.
- `VariableAssigner` may mutate only variables where `writable == True`.
- Conversation variables are created writable during bootstrap.
- Loop and iteration working variables created by container nodes are writable.
- Node output variables stored by the engine are read-only by default.

## Consequences

- Assignment semantics no longer depend on which UI generated the graph.
- Downstream users can opt into writable variables intentionally.
- Scope-aware mutability and privileged external control commands remain out of
  scope for this change.
