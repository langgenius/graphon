# TD-02: Reject invalid graphs before execution

**Status:** completed locally; implementation, independent reviews, and checks complete.
**Last update:** 2026-09-08.
**Priority:** P1; selected as the first debt remediation.
**Owner:** current Graph/DSL development task on `laipz8200/graph-validation`.
**Tracking:** [TD-02 in the debt register](../technical-debt.md#td-02--graph-construction-and-structural-validation).
[Implementation issue #277](https://github.com/langgenius/graphon/issues/277)
tracks this fix and its pull request. Related
[authoring issue #131](https://github.com/langgenius/graphon/issues/131) is not this
fix's implementation ticket.

## Objective and scope

Reject invalid graphs at validated construction boundaries before input can be
silently discarded or execution can report false success. Before this change,
`start → a → b → a` succeeded after running only `start`, leaving `a` and `b`
`UNKNOWN`; duplicate node IDs and malformed edges could also disappear during
normalization. These reproduced results justified selecting TD-02 ahead of the
separate snapshot and file adapter work.

The implementation changes graph construction, default validation, and DSL error
translation. It preserves scheduler behavior, container execution, snapshot
formats, supported editor/legacy inputs, and edge IDs for valid graphs. Current
behavior and upgrade guidance live in [Architecture](../../ARCHITECTURE.md#state-and-execution-invariants)
and [Migration](../../MIGRATION.md#graph-validation).

## Implementation and evidence

| Boundary | Change | Evidence |
| --- | --- | --- |
| [Graph configuration](../../src/graphon/graph/graph.py) | Reject invalid/duplicate executable node IDs and malformed edge fields before indexing or filtering; enforce edge ID uniqueness within each owning scope. | [Scoping tests](../../tests/graph/test_graph_scoping.py) cover rejection before construction, unchanged inputs, legacy ownership, scoped edge IDs, and valid custom containers. |
| [Default validation](../../src/graphon/graph/validation.py) | Share endpoint and cycle checks between retained-subtree preflight and materialized graph validation. | [Scoping tests](../../tests/graph/test_graph_scoping.py) cover self-cycles, nested/inactive cycles, unknown endpoints, sibling-scope exclusion, and trusted bypasses; [validator tests](../../tests/graph/test_graph_validation.py) retain root/endpoint coverage. |
| [Python builder](../../src/graphon/graph/graph.py) | Reject non-string/empty IDs at registration and invalid handles/cycles by `build()`. Nodes are already constructed by the caller. | [Builder tests](../../tests/graph/test_graph.py) exercise both edge creation methods and preserve valid DAGs. |
| [DSL loading](../../src/graphon/dsl/importer.py) | Serialize slotted validation issues with `dataclasses.asdict()` into the existing `graph.validation_failed` envelope. | [Importer tests](../../tests/dsl/test_importer.py) reject the false-success cycle before execution, run its acyclic control, and inspect structured root/cycle errors. |
| [Child frame construction](../../src/graphon/engine/frame.py) | Fresh/restored frames use the same `Graph.init()` boundary; no frame changes were needed. | Existing [container execution](../../tests/engine/test_cooperative_container_execution.py), [serialization](../../tests/engine/test_runtime_state_serialization.py), and [snapshot isolation](../../tests/test_snapshot_version_isolation.py) checks pass in the full suite. |

## Delivery steps

- [x] Inspect construction callers, reproduce the defects, and repeat the required
  issue/PR search before implementation.
- [x] Add behavior regressions, observe their intended failures, and complete
  independent test reviews before implementation.
- [x] Implement input validation, shared cycle detection, builder checks, and
  structured DSL errors.
- [x] Preserve resolved factory root types and explicit validation bypasses, with
  independently reviewed compatibility regressions.
- [x] Run focused and full checks; update affected guides, migration notes,
  changelog, and debt evidence through the independent knowledge review.
- [x] Complete the final naming pass and affected checks before handoff.

The duplicate search was repeated before preparing the draft PR, and
[issue #277](https://github.com/langgenius/graphon/issues/277) was created as
required by [Contributing](../../CONTRIBUTING.md#issues).

## Decision log

- **2026-09-08 — Implementation authorized:** the user requested committing the
  prior documentation and proceeding with TD-02. That documentation is committed
  as `cc674fa`; this plan records the subsequent local implementation.
- **2026-09-08 — Construction boundary:** use stdlib
  `graphlib.TopologicalSorter.prepare()` after endpoint checks. Cross-scope edges
  are already rejected, so one topology check covers the retained scopes without
  constructing children. Changing scheduling would neither recover discarded
  input nor make unsupported cycles valid.
- **2026-09-08 — Compatibility:** root eligibility stays on the constructed
  node's execution/type information, preserving custom factory aliases for
  built-in container entries. `skip_validation=True` bypasses endpoint existence,
  root type, and cycles while mandatory input/ownership checks remain. Raw
  `Graph(...)` stays a trusted assembly surface. The unreleased 0.8.0 migration
  notes describe these changes; local implementation is not a release or an
  accepted repository-wide design decision.

## Validation and outcome

On Python 3.12.13, the initial regressions produced **32 failures and 66 passing
controls** for the missing behavior. A later compatibility run produced **3
failures and 1 passing control** before its fixes. Fresh context-free test reviews
covered both sets.

The current focused command passes **136 tests**:

```bash
uv run pytest -n 0 \
  tests/graph/test_graph_scoping.py \
  tests/graph/test_graph.py \
  tests/graph/test_graph_validation.py \
  tests/dsl/test_importer.py \
  tests/dsl/test_node_factory.py
```

`just test` passes **815 tests**; `just check` and `git diff --check` also pass.
An earlier full run had one gevent subprocess timeout; its isolated rerun and the
current full run passed. Python 3.13 and live external integrations have not been
run locally. After the independent knowledge review's documentation edits,
`uv run pytest -n 0 tests/test_repository_docs.py` passes **1 test**.
The final naming pass applied mechanical renames, and the focused checks,
`just check`, and full 815-test suite passed again afterward.

The preimplementation tracking search covered open and closed graph-validation
issues/PRs and found no dedicated or overlapping fix. Related
[issue #131](https://github.com/langgenius/graphon/issues/131) covers authoring,
[PR #271](https://github.com/langgenius/graphon/pull/271) filter performance, and
[PR #200](https://github.com/langgenius/graphon/pull/200) decision records. The
draft-PR preparation search also found no overlapping fix; #277 now provides
dedicated implementation tracking.
