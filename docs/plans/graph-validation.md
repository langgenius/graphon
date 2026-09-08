# TD-02: Reject invalid graphs before execution

**Status:** active planning; implementation has not started. The contract below
is proposed, not accepted architecture policy.
**Last update:** 2026-09-08.
**Priority:** P1; recommended next debt item.
**Owner:** unassigned; Graph/DSL is the suggested responsibility area.
**Tracking:** [TD-02 in the debt register](../technical-debt.md#td-02--graph-construction-and-structural-validation).
[Issue #131](https://github.com/langgenius/graphon/issues/131) is related authoring
work, assigned to `laipz8200`; it is not an assignment or implementation ticket
for this plan. No dedicated implementation issue/PR was found in the search below.

## Objective and priority

Make the validated construction paths reject graphs that silently lose nodes or
edges, or cannot execute under the current scheduling rules. A workflow containing
`start → a → b → a` currently emits `GraphRunSucceededEvent` after executing only
`start`; `a` and `b` remain `UNKNOWN`. This was reproduced in the reviewed checkout
through public `dsl.loads()`. The Python builder also accepts that cycle.

Prioritize this over TD-03's snapshot consistency risk and TD-04's file adapter
isolation work because it is a reproduced incorrect result without a concurrency
or deployment prerequisite. TD-04 should take precedence if a deployment is about
to run concurrent workflows with distinct host file adapters. This review has no
production incident counts or deployment evidence.

## Context and implementation boundaries

Reviewed commit: `cf6c732` (the knowledge/debt review is present, with no runtime
remediation). Preserve the [execution and state invariants](../../ARCHITECTURE.md)
and the [factory preflight contract](../development.md#add-or-extend-a-node).

| Boundary | Observed problem | Planned change |
| --- | --- | --- |
| [Graph normalization](../../src/graphon/graph/graph.py), `_normalize_nodes()` and `_parse_node_configs()` | Duplicate executable IDs are indexed with the last configuration winning. | Reject duplicate and invalid executable node IDs before the first ID index is built. |
| [Edge preparation, scoping, and construction](../../src/graphon/graph/graph.py) | Non-string endpoints disappear during filtering; an explicit null `sourceHandle` disappears during `_build_edges()`. | Validate raw executable edge fields before filtering can discard them. |
| [Default validators](../../src/graphon/graph/validation.py) and `Graph.init()` preflight | Only endpoints and root type are checked by the default rules. Descendant schemas are preflighted, but descendant execution cycles are accepted. | Share a small topology check between subtree preflight and the builder's default validator. |
| [GraphBuilder](../../src/graphon/graph/graph.py), `_register_node()`, `add_node()`, and `connect()` | Type annotations do not prevent non-string IDs or null handles from reaching the built graph. | Enforce the same ID and handle-type rules through the existing builder/validator path before `build()` returns. |
| [DSL importer](../../src/graphon/dsl/importer.py), `loads()` | It delegates construction to `Graph.init()`, but its `GraphValidationError` handler reads `issue.__dict__` from a slotted dataclass and raises `AttributeError`. | Use `dataclasses.asdict()` to retain the existing structured `DslError` envelope. |
| [Frame construction](../../src/graphon/engine/frame.py), `_create_child_with_state()` | Fresh and restored child frames call `Graph.init()` using retained configuration. | Cover these callers through the shared boundary; retain frame isolation and edge identity. |

[Scheduler.is_node_ready()](../../src/graphon/engine/scheduler.py) waits for every
incoming edge to resolve. `is_execution_complete()` checks only enqueued work.
This explains the false success: neither cycle node becomes ready. Changing join
or completion semantics would affect valid branching and would not recover
discarded input. Fix construction instead.

## Proposed contract

- After removing supported `custom-note` widgets, executable node IDs must be
  non-empty strings and unique across the supplied configuration. Preserve typed
  `BaseNodeData` input, editor metadata, and canonical/legacy owner normalization.
  Normalization must leave caller-owned configuration unchanged.
- Executable edges must have string endpoints naming existing nodes in the same
  direct scope. If `sourceHandle` is absent, keep the current `"source"` default;
  if supplied, require a string. Do not add handle-name membership rules or reject
  currently accepted string handles as part of this work.
- Each executable scope must be acyclic, including self-edges and cycles in
  inactive branches or retained descendants. Validate the retained subtree before
  `create_node()` or `post_init()` runs. Loop, Iteration, and custom container
  nodes remain valid: repetition is owned by their handlers, while their internal
  execution edges follow the same rule. Acyclic disconnected and multiple-root
  configurations remain allowed; this is not a reachability requirement.
- Preserve explicit edge IDs, generated-ID allocation for valid inputs, and
  sibling scopes' ability to reuse local edge IDs. Check endpoints before cycle
  detection so an unknown endpoint does not become an implicit topology node.
- Use the existing `GraphValidationIssue`/`GraphValidationError` types for new
  graph rules. Diagnostics should identify the offending node/edge and the owning
  scope for a cycle. Preserve existing DSL normalization error codes and the
  `graph.validation_failed` envelope for errors raised by graph validators.
- Apply these guarantees to default `Graph.init()`, `dsl.loads()`, and
  `Graph.new().build()`. The builder receives already constructed nodes, so its
  guarantee is rejection at `build()`, not prevention of caller-side construction.
  For `Graph.init(skip_validation=True)`, keep node schemas, ID/type checks,
  ownership checks, and edge-ID checks mandatory; bypass endpoint-existence,
  root-execution-type, and cycle checks. Root ID presence is still required to
  construct the graph, as today. The low-level `Graph(...)`
  constructor remains a trusted assembly surface. Document both bypasses rather
  than adding validation to every engine run. No in-repository caller currently
  passes `skip_validation`.

## Delivery steps

- [x] Review knowledge, construction callers, related tests, and current tracking.
- [x] Reproduce the cycle, input loss, and DSL error translation problems.
- [ ] Before implementation, repeat the issue/PR search required by
  [Contributing](../../CONTRIBUTING.md#issues), confirm an implementation owner,
  and coordinate scope with #131. Record the implementation issue here before
  opening its PR; the new authoring API is not a prerequisite.
- [ ] Add focused regressions in the existing files listed below, then implement
  input checks before lossy normalization/scoping. Keep the existing DSL-specific
  paths and error context. Add the matching ID/handle checks to the existing
  builder/validator path; type annotations alone do not enforce them. A new graph
  schema or validation framework is unnecessary.
- [ ] Add one reusable topology check in `graph/validation.py`, using stdlib
  `graphlib.TopologicalSorter.prepare()` and `CycleError`. Apply it to each scope
  of the retained config before node construction and through the builder's
  default validator. Do not construct child graphs just to validate them.
- [ ] Repair DSL issue serialization and verify rejection through public loading.
  Keep this with the new validation so invalid graphs reliably return `DslError`.
- [ ] Update [Architecture](../../ARCHITECTURE.md), the relevant
  [development guidance](../development.md), and the unreleased 0.8.0 section of
  [MIGRATION.md](../../MIGRATION.md). Explain that previously accepted invalid
  graphs now fail and must be corrected, with explicit containers used for
  repetition. Record release changes in [CHANGELOG.md](../../CHANGELOG.md).
- [ ] Run the focused checks and contributor checks, then record the PR,
  completion evidence, and final contract here and in the debt register.

Target one focused implementation PR. Expected production edits are
`graph/graph.py`, `graph/validation.py`, and `dsl/importer.py`; engine changes,
new dependencies, snapshot formats, and #131's authoring API are outside scope.

## Acceptance checks

Extend existing test helpers; use a small parameterized set of invalid inputs,
not a new test framework.

| Check | Evidence to add or preserve |
| --- | --- |
| Duplicate IDs cannot overwrite nodes, including IDs reused across scopes; malformed endpoints and null/non-string handles cannot drop edges. | [Graph validation tests](../../tests/graph/test_graph_validation.py) and [DSL importer tests](../../tests/dsl/test_importer.py). Retain DSL duplicate-ID errors and builder duplicate-node rejection. Add a focused [builder regression](../../tests/graph/test_graph.py) for non-string IDs and invalid handles supplied through both `add_node()` and `connect()`. |
| Direct construction, DSL loading, and the builder reject self-cycles and `start → a → b → a`. | [Graph validation](../../tests/graph/test_graph_validation.py), [builder](../../tests/graph/test_graph.py), and [workflow event tests](../../tests/workflows/test_full_engine_events.py). The public loading regression must fail before execution can report success. |
| A cycle or malformed edge in a nested/inactive descendant is rejected before any node is created. | Reuse the recording factory in [scoping tests](../../tests/graph/test_graph_scoping.py) and the `create_node` spy in [DSL factory tests](../../tests/dsl/test_node_factory.py); assert no construction. |
| Valid branches, joins, multiple roots, containers, metadata, legacy ownership, omitted handles, and edge IDs retain behavior. | Existing [graph tests](../../tests/graph/), [workflow events](../../tests/workflows/test_full_engine_events.py), and [cooperative container tests](../../tests/engine/test_cooperative_container_execution.py), with small compatibility cases only where missing. |
| Validation returns structured errors rather than `AttributeError`; bypass behavior is explicit. | In [DSL importer tests](../../tests/dsl/test_importer.py), inspect `DslError.code` and `details["issues"]`. In graph tests, check the chosen `skip_validation` behavior without executing the deliberately invalid graph. |
| Valid restored child graphs keep their queued work and identities. | Preserve [engine serialization](../../tests/engine/test_runtime_state_serialization.py), [runtime tests](../../tests/runtime/test_runtime_state.py), and [snapshot version isolation](../../tests/test_snapshot_version_isolation.py); do not regenerate historical fixtures. |

## Decision log

- **2026-09-08 — Proposed:** choose TD-02 first based on reproduced incorrect
  success and input loss. Keep TD-03/TD-04 separate unless deployment evidence
  changes their urgency.
- **2026-09-08 — Proposed:** reject execution cycles using a stdlib check at
  construction, sharing the implementation across config and builder paths.
  Explicit container handlers already provide repetition; scheduler changes and
  a generic graph-analysis framework would expand the task unnecessarily.
- **2026-09-08 — Proposed compatibility:** retain trusted bypasses, reject cycles
  even in inactive scopes, and document stricter default validation for 0.8.0.
  Maintainer review must confirm these public-contract choices and the release
  target before implementation is treated as accepted policy.

## Validation and outcome

On 2026-09-08, Python 3.12.13, the following baseline passed: **93 tests**.

```bash
uv run pytest -n 0 \
  tests/test_repository_docs.py \
  tests/graph/test_graph.py \
  tests/graph/test_graph_validation.py \
  tests/graph/test_graph_scoping.py \
  tests/dsl/test_importer.py \
  tests/dsl/test_node_factory.py
```

Additional local probes confirmed duplicate-ID collapse, edge loss for null
handles and non-string endpoints, builder cycle acceptance, and successful root
construction despite a descendant cycle. The
[public cycle reproduction](../technical-debt.md#validation-record) still produced
`GraphRunSucceededEvent`, `['start']`, and unknown states for `a`/`b`. Selecting
the template node as the DSL root reproduced the slotted-issue `AttributeError`.
Builder probes also accepted an integer node ID and a null source handle.
These are baseline defects, not passing acceptance checks for a fix.

The documentation link/reachability check passed after adding this plan and its
index links. Runtime implementation remains unchanged.

After implementation, rerun the baseline plus the workflow, container, and
snapshot checks linked above, then follow `just test` and `just check` in
[Contributing](../../CONTRIBUTING.md#testing-and-validation). Full-suite and
Python 3.13 validation have not been run for this planning change.

Tracking was checked read-only on 2026-09-08: all 10 open issues and 14 open PRs,
plus open/closed title/body searches for graph validation, cycle, duplicate,
`sourceHandle`, duplicate node, acyclic, cycles, TD-02, and references to #131.
[PR #3](https://github.com/langgenius/graphon/pull/3) already added builder
validation but no cycle rule. [PR #276](https://github.com/langgenius/graphon/pull/276)
merged the debt review only. #131 remains open; no dedicated fix was found in
these targeted searches. [PR #271](https://github.com/langgenius/graphon/pull/271)
and [PR #200](https://github.com/langgenius/graphon/pull/200) remain open work on
filter performance and decision records, respectively, and do not supersede this
plan. Repeat the search when implementation begins; no tracking records were
created or changed during planning.
