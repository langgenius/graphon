<!-- knowledge
last_checked: "2026-09-08T00:00:00Z"
-->
# Snapshot eligibility (TD-03)

**Status:** implementation and reviews complete locally, 2026-09-08.
**Tracking:** [issue #279](https://github.com/langgenius/graphon/issues/279),
[TD-03](../technical-debt.md#td-03--snapshot-consistency-at-the-public-boundary).
**Branch:** `laipz8200/snapshot-eligibility`, stacked on
[PR #278](https://github.com/langgenius/graphon/pull/278).

## Objective and scope

Reject public runtime snapshots while engine activity can mutate the execution.
Preserve pre-run, restored, and quiescent paused/completed snapshots and existing
snapshot formats. Apply the same boundary to layer wrappers and child frames.
Internal frame snapshots remain part of engine execution.

## Context and decisions

- [Engine.run](../../src/graphon/engine/engine.py) invokes start hooks before
  starting workers and yields terminal events before teardown. Snapshot callers
  must finish consuming or close the iterator and wait for execution to stop.
- Persisted `started`, `paused`, and `completed` flags do not prove quiescence.
  Pause drains active workers; abort and failure can leave workers running beyond
  the bounded shutdown joins.
- Use the shared [GraphExecution](../../src/graphon/runtime/execution.py) to
  account for live run, dispatcher, and worker activity. Child frames already
  share this object. Keep the guard transient, outside snapshot schemas.
- Exclude execution startup and other snapshot writers for the entire public
  serialization. This protects engine-owned changes, not arbitrary concurrent
  host mutations of exposed runtime objects.
- A quiescent `on_graph_end` can persist state. Active start, node, and event
  callbacks must request a cooperative pause and persist after teardown.
  Timed-out shutdown continues rejecting snapshots until lingering threads exit.
- Keep shutdown timeouts and the existing pause path; adding live checkpoint
  orchestration or changing cancellation behavior is outside this revision.

## Steps

- [x] Add behavior regressions and confirm the intended failures.
- [x] Complete independent test review, then implement the guard.
- [x] Run focused compatibility checks and ordinary repository tests.
- [x] Complete independent knowledge review.
- [x] Complete the final naming pass and affected checks.

## Validation and outcome

`uv run pytest -n 0 tests/engine/test_runtime_state_serialization.py -k test_runtime_snapshot`
failed all six reviewed cases for the intended missing behavior: active callbacks,
paused child frames, and workers/dispatchers surviving iterator closure could
serialize; concurrent startup and snapshot writers overlapped a held snapshot.
Fresh independent test review completed before implementation; all six cases pass
with the guard in place.

Existing [serialization tests](../../tests/engine/test_runtime_state_serialization.py),
[runtime tests](../../tests/runtime/test_runtime_state.py), and
[version isolation](../../tests/test_snapshot_version_isolation.py) cover paused
containers, task preservation, historical snapshots, and current formats.

The focused serialization/runtime/isolation/dispatch/container/layer-context set
passes **100 tests**. `just test` passes **821 tests** on Python 3.12.13. An earlier
run caught an assumption that standalone workers always have a root frame; the
guard now uses the shared execution from any registered frame, preserving empty
and child-only registry controls. An initial full run also hit the existing
gevent subprocess's ten-second timeout; the final full run passed both variants.
`just check` and `git diff --check` pass. Independent knowledge review checked
the guard, lifecycle, child-frame sharing, and unchanged snapshot codecs against
source and tests, corrected the debt register, and consolidated repeated guidance.
`uv run pytest -n 0 tests/test_repository_docs.py` passes (**1 test**) after those edits.
The independent final naming pass renamed the snapshot lock method and test
helpers/variables without changing behavior; all six regression cases pass after
the renames. Publication is tracked through issue #279.
Python 3.13 and live external integrations were not exercised locally.
