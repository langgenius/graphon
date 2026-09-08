# Execution-scoped file runtime

**Status:** completed locally; implementation, independent reviews, final naming
pass, and checks are complete. Not merged or released. Updated 2026-09-08.

**Tracking:** [TD-04](../technical-debt.md#td-04--execution-scoped-file-integration),
[issue #282](https://github.com/langgenius/graphon/issues/282).
This branch builds on snapshot eligibility in
[PR #280](https://github.com/langgenius/graphon/pull/280).

## Objective and scope

Keep each engine's file URL and byte operations on its own host adapter, including
child frames, resumed execution, and response-stream rendering. Preserve the
process default for existing integrations. File values and persisted snapshots
remain metadata; hosts supply adapters again when rebuilding engines.

## Steps

- [x] Inspect the file helpers, engine threads, response filter, and callers;
  search existing issues and PRs.
- [x] Add behavioral regressions, observe failures, and complete independent test
  review before implementation.
- [x] Retain the adapter on Engine and bind it at execution and consumer boundaries.
- [x] Complete the response-filter callback scope correction and independent test
  review.
- [x] Run focused and broader checks on the final implementation.
- [x] Complete independent knowledge review.
- [x] Complete the final naming pass and verify any renames.

## Decisions

- Reuse `WorkflowFileRuntimeProtocol` and the existing helpers. A scoped context
  avoids threading arguments through every file property and segment renderer.
- Bind explicitly in workers and the dispatcher; a caller's ContextVar does not
  automatically propagate into execution threads.
- Scope generator advancement and closing without keeping a binding across an
  event yield. Response filters carry the adapter in their existing context.
- Host consumers enter the file context when rendering raw file values after
  delivery. Do not attach adapters to shared File objects or persist credentials.
- Existing injected HTTP clients and node-specific file ports keep their roles.

The current API and host integration steps live in the
[migration guide](../../MIGRATION.md#file-runtime-isolation); the
[architecture invariant](../../ARCHITECTURE.md#state-and-execution-invariants)
describes execution ownership.

## Validation and outcome

Before implementation, `just check` passed. A public `dsl.loads()` probe created
an engine with host A, changed the default to host B, and observed host B's URL
in the answer.

The focused regression command is:

```bash
uv run pytest -n 0 tests/engine/test_file_runtime.py tests/file/test_runtime.py
```

Its final pre-implementation run produced nine failures and five passes: engine
outputs, child callbacks, lifecycle callbacks, and response filters used the
caller's adapter; an unconfigured engine adopted a later default, and the scoped
runtime API was absent. The independent test review approved these scenarios.
The new
[engine regressions](../../tests/engine/test_file_runtime.py) exercise the shared
file helpers through execution and rendering. Existing
[file model tests](../../tests/file/test_models.py) and
[snapshot tests](../../tests/engine/test_runtime_state_serialization.py) retain
historical compatibility coverage.

The initial implementation passed all 14 focused tests. Review then identified
streaming-template callbacks during response-filter initialization and loading
that still used the caller's adapter. Four added success/failure cases failed
before that correction while the original 14 passed; a fresh independent test
review approved them. The corrected implementation passes all 18 focused tests.

The final `just test` passed all 834 tests in 9.00 seconds on Python 3.12.13, and
`just check` passed. An earlier full run passed 829 tests and hit the event-stream
gevent subprocess's 10-second timeout; its three tests passed in isolation and
the next full run passed all 830 tests before the four callback cases were added.
Python 3.13 and live host adapters were not exercised locally.

The final naming pass renamed the test factories to `_make_file_adapter`,
`_make_file`, and `_make_file_engine`, and the template callback to `get_template`.
These mechanical changes preserve behavior and public names. After renaming,
the focused command passed all 18 tests in 1.01 seconds and `just check` passed.
The documentation check passed after the completion records were updated.
