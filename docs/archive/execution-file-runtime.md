<!-- knowledge
last_checked: "2026-09-09T20:55:58Z"
-->
# Execution-scoped file runtime

**Status:** archived; completed, including the explicit state ownership follow-up; merged
in [PR #283](https://github.com/langgenius/graphon/pull/283) on 2026-09-09.

**Tracking:** TD-04,
[issue #282](https://github.com/langgenius/graphon/issues/282).
Built on snapshot eligibility in
[PR #280](https://github.com/langgenius/graphon/pull/280).

## Objective and scope

Keep each engine's file URL and byte operations on its own host adapter, including
child frames, resumed execution, and response-stream rendering. File values and
persisted snapshots remain metadata; hosts supply adapters again when rebuilding
engines. The 2026-09-09 follow-up removes process defaults and gives HTTP consumers
and Slim LLMs ownership of their clients and tokenizer caches.

## Original steps (2026-09-08)

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

The [architecture invariant](../../ARCHITECTURE.md#state-and-execution-invariants)
describes execution ownership and host file scopes.

## Validation and outcome

### Initial implementation (2026-09-08)

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

### Follow-up cleanup (2026-09-08)

Removed the redundant file runtime registry and configuration alias while
retaining the set/get/peek/use helpers and process fallback. Removed
`RuntimeState.execution_context` and its worker plumbing; host node-task scopes
use [`Layer.node_run_context`](../../src/graphon/engine/layer/README.md).
File scopes and filter metadata remain separate.

All 79 relevant tests passed before and after the cleanup. `just test` passed all
830 tests in 7.82 seconds, and `just check` passed. Independent test, code, and
knowledge reviews and the documentation check are complete. The final naming pass
required no renames. The earlier test counts and naming results above describe
the original implementation.

### Explicit state ownership follow-up (2026-09-09)

Contributor guidance requires
[explicit state ownership](../../CONTRIBUTING.md#explicit-state-ownership) to
avoid hidden mutable dependencies that complicate debugging. This supersedes
the earlier decision to retain process defaults.

Removed the file setter and process fallback; file resolution now requires an
explicit adapter or scope. Removed the HTTP runtime helpers; each consumer owns
its default client and preserves injected clients, including falsey ones. Each
`SlimLLM` owns a lazy tokenizer cache and initialization lock; dependency-internal
caches are unchanged.

Independent test reviews cover scope restoration, HTTP ownership and injection,
and lazy and concurrent tokenizer initialization. Focused file, HTTP, and Slim LLM
checks passed 39, 133, and 12 tests respectively. `just test` passed all 835 tests
in 7.83 seconds, and `just check` passed. Independent knowledge review corrected
the affected guides and historical status records; the documentation check
passed. The final naming pass clarified tokenizer test/helper names; all 12 Slim
LLM tests passed again after those mechanical renames.
