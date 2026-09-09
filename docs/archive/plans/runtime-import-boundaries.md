<!-- knowledge
last_checked: "2026-09-09T20:55:58Z"
-->
# Runtime and public contract imports

**Status:** archived; completed, including the architectural layering follow-up; merged
in [PR #287](https://github.com/langgenius/graphon/pull/287) on 2026-09-09.
**Tracking:** [implementation issue #286](https://github.com/langgenius/graphon/issues/286),
TD-01 and TD-05, originating in
[architecture review #275](https://github.com/langgenius/graphon/issues/275).
Built on knowledge metadata checks in
[PR #285](https://github.com/langgenius/graphon/pull/285).

## Objective and scope

Runtime construction and current snapshot restoration must work without loading
the engine or registering built-in nodes. Public contract imports must leave
registration to explicit node imports. Preserve queue identities at existing
engine paths, custom queue injection, historical snapshots, and explicit Code/LLM
class imports. Use Import Linter to enforce the dependency boundaries in ordinary
local and CI checks.

## Initial implementation steps

- [x] Read architecture, source, callers, and existing tests; search open and
  closed issues and PRs for overlapping work. No matching remediation found.
- [x] Reproduce both import side effects with fresh-process tests and detect
  runtime/engine and facade/Code dependencies with Import Linter.
- [x] Complete independent test review and implementation.
- [x] Run focused tests, forbidden-import probes, and ordinary checks.
- [x] Complete independent knowledge review and correct affected guidance.
- [x] Complete final naming pass and verification.

## Decisions

- Move the existing queue and task definitions under runtime; retain engine-path
  aliases. Queue behavior and persisted formats stay the same.
- Keep the code executor contract with its consumer. Load Code and LLM classes
  only when explicitly requested from their package exports or implementation
  modules. Bare package imports cease to register these nodes.
- Initially use targeted forbidden contracts. The requested layering follow-up
  below replaces them with one exhaustive architectural contract. Shared schemas
  remain legitimate dependencies. Fresh-process tests cover Python package
  initializer effects that static dependency checks do not represent.

## Initial implementation validation and outcome

The results in this plan are historical implementation checks from 2026-09-09.

Before implementation, the two fresh-process tests failed at the intended engine
import and registry assertions; existing public export checks passed. Import
Linter reported two broken boundaries and kept the execution/DSL boundary.

On Python 3.12.13:

- `uv run pytest -n 0 tests/runtime/test_runtime_imports.py tests/runtime/test_runtime_state.py tests/test_snapshot_version_isolation.py`: 32 passed.
- `uv run pytest -n 0 tests/engine tests/runtime tests/test_protocols_exports.py tests/test_snapshot_version_isolation.py tests/dsl/test_app_bootstrap.py`: 181 passed.
- `just test`: 861 passed. An earlier run found a stale moved-file link (fixed)
  and a gevent late-patching subprocess timeout; both the focused engine run and
  full rerun passed without changing the gevent test or implementation.
- `just check`: passed, including all three import contracts.
- Temporary source copies passed the real Import Linter configuration, rejected
  runtime/engine imports under `TYPE_CHECKING`, an indirect engine/helper/Slim
  path, and both Code and LLM implementation imports from public contracts. Each
  failure named its source and repair guidance; removing the probes restored
  all three contracts. No probe imports were left in source.

Independent knowledge review clarified the exact contract scope and explicit
class bootstrap, removed stale dependency-gap wording and a duplicate diagram
edge, and aligned the recorded review status.
`uv run pytest -n 0 tests/test_repository_docs.py`: 25 passed after these edits.

The final naming pass renamed the runtime import test to distinguish loading the
engine from registering nodes, and `loaded_engine` to `loaded_engine_modules`.
No production names required changes. The runtime import, public protocol, and
documentation checks passed together: 29 tests. `just check` and
`git diff --check` passed after the naming edits.

Python 3.13 and live external integrations were not exercised locally.

## Architectural layering follow-up

**Requirement:** replace case-specific import rules with general architectural
layering. The six ordered layers are maintained in
[architecture](../../../ARCHITECTURE.md#import-boundaries); their package groups
reflect existing schema and runtime dependencies. The exhaustive contract covers
all top-level modules, including future additions, and rejects upward imports
through descendants and type-checking guards. The existing import-side-effect
checks retain responsibility for runtime isolation and node registration.

- [x] Map source dependencies and confirm the proposed layers accept the checkout.
- [x] Add behavior checks; observe the old rules wrongly accept a file/runtime
  dependency and an unclassified module, while still rejecting runtime/engine.
- [x] Complete fresh independent test review and replace the three forbidden rules.
- [x] Complete ordinary checks.
- [x] Complete independent knowledge review.
- [x] Complete final naming and verification.

On Python 3.12.13, the layer, runtime import, and public protocol checks passed
together (7 tests). `just test` passed all 864 tests, and `just check` passed with
one architectural layers contract. Knowledge review clarified the static
contract's registration limit and distinguished initial completion from this
follow-up. Final naming made test directory paths, package names, and synthetic
modules explicit and clarified the contract's display name. The 3 layer tests
and `just check` passed after these mechanical renames.
