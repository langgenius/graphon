<!-- knowledge
last_checked: "2026-09-10T01:03:07Z"
-->
# Contributing to Graphon

This guide reflects the repository's current local tooling and GitHub Actions
checks.

By default, use `just` for routine development. Direct
[`uv`](https://docs.astral.sh/uv/), [`ruff`](https://docs.astral.sh/ruff/),
[`pytest`](https://docs.pytest.org/), and [`prek`](https://prek.j178.dev/)
usage is still fine when you need a targeted command.

## Development Style

Read and follow this section at the start of every development workflow,
including work performed by agents.

### Tests first

Follow test-driven development for behavior changes and bug fixes:

1. Write or update the smallest behavior-focused test that expresses the
   requirement or reproduces the bug before writing the implementation.
2. Run the test and confirm it fails for the intended missing or incorrect
   behavior. Unrelated setup or fixture failures do not establish this. If the
   test already passes, investigate whether the behavior already exists or the
   test misses the problem before changing the implementation.
3. Delegate review of the tests and their observed failure to a
   [fresh, context-free subagent](#context-free-reviews). Resolve its findings
   before implementation.
4. Write the minimum implementation needed to make the test pass, then run it
   again to confirm the result.
5. Refactor with the tests kept green, then run the relevant broader checks from
   [Testing and Validation](#testing-and-validation).

Choose tests around observable behavior and meaningful scenarios. Avoid overly
fine-grained tests for every helper or private method. Assert only what the
requirement depends on; do not assume internal call order, private structure, or
unrelated outputs. Keep fixtures, mocks, and input assumptions limited to what
the scenario needs, and ground expected results in the intended behavior.

### Concrete names

Use simple, intuitive, easily understood words for functions, methods, files,
classes, and variables. Name functions and methods for what they do, files and
classes for what they contain or represent, and variables for the values they
hold. Avoid names built around abstract concepts or generic design-pattern
labels that hide the actual action or data.

Prefer concrete names such as `load_graph`, `Graph`, `node_ids`, and `graph.py`.
Keep names concise without obscure abbreviations or loss of meaning.

### Explicit state ownership

Avoid mutable global variables. Keep runtime data, integration adapters, and
caches on the engine, node, or host object that owns their lifetime, and pass
dependencies explicitly. Do not hide shared mutable state in a singleton,
registry, class variable, or closure: that still makes behavior depend on
unrelated calls and complicates debugging.

Module constants and fixed `ContextVar` keys are fine. Context-local values must
be bound explicitly and restored on exit, including failure; they must not fall
back to a mutable process default. Prefer constructor arguments when the caller
can pass the dependency directly.

### Context-free reviews

Delegate test review, knowledge review, and the final naming pass to separate,
newly spawned subagents. Start each with no inherited conversation history
(`fork_turns="none"` when using `spawn_agent`); do not reuse an implementation
agent or an earlier reviewer. Supply only the task requirements, repository
location, files or diff to review, repository rules, and relevant test
commands/results. Do not pass the parent agent's plan, implementation narrative,
or previous review conclusions. The subagent should inspect repository source,
tests, and callers independently.

- **Test review:** the subagent checks that tests express the requirements, fail
  for the intended reason, use meaningful behavioral scenarios, and avoid
  excessive granularity or unsupported assumptions. Resolve findings and have
  the revised tests reviewed by a new context-free subagent. Later changes to
  test behavior or assertions also require a fresh review; mechanical reference
  updates during renaming only require verification.
- **Knowledge review:** after each development cycle's implementation and
  ordinary checks, an independent subagent reviews and updates the knowledge
  relevant exclusively to the current revision. It must correct outdated
  information, remove redundancy, and shorten overly detailed material where
  needed, applying the edits itself. Follow the
  [scope and validation guidance](docs/maintenance.md#review-for-drift).
  Complete this review before the final naming pass.
- **Final naming pass:** after implementation, refactoring, test and knowledge
  reviews, all other reviews and fixes, documentation, and ordinary validation
  are complete, spawn a new context-free naming subagent. It must identify and
  rename non-compliant function, method, file, class, and variable names
  introduced or changed by the task. Reporting suggestions alone does not complete
  the pass.
  Update affected callers, imports, tests, and documentation as part of each
  rename, preserving behavior and following repository compatibility rules.
  Keep renames within the task's scope.

After the naming subagent finishes, rerun the checks affected by its renames and
inspect the diff before handoff. If further substantive development is needed,
complete it and repeat the knowledge review followed by the final naming pass,
each with a new context-free subagent.

## Development Setup

### Requirements

- Python compatible with [project metadata](pyproject.toml)
- [`uv`](https://docs.astral.sh/uv/)
- [`just`](https://just.systems/)
- [`fd`](https://github.com/sharkdp/fd)
- `git`

### Bootstrap

```bash
just dev
# optional for interactive work
source .venv/bin/activate
```

The [justfile](justfile) defines setup and development commands;
[prek.toml](prek.toml) defines the installed Git hooks. Use direct tools when you
need a targeted command, as in the [development guide](docs/development.md#validate-the-behavior-you-changed).

## Testing and Validation

For normal development, run:

```bash
just test
just check
```

Review the diff afterward for tool-applied fixes. Use `just` to list
recipes; their behavior is defined in the [justfile](justfile). Test defaults and
tool configuration live in [pyproject.toml](pyproject.toml).

### CI Checks

The [PR workflow](.github/workflows/pr.yml) is authoritative for automated PR
requirements; the reusable [test workflow](.github/workflows/test.yml) defines
the Python matrix. Local checks help catch failures before pushing; review CI
results for checks that depend on GitHub events or other Python versions.

## Repository Knowledge

Use the [knowledge index](docs/README.md) to find architecture, component guides,
and the tests relevant to a change. Keep affected documentation and source/test
links current in the same change. See [Knowledge maintenance](docs/maintenance.md)
for document placement, review, and plans for work that spans sessions.

For documentation-specific checks and review guidance, follow
[knowledge validation](docs/maintenance.md#validation).

## Git Commits

Use [Conventional Commits](https://www.conventionalcommits.org/) so the squash
merge history describes each change. The PR title becomes the squash commit
message; the [title check](.github/workflows/pr.yml) defines accepted types.

## Issues

Before you start implementation or open a new issue, search the existing open
and closed issues and pull requests to confirm the work is not already tracked
or in progress.

Rules:

- do not open duplicate issues or parallel pull requests for the same change
- if related work already exists, continue that discussion instead of starting a
  new thread
- if no issue exists for the change, create one before opening a pull request
- if GitHub presents an issue template or issue form, fill out every required
  field and keep the provided structure intact

## Pull Requests

Follow the [PR template](.github/pull_request_template.md) when preparing the
body. The [body check](.github/scripts/check_pr.py) enforces its structure and
issue reference. Use the [commit guidance](#git-commits) for the title.

Before opening a PR, follow the [issue search](#issues),
[validation](#testing-and-validation), and
[development review workflow](#development-style). Keep the change focused and
reviewable, and update affected knowledge. If a template section does not apply,
say so explicitly.

If CLA Assistant prompts you, follow [CLA guidance](#cla).

## CLA

Read [CLA.md](CLA.md) before signing. Follow the signing instructions posted by
[CLA Assistant](.github/workflows/cla.yml) in the PR conversation.

## Maintainer Notes

Version updates are managed manually with [`uv`](https://docs.astral.sh/uv/)
`version`:

```bash
uv version --no-sync --bump patch
uv version --no-sync --bump minor
uv version --no-sync --bump major
```

Review the package metadata and lockfile changes together before merging the
version bump. Tag the merged version when ready to release; the
[release workflow](.github/workflows/release.yml) owns tag validation, build and
publication order. Inspect it and the referenced GitHub environment settings
before changing release operations.

The CLA signature storage configuration and its operational requirements live in
the [CLA workflow](.github/workflows/cla.yml).
