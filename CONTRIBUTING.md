<!-- knowledge
last_checked: "2026-09-08T17:06:45Z"
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

- Python 3.12 or 3.13
- [`uv`](https://docs.astral.sh/uv/)
- [`just`](https://just.systems/)
- [`fd`](https://github.com/sharkdp/fd)
- `git`

Python 3.14 is currently unsupported because `unstructured`, which is used by
the document extraction node, currently declares `Requires-Python: <3.14`.

### Bootstrap

```bash
just dev
# optional for interactive work
source .venv/bin/activate
```

`just dev` will:

- run `uv sync`
- install [`prek`](https://prek.j178.dev/) Git hooks

The repository uses [`uv`](https://docs.astral.sh/uv/) for dependency and
virtual environment management. The default development environment includes
[`ruff`](https://docs.astral.sh/ruff/), [`pytest`](https://docs.pytest.org/),
`pytest-xdist`, `pytest-cov`, `pytest-mock`, and
[`prek`](https://prek.j178.dev/).

### Git Hooks

`just dev` installs [`prek`](https://prek.j178.dev/) hooks from
[`prek.toml`](prek.toml).

The current hook set includes:

- trailing whitespace and end-of-file cleanup
- BOM cleanup and line ending normalization
- TOML and YAML validation
- shebang executable checks
- local `just tc` (which runs `format`, `lint`, and `ty check` in sequence)

Useful direct commands:

```bash
uv run prek install
uv run prek run -a
uv run prek list
uv run prek validate-config prek.toml
```

Use `just` by default. For targeted work, direct tool usage is still fine:

```bash
uv run ruff check src/graphon/path.py
uv run pytest tests/path/test_file.py -k keyword
uv run prek run -a
```

## Testing and Validation

Use these commands for normal development:

- `just`: list available recipes
- `just format`: run `uv run ruff format`
- `just lint`: run `just format`, then `uv run ruff check --fix`
- `just tc`: run `just lint`, then `uv run ty check`
- `just check`: run `uv lock --check && uv run ruff format --check && uv run ruff check && uv run ty check`
- `just test`: run `just tc`, then `uv run pytest`
- `just build`: build the package distributions
- `just clean`: remove build artifacts and caches

Notes:

- `just lint` is mutating. It may rewrite files.
- `just tc` is the local type-check entrypoint used by Git hooks. It includes
  formatting and lint fixes first.
- `just test` is the progressive local full-chain target. It formats, applies
  lint fixes, runs `ty check`, and then runs the test suite.
- `just check` aggregates the same non-mutating lockfile, lint, and type-check
  commands used by CI.
- [`pytest`](https://docs.pytest.org/) is configured with `-n auto` and
  `testpaths = ['tests']`, so the test suite runs in parallel by default.
- If you change dependencies, refresh and commit `uv.lock` before opening a
  pull request.

For most changes, a good local sequence is:

```bash
just test
just check
```

`just test` applies local fixes, runs `ty check`, and then runs the test suite.
`just check` then confirms the non-mutating CI check job will pass.

### CI Checks

Pull requests targeting `main` currently run three kinds of checks:

1. PR title validation with `amannn/action-semantic-pull-request`
2. `just check` including `uv.lock` freshness validation
3. `uv run pytest` on Python 3.12 and 3.13

Keep local workflow aligned with those checks. A green local `just test` plus
`just check` is useful, but it is not a complete substitute for the exact CI
flow because CI also validates PR titles and a Python version matrix.

## Repository Knowledge

Use the [knowledge index](docs/README.md) to find architecture, component guides,
and the tests relevant to a change. Keep affected documentation and source/test
links current in the same change. See [Knowledge maintenance](docs/maintenance.md)
for document placement, review, and plans for work that spans sessions.

The normal pytest suite checks local documentation links, reachability, and
review metadata, failing if the oldest review is more than seven days old.
To run these checks alone:

```bash
uv run pytest -n 0 tests/test_repository_docs.py
```

These checks verify navigation and recorded review age. Review the page's claims
before updating its `last_checked` timestamp; see the
[metadata format and scope](docs/maintenance.md#document-metadata-and-review-deadline).

## Git Commits

This repository enforces
[Conventional Commits](https://www.conventionalcommits.org/) for commit
messages. The same format is required for pull request titles.

The PR title validator currently accepts these types:

- `feat`
- `fix`
- `docs`
- `style`
- `refactor`
- `perf`
- `test`
- `build`
- `ci`
- `chore`
- `revert`

Rules:

- use an optional scope when it improves clarity
- mark breaking changes with `!`
- remember that the pull request title becomes the squash merge commit message

Examples:

```text
feat: add graph snapshot export
fix(runtime): avoid duplicate node completion events
docs(contributing): clarify CI workflow
refactor(api)!: remove deprecated runtime entrypoint
```

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

Every pull request must be linked to an issue. Use a closing or reference
keyword such as `Closes #123`, `Fixes #123`, or `Refs #123` in the pull request
body.

Before you open a pull request:

- search existing pull requests again to confirm there is no duplicate review in
  progress
- make sure the change stays focused and reviewable
- run `just test` before pushing and keep `just check` green locally when
  possible

When you open a pull request:

- use a Conventional Commits title, and mark breaking changes with `!`, because
  the pull request title becomes the squash merge commit message
- link the related issue in the pull request body
- follow [`.github/pull_request_template.md`](.github/pull_request_template.md)
  exactly
- do not delete required headings or checklist items from the template; if a
  section is not applicable, say so explicitly
- if CLA Assistant prompts you, sign [CLA.md](CLA.md) in the pull request
  conversation before merge
- follow [Development Style](#development-style) for tests, independent reviews,
  and the final naming pass
- update contributor-facing or user-facing documentation when needed

## CLA

If CLA Assistant asks you to sign the repository CLA, read [CLA.md](CLA.md) and
post this exact comment once in the pull request conversation:

```text
I have read the CLA Document and I hereby sign the CLA
```

The CLA workflow is separate from the normal PR checks.

## Maintainer Notes

Version updates are managed manually with [`uv`](https://docs.astral.sh/uv/)
`version`:

```bash
uv version --no-sync --bump patch
uv version --no-sync --bump minor
uv version --no-sync --bump major
```

Those commands update the package version in `pyproject.toml`. If the lock file
also needs to reflect the new root package version, refresh and commit
`uv.lock` as part of the version bump change. The version update step does not
create tags, releases, or changelog entries.

Release tags use the `v` prefix and are intended to be created from `main`
after the version bump pull request has been merged. The pushed tag must match
`[project].version` in `pyproject.toml`.

Pushing `vX.Y.Z` triggers the release workflow. It:

1. verifies the tag matches `pyproject.toml` and points to a commit reachable
   from `main`
2. runs tests before building release distributions
3. creates or updates a GitHub draft release and publishes the same build
   artifacts to TestPyPI
4. waits for approval on the `pypi` environment
5. publishes the same build artifacts to PyPI and publishes the GitHub draft
   release

CLA signatures are stored on the dedicated `cla-signatures` branch. Maintainers
must keep that branch available and writable to GitHub Actions.
