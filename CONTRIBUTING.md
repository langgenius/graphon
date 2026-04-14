# Contributing to Graphon

This guide reflects the repository's current local tooling and GitHub Actions
checks.

By default, use `make` for routine development. Direct
[`uv`](https://docs.astral.sh/uv/), `ruff`, `pytest`, and
[`prek`](https://prek.j178.dev/) usage is still fine when you need a targeted
command.

## Development Setup

### Requirements

- Python 3.12 or 3.13
- [`uv`](https://docs.astral.sh/uv/)
- `make`
- `git`

Python 3.14 is currently unsupported because `unstructured`, which is used by
the document extraction node, currently declares `Requires-Python: <3.14`.

### Bootstrap

```bash
make dev
# optional for interactive work
source .venv/bin/activate
```

`make dev` will:

- run `uv sync`
- install [`prek`](https://prek.j178.dev/) Git hooks

The repository uses [`uv`](https://docs.astral.sh/uv/) for dependency and
virtual environment management. The default development environment includes
`ruff`, `pytest`, `pytest-xdist`, `pytest-cov`, `pytest-mock`, and
[`prek`](https://prek.j178.dev/).

### Git Hooks

`make dev` installs [`prek`](https://prek.j178.dev/) hooks from
[`prek.toml`](prek.toml).

The current hook set includes:

- trailing whitespace and end-of-file cleanup
- BOM cleanup and line ending normalization
- TOML and YAML validation
- shebang executable checks
- local `make format`
- local `make lint`

Useful direct commands:

```bash
uv run prek install
uv run prek run -a
uv run prek list
uv run prek validate-config
```

Use `make` by default. For targeted work, direct tool usage is still fine:

```bash
uv run ruff check src/graphon/path.py
uv run pytest tests/path/test_file.py -k keyword
uv run prek run -a
```

## Testing and Validation

Use these commands for normal development:

- `make format`: run `uv run ruff format`
- `make lint`: run `uv run ruff check --fix`
- `make check`: run `uv run ruff format --check && uv run ruff check`
- `make test`: run `uv run --frozen pytest`
- `make pre`: run `format`, `lint`, and `test`
- `make build`: build the package distributions
- `make clean`: remove build artifacts and caches

Notes:

- `make lint` is mutating. It may rewrite files.
- `make check` is the non-mutating style and lint check used in CI.
- `pytest` is configured with `-n auto` and `testpaths = ['tests']`, so the
  test suite runs in parallel by default.
- `make test` uses `--frozen`. If you change dependencies, refresh and commit
  `uv.lock` before opening a pull request.

For most changes, a good local sequence is:

```bash
make pre
make check
```

`make pre` applies local fixes and runs the test suite. `make check` then
confirms the non-mutating CI check job will pass.

### CI Checks

Pull requests targeting `main` currently run three kinds of checks:

1. PR title validation with `amannn/action-semantic-pull-request`
2. `make check`
3. `make test` on Python 3.12 and 3.13

Keep local workflow aligned with those checks. A green local `make pre` is
useful, but it is not a complete substitute for the exact CI flow because CI
also validates PR titles and a Python version matrix.

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
- run `make pre` before pushing and keep `make check` green locally when
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
- add or update tests for behavior changes unless the change genuinely does not
  require them
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
2. runs tests and builds release distributions once
3. creates or updates a GitHub draft release and publishes the same build
   artifacts to TestPyPI
4. waits for approval on the `pypi` environment
5. publishes the same build artifacts to PyPI and publishes the GitHub draft
   release

CLA signatures are stored on the dedicated `cla-signatures` branch. Maintainers
must keep that branch available and writable to GitHub Actions.
