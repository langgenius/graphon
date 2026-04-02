# Contributing to Graphon

This guide reflects the repository's current local tooling and GitHub Actions
checks.

By default, use `make` for routine development. Direct
[`uv`](https://docs.astral.sh/uv/), `ruff`, `pytest`, and
[`prek`](https://prek.j178.dev/) usage is still fine when you need a targeted
command.

## Prerequisites

- Python 3.12 or newer
- [`uv`](https://docs.astral.sh/uv/)
- `make`
- `git`

## Initial Setup

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

## Daily Workflow

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

## What CI Checks

Pull requests targeting `main` currently run four kinds of checks:

1. PR title validation with `amannn/action-semantic-pull-request`
2. Commit history validation with `cocogitto check`
3. `make check`
4. `make test` on Python 3.12, 3.13, and 3.14

Keep local workflow aligned with those checks. A green local `make pre` is
useful, but it is not a complete substitute for the exact CI flow because CI
also validates commit messages, PR titles, and a Python version matrix.

## Commit and Pull Request Conventions

This repository enforces
[Conventional Commits](https://www.conventionalcommits.org/) for both commit
messages and pull request titles.

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
- keep the pull request title aligned with the final change being merged
- keep the entire commit history reviewable, because CI validates all commits in
  the pull request

Examples:

```text
feat: add graph snapshot export
fix(runtime): avoid duplicate node completion events
docs(contributing): clarify CI workflow
refactor(api)!: remove deprecated runtime entrypoint
```

## Git Hooks

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

## CLA

If CLA Assistant asks you to sign the repository CLA, read [CLA.md](CLA.md) and
post this exact comment once in the pull request conversation:

```text
I have read the CLA Document and I hereby sign the CLA
```

The CLA workflow is separate from the normal PR checks.

## Maintainer Notes

Version bumps and changelog updates are managed with
[`uv`](https://docs.astral.sh/uv/) `version` and `cog`:

```bash
make bump SEM=patch
make bump SEM=minor
make bump SEM=major
```

Release tags use the `v` prefix and are intended to be created from `main`.

CLA signatures are stored on the dedicated `cla-signatures` branch. Maintainers
must keep that branch available and writable to GitHub Actions.

## Direct Tool Usage

Use `make` by default. For targeted work, direct tool usage is still fine:

```bash
uv run ruff check src/graphon/path.py
uv run pytest tests/path/test_file.py -k keyword
uv run prek run -a
```
