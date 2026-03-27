# Contributing to Graphon

This document covers the recommended workflow for contributing to Graphon.

## Overview

By default, use `make` for routine development commands in this repository.

That is the recommended path because:

- it reflects the repository's intended workflow
- it keeps formatting, linting, and testing commands consistent
- it reduces command drift across contributors and CI

If you know exactly what you are doing, using `ruff`, `pytest`, or `prek`
directly is still your choice. Those tools remain fully available, but they
should be treated as the lower-level interface rather than the default one.

## Getting Started

### Prerequisites

- `uv`
- `make`
- `git`

### Initial Setup

This project uses `uv` for dependency and virtual environment management.

```bash
make dev
source .venv/bin/activate
```

`make dev` will:

- run `uv sync`
- ensure `prek` hooks are installed

After that, the repository will have a local `.venv`, the project will be
installed in editable mode, and Git hooks will be ready.

## Development Workflow

For normal development, the expected flow is:

```bash
make dev
make pre
```

For day-to-day iteration, the usual commands are:

```bash
make format
make lint
make test
```

Before opening a pull request, run:

```bash
make pre
```

## Submitting Changes

When you open a pull request:

- keep the change focused and reviewable
- run `make pre` before you push
- use Conventional Commits for the branch's commit history and the pull request
  title
- call out breaking changes clearly and mark them with `!`
- make sure the pull request title matches the final change being merged

## Common Commands

Use these commands by default:

- `make dev`: run `uv sync` and ensure `prek` is installed
- `make format`: run `ruff format`
- `make lint`: run `ruff check --fix`
- `make check`: run `ruff format --check && ruff check`
- `make test`: run `uv run --frozen pytest`
- `make build`: build the package distributions
- `make pre`: run `format`, `lint`, and `test` in sequence
- `make clean`: remove build artifacts, caches, and `__pycache__`

## Commit and Pull Request Conventions

All commit messages must follow the
[Conventional Commits](https://www.conventionalcommits.org/) specification.

This is a repository requirement, not a suggestion.

The required commit types are:

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

Use the Conventional Commits structure consistently, for example:

```text
feat: add graph snapshot export
fix(runtime): avoid duplicate node completion events
docs(readme): document development workflow
refactor(variable-pool): simplify selector lookup
test(encoders): cover nested dataclass encoding
feat!: drop legacy workflow payload format
refactor(api)!: remove deprecated runtime entrypoint
```

Pull request titles must follow the exact same convention.

If a change is breaking, the commit message and the pull request title must
include `!`.

Examples:

```text
feat!: remove deprecated graph initialization path
fix(parser)!: change selector parsing semantics
```

In other words:

- every commit message should use Conventional Commits
- every pull request title should also use Conventional Commits
- every breaking change must be marked with `!`
- the pull request title must stay aligned with the final change being merged

Do not use free-form pull request titles that diverge from the commit message
standard.

## Tooling Details

### `uv`

`uv` is responsible for environment and dependency management.

Common commands:

```bash
uv sync
uv run pytest
uv run ruff check
uv run prek run -a
```

Use `uv` when you need to:

- create or refresh the local virtual environment
- install project and development dependencies
- run low-level commands without activating `.venv`

### `make`

`make` is the default command surface for this repository.

If you are unsure which command to use, start with `make`.

This is especially true for:

- initial setup
- formatting
- linting
- running the test suite
- running the standard pre-check flow

### `ruff`

`ruff` handles both formatting and linting.

Direct usage is supported, but it is secondary to `make`.

Common direct commands:

```bash
ruff format
ruff check --fix
ruff format --check
ruff check
```

Prefer direct `ruff` usage when:

- you want to target a specific file or rule
- you are debugging a particular lint issue
- you intentionally want something more precise than the `Makefile` wrapper

### `pytest`

`pytest` runs the test suite.

Common direct commands:

```bash
uv run pytest
uv run pytest tests
uv run pytest tests/utils/test_condition_processor.py
uv run pytest -k condition
```

Notes:

- the current default configuration includes `-n auto`
- `testpaths` is set to `tests`
- `make test` is the default wrapper and should be preferred unless you need a
  more specific invocation

### `prek`

`prek` manages Git hooks for this repository. Its configuration is stored in
`prek.toml`.

Common direct commands:

```bash
uv run prek install
uv run prek run -a
uv run prek list
uv run prek validate-config
```

Notes:

- `uv run prek install`: install Git hooks into `.git/hooks/`
- `uv run prek run -a`: run hooks against all files in the repository
- the current configuration mainly includes built-in cleanup and validation
  hooks
- the current configuration also includes local `make format` / `make lint`
  hooks

## Direct Tool Usage

If you know what you are doing, direct tool usage is completely acceptable.

The important distinction is:

- default recommendation: use `make`
- advanced or targeted usage: use `ruff`, `pytest`, or `prek` directly
