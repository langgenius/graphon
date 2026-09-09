<!-- knowledge
last_checked: "2026-09-09T21:56:06Z"
-->
# AGENTS

All agents working in this repository must follow the guidance in
[CONTRIBUTING.md](CONTRIBUTING.md). If there is any ambiguity, treat
`CONTRIBUTING.md` as the source of truth.

At the start of every development workflow, read and follow
[Development Style](CONTRIBUTING.md#development-style).

## Where to start

- [README.md](README.md): project overview and runnable examples.
- [ARCHITECTURE.md](ARCHITECTURE.md): execution flow, boundaries, and invariants.
- [Repository knowledge](docs/README.md): index of focused guides and evidence.
- [Development guide](docs/development.md): find the source and tests for a task.
- [Knowledge maintenance](docs/maintenance.md): keep docs, plans, and decisions useful.

## Working with repository knowledge

Before changing behavior, read the relevant architecture section and follow its
links to the implementation and tests. Inspect callers and integration boundaries
before choosing where to make a change.

Apply the [knowledge maintenance principles](docs/maintenance.md#repository-knowledge-maintenance)
when reading or changing documentation.
