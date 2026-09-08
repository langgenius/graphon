# AGENTS

All agents working in this repository must follow the guidance in
[CONTRIBUTING.md](CONTRIBUTING.md). If there is any ambiguity, treat
`CONTRIBUTING.md` as the source of truth.

## Where to start

- [README.md](README.md): project overview and runnable examples.
- [ARCHITECTURE.md](ARCHITECTURE.md): execution flow, boundaries, and invariants.
- [Repository knowledge](docs/README.md): index of focused guides and evidence.
- [Development guide](docs/development.md): find the source and tests for a task.
- [Knowledge maintenance](docs/maintenance.md): keep docs, plans, and decisions useful.
- [MIGRATION.md](MIGRATION.md): public API changes and snapshot compatibility.

## Working with repository knowledge

Before changing behavior, read the relevant architecture section and follow its
links to the implementation and tests. Inspect callers and integration boundaries
before choosing where to make a change.

Treat architecture descriptions as a map of the current implementation. Do not
turn observed behavior or an open proposal into an accepted design decision.
When code and docs disagree, investigate and update the stale material in the
same change; contributor policy remains governed by `CONTRIBUTING.md`.

Keep this file short. Put durable knowledge in a focused document, link it from
the index, and update affected source/test links when files move. For work that
spans sessions or needs a decision log, follow the plan guidance in
[Knowledge maintenance](docs/maintenance.md).
