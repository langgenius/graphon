<!-- knowledge
last_checked: "2026-09-09T21:56:06Z"
-->
# Graphon

Graphon is a Python graph execution engine for agentic AI workflows.

The repository is still evolving, but it already contains a working execution
engine, built-in workflow nodes, model runtime abstractions, integration
protocols, and a runnable end-to-end example.

## Highlights

- Queue-based `Engine` orchestration with event-driven execution
- Graph parsing, validation, and fluent graph building
- Shared runtime state, variable pool, and workflow execution state
- Built-in node implementations for common workflow patterns
- DSL import support with Slim-backed LLM nodes
- HTTP, file, tool, and human-input integration protocols
- Extensible engine layers and external command channels

## Get started

Follow [development setup](CONTRIBUTING.md#development-setup), then run the
[Slim LLM example](examples/slim_llm/README.md). It includes both DSL import and
direct Python graph construction.

## Find the implementation

- [Architecture](ARCHITECTURE.md): public entry points, execution flow, and state ownership.
- [Development guide](docs/development.md): change surfaces and host integrations.
- [Repository knowledge](docs/README.md): component guides and maintenance.
- [Contributing](CONTRIBUTING.md): setup, validation, and contribution workflow.

## License

Apache-2.0. See [LICENSE](LICENSE).
