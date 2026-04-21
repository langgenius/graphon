# Graphon

Graphon is a Python graph execution engine for agentic AI workflows.

The repository is still evolving, but it already contains a working execution
engine, built-in workflow nodes, model runtime abstractions, integration
protocols, and a runnable end-to-end example.

## Highlights

- Queue-based `GraphEngine` orchestration with event-driven execution
- Graph parsing, validation, and fluent graph building
- Shared runtime state, variable pool, and workflow execution domain models
- Built-in node implementations for common workflow patterns
- Pluggable model runtime interfaces, including a local `SlimRuntime`
- HTTP, file, tool, and human-input integration protocols
- Extensible engine layers and external command channels

Repository modules currently cover node types such as `start`, `end`, `answer`,
`llm`, `if-else`, `code`, `template-transform`, `question-classifier`,
`http-request`, `tool`, `variable-aggregator`, `variable-assigner`, `loop`,
`iteration`, `parameter-extractor`, `document-extractor`, `list-operator`, and
`human-input`.

## Quick Start

Graphon is currently easiest to evaluate from a source checkout.

### Requirements

- Python 3.12 or 3.13
- [`uv`](https://docs.astral.sh/uv/)
- `make`

Python 3.14 is currently unsupported because `unstructured`, which backs part
of the document extraction stack, currently declares `Requires-Python: <3.14`.

### Set up the repository

```bash
make dev
source .venv/bin/activate
make test
```

`make dev` installs the project, syncs development dependencies, and sets up
[`prek`](https://prek.j178.dev/) Git hooks. `make test` is the progressive
local validation entrypoint: it formats, applies lint fixes, runs `ty check`,
and then runs `pytest`.

## Run the Example Workflow

The repository includes a minimal runnable example at
[`examples/graphon_openai_slim`](examples/graphon_openai_slim).

It builds and executes this workflow:

```text
start -> llm -> output
```

To run it:

```bash
make dev
source .venv/bin/activate
cd examples/graphon_openai_slim
cp .env.example .env
python3 workflow.py "Explain Graphon in one short sentence."
```

Before running the example, fill in the required values in `.env`.

The example currently expects:

- an `OPENAI_API_KEY`
- a `SLIM_PLUGIN_ID`
- a local `dify-plugin-daemon-slim` setup or equivalent Slim runtime

For the exact environment variables and runtime notes, see
[examples/graphon_openai_slim/README.md](examples/graphon_openai_slim/README.md).

## How Graphon Fits Together

At a high level, Graphon usage looks like this:

1. Build or load a graph and instantiate nodes into a `Graph`.
2. Prepare `GraphRuntimeState` and seed the `VariablePool`.
3. Configure model, file, HTTP, tool, or human-input adapters as needed.
4. Run `GraphEngine` and consume emitted graph events.
5. Read final outputs from runtime state.

The bundled example follows exactly that path. The execution loop is centered
around `GraphEngine.run()`:

```python
engine = GraphEngine(
    workflow_id="example-start-llm-output",
    graph=graph,
    graph_runtime_state=graph_runtime_state,
    command_channel=InMemoryChannel(),
)

for event in engine.run():
    ...
```

See
[examples/graphon_openai_slim/workflow.py](examples/graphon_openai_slim/workflow.py)
for the full example, including `SlimRuntime`, `SlimPreparedLLM`, graph
construction, input seeding, and streamed output handling.

## Project Layout

- `src/graphon/graph`: graph structures, parsing, validation, and builders
- `src/graphon/graph_engine`: orchestration, workers, command channels, and
  layers
- `src/graphon/runtime`: runtime state, read-only wrappers, and variable pool
- `src/graphon/nodes`: built-in workflow node implementations
- `src/graphon/model_runtime`: provider/model abstractions and Slim runtime
- `src/graphon/graph_events`: event models emitted during execution
- `src/graphon/http`: HTTP client abstractions and default implementation
- `src/graphon/file`: workflow file models and file runtime helpers
- `src/graphon/protocols`: public protocol re-exports for integrations
- `examples/`: runnable examples
- `tests/`: unit and integration-style coverage

## Internal Docs

- [CONTRIBUTING.md](CONTRIBUTING.md): contributor workflow, CI, commit/PR rules
- [examples/graphon_openai_slim/README.md](examples/graphon_openai_slim/README.md):
  runnable example setup
- [src/graphon/model_runtime/README.md](src/graphon/model_runtime/README.md):
  model runtime overview
- [src/graphon/graph_engine/layers/README.md](src/graphon/graph_engine/layers/README.md):
  engine layer extension points
- [src/graphon/graph_engine/command_channels/README.md](src/graphon/graph_engine/command_channels/README.md):
  local and distributed command channels

## Development

Contributor setup, tooling details, CLA notes, and commit/PR conventions live
in [CONTRIBUTING.md](CONTRIBUTING.md).

CI currently validates pull request titles, runs `make check` including
`uv.lock` freshness validation, and runs `uv run pytest` on Python 3.12 and
3.13. Python 3.14 is currently excluded because `unstructured` does not yet
support it.

## License

Apache-2.0. See [LICENSE](LICENSE).
