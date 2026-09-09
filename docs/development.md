<!-- knowledge
last_checked: "2026-09-08T00:00:00Z"
-->
# Development navigation

At the start of every development workflow, read and follow
[Development Style](../CONTRIBUTING.md#development-style).

Read [CONTRIBUTING.md](../CONTRIBUTING.md) for setup, validation commands, CI,
and contribution rules. This page connects changes to the code and tests that
define their behavior; it does not replace those rules.

## Find the change surface

| Task | Start in source | Relevant checks |
| --- | --- | --- |
| Change package dependencies or queue ownership | [Import boundaries](../ARCHITECTURE.md#import-boundaries), [runtime queues](../src/graphon/runtime/ready_queue/) | `just imports`, [runtime isolation](../tests/runtime/test_runtime_imports.py), [public contract imports](../tests/test_protocols_exports.py) |
| Parse or validate a graph | [graph](../src/graphon/graph/), [graph config](../src/graphon/entities/graph_config.py) | [graph tests](../tests/graph/), especially validation and scoping |
| Import Dify DSL or wire node dependencies | [importer](../src/graphon/dsl/importer.py), [node factory](../src/graphon/dsl/node_factory.py) | [DSL tests](../tests/dsl/), especially importer, node factory, and app bootstrap |
| Change a built-in node | [nodes](../src/graphon/nodes/), [base node](../src/graphon/nodes/base/node.py) | Matching [node tests](../tests/nodes/); [workflow events](../tests/workflows/test_full_engine_events.py) for graph-visible changes |
| Change dispatch, scheduling, or containers | [engine](../src/graphon/engine/), [container effects](../src/graphon/nodes/container_effects.py) | [engine tests](../tests/engine/), [container dispatch](../tests/nodes/test_container_dispatch.py), [workflow events](../tests/workflows/test_full_engine_events.py) |
| Change event payloads or response streaming | [node events](../src/graphon/node_events/), [engine events](../src/graphon/engine_events/), [filters](../src/graphon/engine/filter/) | [node event tests](../tests/node_events/), [engine event tests](../tests/engine_events/), [response filter tests](../tests/engine/test_response_stream_filter.py) |
| Change variables or secrets | [variable pool](../src/graphon/runtime/variable_pool.py), [variables](../src/graphon/variables/) | [variable pool tests](../tests/runtime/test_variable_pool.py), [variable tests](../tests/variables/), [HTTP secret masking](../tests/http/test_executor_secret_masking.py) |
| Change persistence or resume | [runtime state](../src/graphon/runtime/runtime_state/), [container state](../src/graphon/runtime/container_state.py) | [runtime tests](../tests/runtime/), [engine serialization](../tests/engine/test_runtime_state_serialization.py), [version isolation](../tests/test_snapshot_version_isolation.py) |
| Change model invocation | [graph-facing LLM protocol](../src/graphon/nodes/llm/runtime_protocols.py), [Slim adapter](../src/graphon/dsl/slim/llm.py), [model runtime](../src/graphon/model_runtime/) | [LLM node tests](../tests/nodes/llm/), [Slim tests](../tests/dsl/test_slim_llm.py), [model dispatch](../tests/model_runtime/test_model_dispatch.py) |
| Change file or HTTP integration | [file](../src/graphon/file/), [engine adapter binding](../src/graphon/engine/engine.py), [HTTP](../src/graphon/http/), [node ports](../src/graphon/nodes/protocols.py) | [file tests](../tests/file/), [execution file isolation](../tests/engine/test_file_runtime.py), [HTTP tests](../tests/http/), [HTTP node tests](../tests/nodes/http_request/), [extractor tests](../tests/nodes/document_extractor/) |

## Add or extend a node

1. Read the closest existing implementation and its tests. Write behavior tests
   next to the related node tests and run them to confirm the intended failure
   before implementing the node. If DSL support changes, exercise `loads()` as
   well. For changes to branching, outputs, failure handling, or containers,
   assert the complete engine behavior using the existing
   [workflow tests](../tests/workflows/test_full_engine_events.py).
   Complete the [context-free test review](../CONTRIBUTING.md#context-free-reviews)
   before implementation.
2. Define validated data
   with [BaseNodeData](../src/graphon/entities/base_node_data.py), then subclass
   `Node[YourNodeData]`. Supply `node_type`, a numeric-string `version()`, and
   `_run()`. Existing nodes such as [StartNode](../src/graphon/nodes/start/start_node.py)
   show the result-returning form; streaming nodes emit node event payloads.
3. Import the node class during host bootstrap. Subclasses register when their
   implementation modules load; registry lookup does not discover or import
   packages. Bare Code/LLM package imports do not load their classes; see
   [bootstrap migration](../MIGRATION.md#runtime-queues-and-node-imports).
   Registration alone does not add support to the default DSL importer:
   [SlimDslNodeFactory.NODE_BUILDERS](../src/graphon/dsl/node_factory.py) explicitly
   controls that surface and wires its dependencies.
4. For a custom host factory, follow [NodeFactory](../src/graphon/graph/graph.py).
   `validate_node()` must resolve the same class/version as `create_node()` without
   constructing nodes, initializing services, or mutating execution state.
   `with_runtime_state()` and `with_graph_config()` must keep parent and sibling
   frames isolated. `post_init()` runs in the node constructor, so the scoped
   graph config must already be bound then.
5. Run the tests again to confirm they pass, then refactor with the tests kept
   green. Complete the [validation and final reviews](#validate-the-behavior-you-changed)
   described below.

## Integrate host services

Use the existing [public protocol exports](../src/graphon/protocols/__init__.py)
and the node's constructor instead of importing host infrastructure into the
engine. [Protocol export tests](../tests/test_protocols_exports.py) protect that
public surface.

- **Models:** inject a prepared `LLMProtocol` as `LLMNode.model_instance`.
  [SlimLLM](../src/graphon/dsl/slim/llm.py) is the provided graph-facing adapter;
  [code.py](../examples/slim_llm/code.py) shows direct wiring. Provider discovery
  and capability wrappers have separate protocols; see the
  [model runtime guide](../src/graphon/model_runtime/README.md) and
  [dispatch tests](../tests/model_runtime/test_model_dispatch.py).
- **Files and HTTP:** [WorkflowFileRuntimeProtocol](../src/graphon/file/protocols.py)
  covers storage, URLs, downloads, and preview signatures. Pass it to
  `Engine(file_runtime=adapter)`, or enter an explicit file scope before
  constructing the engine. For DSL construction and host rendering, use
  [use_workflow_file_runtime](../src/graphon/file/runtime.py); the
  [migration example](../MIGRATION.md#file-runtime-isolation) shows both boundaries.
  Use `EngineEventFilterContext.from_engine()` when wiring response filters.
  Node-specific download, file-reference, and tool-file ports live
  in [nodes/protocols.py](../src/graphon/nodes/protocols.py); generated LLM files use
  [LLMFileSaver](../src/graphon/nodes/llm/file_saver.py). Each HTTP consumer accepts
  an injected client or constructs its own [HTTPX client](../src/graphon/http/client.py).
- **Tools and code:** implement [ToolNodeRuntimeProtocol](../src/graphon/nodes/runtime.py)
  or [CodeExecutorProtocol](../src/graphon/nodes/code/protocols.py). Default DSL
  adapters live in [tool_runtime.py](../src/graphon/dsl/tool_runtime.py) and
  [code_runtime.py](../src/graphon/dsl/code_runtime.py); the latter calls a Dify
  sandbox service. Follow the [tool tests](../tests/nodes/tool/) and
  [code runtime tests](../tests/dsl/test_code_runtime.py).
- **Human input:** inject the [HITLCallback](../src/graphon/nodes/human_input/entities.py)
  into [HumanInputNode](../src/graphon/nodes/human_input/human_input_node.py).
  The host returns a pause, completed, or expired decision. See
  [human-input tests](../tests/nodes/human_input/) for the event contract.

## Validate the behavior you changed

Start with the mapped test file, for example:

```bash
uv run pytest tests/dsl/test_node_factory.py -k http_request -n 0
```

`-n 0` disables the repository's default parallel execution for focused debugging.
The defaults are configured in [pyproject.toml](../pyproject.toml). Reuse
[test builders](../tests/helpers/builders.py) for runtime inputs and variable pools;
[workflow helpers](../tests/helpers/workflow_events.py) provide mocked Slim responses,
event paths, and final output assertions. Tests can exercise model flows without
live provider credentials; the runnable [Slim examples](../examples/slim_llm/README.md)
need real credentials and a local or remote daemon.

Before handing off a code change, follow the validation sequence in
[CONTRIBUTING.md](../CONTRIBUTING.md#testing-and-validation). `just test` and
`just tc` apply formatting and lint fixes, so review the diff afterward.
For dependency changes, update [uv.lock](../uv.lock) with the package metadata.
Complete the [knowledge review](maintenance.md#review-for-drift) for the current
revision. Once all other development steps are complete, finish with the
[context-free naming pass and rename verification](../CONTRIBUTING.md#context-free-reviews).

## Runtime pitfalls

- Once execution starts, `RuntimeState.dumps()` requires teardown and all
  execution threads to stop. Start, node, and event callbacks cannot take runtime
  snapshots; quiescent `on_graph_end` hooks can. See the
  [snapshot eligibility guidance](../MIGRATION.md#snapshot-eligibility).
- A direct `Node.run()` call requires `bind_execution_id()` first. The engine
  normally handles this; [the binding test](../tests/nodes/base/test_node_execution_binding.py)
  captures the failure when it is missing.
- Node instance fields are not persisted in runtime snapshots. Store resumable
  data in persisted runtime state or outputs, following the container machinery.
  Preserve historical snapshot fixture bytes in [runtime fixtures](../tests/runtime/fixtures/)
  and [engine fixtures](../tests/engine/fixtures/) when changing current writers;
  those files are compatibility inputs, not regenerated expectations.
- Engines retain their file adapter at construction, including an unconfigured
  state; later scopes do not reconfigure them. Rebind
  adapters when rebuilding from snapshots. Raw event consumers render in their
  own file scope, so bind `engine.file_runtime` when resolving delivered values.
  [Execution file tests](../tests/engine/test_file_runtime.py) cover these boundaries;
  [file runtime tests](../tests/file/test_runtime.py) cover nested scope restoration.
  File scopes restore their caller's binding on exit; HTTP clients belong to
  their consumers. Follow [explicit state ownership](../CONTRIBUTING.md#explicit-state-ownership)
  when adding integrations or caches.
- Built-in node availability and default DSL support differ. Consult
  [the factory](../src/graphon/dsl/node_factory.py) before promising import support.
  Its default file adapters reject unsupported file operations; adding host file
  support also requires wiring the appropriate node dependencies.
- Default graph construction checks input and topology throughout the retained
  subtree before constructing nodes; root type is checked on the resolved node.
  Use acyclic edges inside each container; invalid fields or duplicate IDs
  are rejected instead of being silently discarded. See the
  [graph validation migration notes](../MIGRATION.md#graph-validation) for the
  supported defaults and trusted validation bypasses.

When a change alters these workflows, update this page and the linked source or
tests in the same change. Put user-visible compatibility changes in
[MIGRATION.md](../MIGRATION.md) and [CHANGELOG.md](../CHANGELOG.md) as appropriate.
