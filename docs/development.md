<!-- knowledge
last_checked: "2026-09-09T21:56:06Z"
-->
# Development navigation

At the start of every development workflow, read and follow
[Development Style](../CONTRIBUTING.md#development-style).

Read [CONTRIBUTING.md](../CONTRIBUTING.md) for setup, validation commands, CI,
and contribution rules. This page connects changes to the code and tests that
define their behavior; it does not replace those rules.

## Find the change surface

Choose boundaries around state ownership and callers. Extract a responsibility
when it has independent ownership or repeated churn; file length alone does not
justify splitting a module.

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

1. Follow [tests first](../CONTRIBUTING.md#tests-first), using the closest node's
   tests as a starting point. Exercise `loads()` for DSL changes and
   [workflow tests](../tests/workflows/test_full_engine_events.py) for branching,
   outputs, failure handling, or containers.
2. Use [BaseNodeData](../src/graphon/entities/base_node_data.py) and the
   [Node contract](../src/graphon/nodes/base/node.py). [StartNode](../src/graphon/nodes/start/start_node.py)
   shows a small implementation; streaming nodes use node event payloads.
3. Consult [import boundaries](../ARCHITECTURE.md#import-boundaries) for
   registration behavior and [the DSL factory](../src/graphon/dsl/node_factory.py)
   when extending import support.
4. For a custom host factory, use the
   [factory validation and isolation contract](../ARCHITECTURE.md#state-and-execution-invariants).
   Bind scoped configuration before construction because `post_init()` runs in
   the constructor.
5. Complete [validation](#validate-the-behavior-you-changed).

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
- **Files and HTTP:** use the [file binding guidance](../ARCHITECTURE.md#state-and-execution-invariants)
  when wiring engines, filters, or host rendering. Node-specific ports live in
  [nodes/protocols.py](../src/graphon/nodes/protocols.py); generated LLM files use
  [LLMFileSaver](../src/graphon/nodes/llm/file_saver.py). Client injection behavior
  is covered by [HTTP client tests](../tests/http/test_client.py). Follow
  [state ownership](../CONTRIBUTING.md#explicit-state-ownership) for adapters and caches.
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

See [pytest defaults](../pyproject.toml) when adjusting test execution. Reuse
[test builders](../tests/helpers/builders.py) for runtime inputs and variable pools;
[workflow helpers](../tests/helpers/workflow_events.py) provide mocked Slim responses,
event paths, and final output assertions. For live integration setup, use the
[Slim example guide](../examples/slim_llm/README.md).

Before handoff, follow [validation](../CONTRIBUTING.md#testing-and-validation)
and the [independent review sequence](../CONTRIBUTING.md#context-free-reviews).

## Runtime pitfalls

Consult the [state and execution contracts](../ARCHITECTURE.md#state-and-execution-invariants)
when changing snapshots, file adapters, or graph construction.
For direct node execution, follow the
[execution binding test](../tests/nodes/base/test_node_execution_binding.py).
Use persisted runtime state or outputs for resumable data rather than transient
node attributes. Historical [runtime fixtures](../tests/runtime/fixtures/) and
[engine fixtures](../tests/engine/fixtures/) are compatibility evidence, not
expectations to regenerate when changing a writer; the
[PR file check](../.github/workflows/pr.yml) protects their existing bytes.

Consult [the DSL factory](../src/graphon/dsl/node_factory.py) before promising
import support; host file support also needs the corresponding node dependencies.
