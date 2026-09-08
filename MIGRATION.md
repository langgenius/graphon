# Migration guide

## 0.7.x to 0.8.0 (Unreleased)

Graphon 0.8.0 intentionally removes the old names instead of carrying aliases.
Upgrade Graphon and its importing application together. If paused executions or
response-filter state are persisted, retain the graph definition used by each run
until its state has been restored and serialized by 0.8.0.

### Upgrade order

1. Update imports, engine construction, event consumers, and extension protocols.
2. Deploy code that reads the persisted versions described below.
3. Where old saved graph state exists, restore it against the matching graph and
   persist it again to write the current format.
4. Remove downstream handling of the old Python names and owner fields.

The package version remains `0.7.0` on this development branch. The release PR
will bump it to `0.8.0` separately.

### File runtime isolation

`graphon.file.runtime.WorkflowFileRuntimeRegistry` and
`configure_workflow_file_runtime()` were removed. Use `set_workflow_file_runtime()`
for the process default, `get_workflow_file_runtime()` for a required adapter, and
`peek_workflow_file_runtime()` for an optional lookup. The removed configuration
alias returned its adapter; `set_workflow_file_runtime()` returns `None`, so retain
the adapter separately if the old call was used in an assignment or expression.
For independent adapters, use engine injection or the scope below instead of
creating registry instances.

Pass `Engine(..., file_runtime=adapter)` to select an execution's
`WorkflowFileRuntimeProtocol`. Omitting it or passing `None` captures the current
scoped adapter or process default **at engine construction**, including an
unconfigured state. Configure the process default before constructing engines;
changing it later no longer changes existing engines' file operations.

The engine binds its adapter in workers, the dispatcher, and layer hooks,
including child and resumed execution. `EngineEventFilterContext.from_engine()`
carries it to `ResponseStreamFilter`, including a restored filter. Custom filters
can use `context.file_runtime` with the context manager below. The built-in filter
binds it during `initialize()`, `on_event()`, `flush()`, and `loads()`, including
streaming-template callbacks, and restores the caller's scope on return or failure.
Constructing a filter context directly captures its current scoped/process default when
`file_runtime` is omitted; prefer `from_engine()` to retain the engine's selection.

For DSL construction and host-side rendering of delivered files, use
`use_workflow_file_runtime` from `graphon.file.runtime`:

```python
from graphon.dsl import loads
from graphon.file.runtime import use_workflow_file_runtime

with use_workflow_file_runtime(adapter):
    engine = loads(dsl)

for event in engine.run():
    with use_workflow_file_runtime(engine.file_runtime):
        consume(event)  # File.to_dict(), markdown, or plugin parameter rendering
```

Engine iteration restores the caller's binding before yielding each event and
after close or failure. Scoped `None` disables resolution even if a process
default exists. Direct file helpers outside a scope still use the process default.
Explicitly injected HTTP clients and node-specific file ports remain independent.

Adapters are transient: pass them again when rebuilding an engine from
`RuntimeState.from_snapshot()`. File metadata, references, and snapshot formats
are unchanged. Use `File.model_dump(mode="json")` for metadata without URL
resolution; `to_dict()`, Markdown, and plugin parameters still resolve URLs.

### Public names and imports

| 0.7.x | 0.8.0 |
| --- | --- |
| `graphon.graph_engine.GraphEngine` | `graphon.engine.Engine` |
| `GraphEngineConfig(max_workers=n)` | `Engine(..., workers=n)` |
| `graphon.graph_events` | `graphon.engine_events` |
| `GraphEngineEvent` / `BaseGraphEvent` | `EngineEvent` |
| `GraphNodeEventBase` / `GraphAgentNodeEventBase` | `NodeEvent` |
| `graphon.node_events.NodeEventBase` | `graphon.node_events.NodeEventPayload` |
| `graphon.runtime.graph_runtime_state.GraphRuntimeState` | `graphon.runtime.RuntimeState` |
| `graphon.runtime.graph_runtime_state_protocol.ReadOnlyGraphRuntimeState` | `graphon.runtime.ReadOnlyRuntimeState` |
| `ReadOnlyGraphRuntimeStateWrapper` | `ReadOnlyRuntimeStateWrapper` |
| `graphon.entities.graph_init_params.GraphInitParams` | `graphon.runtime.InitParams` |
| `graphon.graph_engine.layers.GraphEngineLayer` | `graphon.engine.layer.Layer` |
| `graphon.graph_engine.layers.ExecutionLimitsLayer` | `graphon.engine.layer.ExecutionLimitsLayer` |
| `graphon.filters.GraphEventFilter` | `graphon.engine.filter.EngineEventFilter` |
| `GraphEventFilterContext` | `EngineEventFilterContext` |
| `filter_graph_events()` | `filter_engine_events()` |
| `graphon.graph_engine.command_channels` | `graphon.engine.command` |
| `graphon.graph_engine.container_handlers` | `graphon.engine.container_handler` |
| `graphon.graph_engine.loop_container_handler` | `graphon.engine.container_handler.builtin.loop` |
| `graphon.graph_engine.iteration_container_handler` | `graphon.engine.container_handler.builtin.iteration` |
| `graphon.graph_engine.ready_queue` | `graphon.engine.ready_queue` |
| `graphon.graph_engine.worker` | `graphon.engine.worker` |
| `TaskEvent` | `NodeEventTask` |

Concrete `GraphRun*`, `GraphEdge*`, and `NodeRun*` event class names are unchanged;
import them from `graphon.engine_events`. Application-owned database field names,
such as `serialized_graph_runtime_state`, do not have to change.

Python pickles, `isinstance` checks, schema-title assertions, and imports that name
removed classes are not compatible and must be recreated or updated.

### Constructing an engine

`workflow_id` now belongs to `RuntimeState`, the command channel is optional, and
the worker count is a fixed positive integer:

```python
from graphon.engine import Engine
from graphon.runtime import RuntimeState

state = RuntimeState(
    variable_pool=variable_pool,
    start_at=start_at,
    workflow_id=workflow_id,
)
engine = Engine(
    graph=graph,
    runtime_state=state,
    command_channel=command_channel,  # optional
    workers=5,
)
engine.add_layer(layer)
events = engine.run()
```

Apply these call-site changes:

- `graph_runtime_state=` becomes `runtime_state=`.
- `graph_init_params=` becomes `init_params=` on nodes and factories.
- `engine.graph_runtime_state` and `node.graph_runtime_state` become
  `engine.runtime_state` and `node.runtime_state`.
- `engine.layer(layer)` becomes `engine.add_layer(layer)` and no longer returns
  the engine.
- `graphon.dsl.loads()` accepts `workers=` directly; remove `GraphEngineConfig`.

### Events and container ownership

All emitted events now derive from `EngineEvent` and carry `container_id`:

- `""` means the root frame.
- A non-empty value is the node ID of the frame's direct container.
- Nested events name only their direct owner, not every ancestor.
- Node implementations yield `NodeEventPayload`; the engine adds execution context
  and emits a `NodeEvent`.

For graph DSL, write `data.container_id` on nested nodes. The loader still accepts
React Flow `parentId` and legacy `iteration_id` / `loop_id`. When both legacy fields
exist, their ancestry must identify one direct owner; otherwise add `container_id`
explicitly. The owner must be a `NodeExecutionType.CONTAINER` node.

Each loop or iteration frame receives only its scoped graph. Child variables can
inherit visible parent values when the frame is created, but child graph and state
do not expose siblings or parent graph structure. They do not provide a live
fallback to later parent-pool changes.

An abort or fatal failure now wins over a concurrent pause. Persist resumable state
only after the engine emits `GraphRunPausedEvent` and its run iterator has been
fully consumed; do not infer a pause from a queued command. Loop handlers write
only their configured selectors back to the parent, while iteration frame state
remains isolated.

### Graph validation

Default `Graph.init()`, `graphon.dsl.loads()`, and `Graph.new().build()` now reject
execution cycles, including self-edges and cycles in inactive paths or nested
containers. Express repetition through Loop, Iteration, or a custom container
handler with acyclic internal edges. Valid disconnected graphs and multiple
roots remain supported.

Executable node IDs must be non-empty strings and unique across the supplied
configuration. Edges must name existing nodes in the same direct scope, and an
explicit `sourceHandle` must be a string. Omitting it still defaults to `"source"`;
other string handles retain their existing behavior. Duplicate edge IDs are
rejected within each scope, including descendants, while sibling scopes may
reuse IDs. Supported editor notes and legacy ownership fields still normalize.

For configured graphs, input and topology checks run before node constructors
or `post_init()` hooks throughout the retained subtree. A child graph's cycle
check excludes its parent and sibling scopes.
Root type is checked afterward against the resolved node, so custom factory
aliases for loop and iteration entry nodes remain supported.
The Python builder receives existing nodes and rejects invalid graphs by
`build()`. Graph validation errors retain structured issues; DSL loading exposes
them as `DslError(code="graph.validation_failed")` rather than an `AttributeError`.
Existing DSL normalization errors keep their codes.

Correct previously accepted invalid definitions before loading or restoring them.
Snapshot formats and edge IDs for valid graphs are unchanged.
`Graph.init(skip_validation=True)` still bypasses endpoint existence, root type,
and cycle checks; raw field/ID/schema/ownership checks and root ID presence
remain mandatory. The low-level `Graph(...)` constructor remains unchecked.
These trusted paths do not
provide the guarantees of default construction.

### Edge identity

`Edge.id` is now the DSL edge ID and is unique only inside one graph. Code that
indexes edges across an execution must use `(frame_id, edge_id)`. Traversal events
therefore include `frame_id`; do not deduplicate them by `edge_id` alone.

Generated IDs reserve explicit IDs in the same graph. Preserve explicit DSL IDs
when importing and exporting graphs so paused executions and human-input state can
be restored against the same definition.

### Extension protocols

Custom `NodeFactory` implementations must provide all four operations:

```python
class NodeFactory(Protocol):
    def validate_node(self, node_config: NodeConfigDict) -> NodeExecutionType: ...
    def with_graph_config(
        self, graph_config: Mapping[str, object]
    ) -> "NodeFactory": ...
    def with_runtime_state(self, runtime_state: RuntimeState) -> "NodeFactory": ...
    def create_node(self, node_config: NodeConfigDict) -> Node: ...
```

`validate_node()` must not construct a node, but it must perform the same concrete
schema, plugin, dependency, and credential validation required by `create_node()`.
The two `with_*()` methods must return a factory whose future nodes see only the
supplied scoped graph and frame state; stateful factories should return an
independent copy. Copy `InitParams` before node construction because
`Node.post_init()` runs during construction.

Other extension renames are direct:

| 0.7.x | 0.8.0 |
| --- | --- |
| `ContainerHandler.start_await()` | `handle_request()` |
| `ContainerHandler.complete_frame()` | `complete_frame_if_ready()` |
| `ContainerHandler.should_collect()` | `should_emit()` |
| `FrameRegistry.materialize_child_frame()` | `create_child()` |
| `FrameRegistry.materialize_child_frame_from_state()` | `restore_child()` |
| `FrameRegistry.get(frame_id)` | `registry[frame_id]` |
| `ExecutionFrame.graph_runtime_state` | `state` |
| `ExecutionFrame.state_manager` / `edge_processor` | `scheduler` |
| `ExecutionFrame.error_handler` | `failure_handler` |
| `WorkerPool.drain()` | `pause()` |
| `ReadyQueue.drain()` | `take_all()` |
| `RuntimeState.drain_deferred_ready_tasks()` | `take_deferred_ready_tasks()` |

Custom ready queues now store `ReadyTask` (`StartTask | ResumeTask`) and no longer
use `ReadyQueueState`. Worker internals named `task_claim_lock` and
`task_claiming` are now `task_acquisition_lock` and `task_acquisition_enabled`.
Import `ROOT_FRAME_ID` from `graphon.runtime.execution`, not the ready-queue
package.

### Layers, filters, and commands

`RuntimeState(execution_context=...)`, its `execution_context` property, and the
matching `Worker`/`WorkerPool` arguments were removed. Move host node-task context
activation into `Layer.node_run_context(node, parent_execution_id=...)` and
register the layer with `engine.add_layer(layer)`. Return a fresh context manager
for each task, including container resumes; see [layer scopes](src/graphon/engine/layer/README.md).

This changes failure handling: the former runtime manager used ordinary `with`
semantics. Layer context entry/exit exceptions are logged and isolated, and the
manager cannot suppress node execution errors. Move setup that must prevent
execution on failure before running the engine or into the node's execution path. These
task scopes do not replace file-runtime scopes or response-filter metadata.

- `Layer` lifecycle hooks have no-op defaults. `DebugLoggingLayer` and
  `GraphEngineLayerNotInitializedError` were removed. `LimitType` and the old
  `ExecutionLimitsLayer` helper methods were also removed; the layer itself remains.
- `ResumableGraphEventFilter` and `filter_id` were removed. Persist a concrete
  filter such as `ResponseStreamFilter` directly. Its former support types such as
  `Path`, `ResponseSession`, and `ResponseStreamFilterState` are private internals.
- `GraphEngineCommand`, `CommandType`, and `VariableUpdate` were removed. Send
  `AbortCommand`, `PauseCommand`, or `UpdateVariablesCommand`; pass `Variable`
  objects directly in `UpdateVariablesCommand.updates`.
- `CommandProcessor` now receives `frame_registry`, so variable updates reach all
  active frame scopes. Custom `register_handler()` extensions were removed.
- `GraphEngineManager` was removed. Applications should own command-channel key
  policy and send concrete commands through `RedisChannel` or another
  `CommandChannel` directly.
- A custom Redis pipeline must implement `set()` in addition to the existing list
  and expiry operations. For a rolling deployment, keep the legacy pending marker
  and wrapped update payload for at least one configured command TTL after all old
  consumers have stopped.

### Snapshot eligibility

`RuntimeState.dumps()` and the layer read-only wrapper now raise `RuntimeError`
while an engine run or any of its execution threads is active. This includes
start, node, and event callbacks, even when a pause or completion flag is set.
Receiving a terminal event alone does not finish the run iterator.

For resumable persistence, request a cooperative pause and fully consume the
iterator before taking a snapshot. Layers can persist from `on_graph_end` once
all execution threads have stopped. Closing an iterator also initiates teardown,
but aborts, failures, and early closure can leave threads alive beyond the shutdown
timeout; snapshots remain unavailable until those threads exit. Closing alone
does not establish a resumable pause.

Pre-run and restored states remain serializable, subject to existing migration
requirements. Snapshot formats are unchanged. Serialization excludes engine
startup and other snapshot writers across all frames; the host must avoid
concurrent direct mutations of runtime objects.

### Persisted state

| State | Current writer | Accepted input | Migration requirement |
| --- | --- | --- | --- |
| `RuntimeState` | 3.0 | 1.0, 2.0, 3.0 | If saved graph state exists, attach the matching graph before re-serializing 1.0 or 2.0 state. |
| `ResponseStreamFilter` | 2.0 | 1.0, 2.0 | Initialize with the matching graph before re-serializing 1.0 state. |

Runtime snapshot 2.0 stored positional full-graph edge IDs; 3.0 stores frame-local
DSL edge IDs. Migration is graph-aware and commits only after every frame maps
successfully. An unattached restored snapshot may be attached later, but it cannot
be serialized as 3.0 before legacy edge IDs are migrated.

Version 2.0 remains explicit while it is pending migration; there is no hidden
version marker. `ResponseStreamFilter` version 1.0 state likewise cannot be
serialized as version 2.0 until the filter has been initialized with its graph.

Snapshot compatibility is isolated by file:

- `graphon.runtime.runtime_state.snapshot` contains structures and dispatch shared
  by all runtime-state versions; `v1`, `v2`, and `v3` contain the exact schemas and
  conversion owned by those versions.
- `graphon.engine.filter.builtin.response_stream.snapshot` contains the shared
  response-filter state; `v1` owns its graph-aware migration and `v2` owns the
  current format.

Dispatch derives the requested version module from the persisted `version` field;
current code does not import legacy modules. After the migration window, remove
runtime-state `v1.py` and `v2.py`, response-stream `v1.py`, and their legacy-only
tests and fixtures. No remaining source file needs an edit.

Runtime snapshot 1.0 is supported for root-frame work. A 1.0 snapshot containing
queued loop or iteration child work cannot be resumed because that format did not
record the child frame; restoration fails before workers start instead of hanging.

Programmatic `Graph.new()` snapshots without `graph_config` remain restorable when
their legacy edge IDs are the original contiguous `edge_N` sequence. Persisted
envelopes around Graphon's JSON do not need their own version bump unless the
application validates Graphon field names itself.

### Removed engine internals

Downstream code must stop importing `GraphEngineManager`, `GraphStateManager`,
`EdgeProcessor`, `SkipPropagator`, the old orchestration/event-management packages,
or dynamic worker-management types. Scheduling is owned by `Scheduler`, event
processing by `graphon.engine.event`, and frame construction by `FrameRegistry`.

`GraphExecution` and `NodeExecution` now live in `graphon.runtime.execution`.
Replace the mutating `GraphExecution.loads(data)` call with the constructor-style
`GraphExecution.from_snapshot(data)`. The application-specific
`DIFY_RUN_CONTEXT_KEY` constant is no longer exported; downstream applications
should own that key.
`WorkflowExecution` remains temporarily available and is planned for removal in a
later release.
