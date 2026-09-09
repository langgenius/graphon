# Changelog

This file records user-facing changes from Graphon 0.8.0 onward.

## 0.8.0 (Unreleased)

This is a breaking release. See the [0.7.x to 0.8.0 migration guide](MIGRATION.md)
before upgrading an integration.

### Changed

- Runtime now owns ready queues and task values; existing engine queue imports
  remain aliases. Runtime construction and current snapshot restoration avoid
  loading the engine. Public contract and bare Code/LLM package imports no longer
  register nodes; bootstrap uses explicit class imports. See
  [import migration](MIGRATION.md#runtime-queues-and-node-imports).
- Engines now retain their file adapter at construction and use it across
  execution threads, child/resumed runs, and response-stream rendering.
  `Engine(file_runtime=...)` and `use_workflow_file_runtime(...)` support distinct
  host adapters in one process. Inject an adapter or bind a file scope before construction;
  rebind adapters when restoring engines. File values and snapshot formats are
  unchanged. See [file runtime isolation](MIGRATION.md#file-runtime-isolation).
- HTTP consumers now own their default clients and preserve injected clients,
  including falsey ones. Each `SlimLLM` owns its lazy tokenizer cache and lock.
- Public runtime snapshots now reject active engine runs and execution threads,
  including threads still running after shutdown times out. Persist paused state
  after fully consuming the run iterator, or from a quiescent `on_graph_end` hook.
  Snapshot formats are unchanged.
- Graph construction now rejects invalid or duplicate node IDs, malformed edge
  fields, duplicate edge IDs within a scope, and execution cycles
  before constructing configured nodes, including errors in nested scopes.
  The Python builder also rejects invalid IDs, handles, and cycles. DSL graph
  validation failures now preserve their structured issue details.
- Renamed the public execution API around `Engine`, `EngineEvent`, `NodeEvent`,
  `RuntimeState`, and `InitParams`; the corresponding packages now use singular,
  responsibility-based names.
- Replaced dynamic worker configuration with the fixed `workers` argument on
  `Engine` and `graphon.dsl.loads()`.
- Replaced `in_loop_id` and `in_iteration_id` with one `container_id` on every
  engine event. Root-frame events use `container_id=""`.
- Scoped each container frame to its own `Graph` and `RuntimeState`. Child frames
  inherit visible parent variables when created, while child state and graph
  structure remain isolated.
- Preserved DSL edge IDs. An edge ID is unique within one graph; use
  `(frame_id, edge_id)` as its execution-wide identity.
- Simplified layer, filter, command, container-handler, frame, worker, and ready
  queue extension contracts.
- Made `NodeFactory.with_runtime_state()` part of the public protocol used to bind
  nodes to child-frame state.
- Made aborts and fatal failures take precedence over a concurrent pause. Persist
  resumable state only after receiving `GraphRunPausedEvent` and fully consuming
  the run iterator.
- Kept loop write-backs explicit and parent-owned while iteration frame state
  remains isolated; external variable-update commands stay authoritative.

### Removed

- Removed `WorkflowFileRuntimeRegistry`, `configure_workflow_file_runtime()`,
  `set_workflow_file_runtime()`, and the file process default; use
  [explicit adapters or scopes](MIGRATION.md#file-runtime-isolation).
- Removed `graphon.http.runtime` and its process-default get/set helpers; use
  [HTTP client injection](MIGRATION.md#http-client-ownership).
- Removed `RuntimeState.execution_context` and its constructor/worker arguments;
  use `Layer.node_run_context()` for host node-task scopes. See the
  [migration notes](MIGRATION.md#layers-filters-and-commands) for failure handling.
- Removed `GraphEngineConfig`, dynamic worker scaling, `GraphEngineManager`,
  `DebugLoggingLayer`, `ResumableGraphEventFilter`, and the old read/write lock.
- Removed the `graphon.graph_engine`, `graphon.graph_events`, and `graphon.filters`
  compatibility surfaces. This release does not provide aliases for renamed APIs.
- Removed public command wrappers `GraphEngineCommand`, `CommandType`, and
  `VariableUpdate` in favor of concrete command models.

### Persistence compatibility

- `RuntimeState` writes snapshot version 3.0 and can read versions 1.0, 2.0, and
  3.0. Version 1.0 snapshots containing queued child-container work cannot be
  resumed because they do not contain enough frame information.
- `ResponseStreamFilter` writes state version 2.0 and migrates version 1.0 state
  when initialized with the persisted graph.
- Runtime and response-filter snapshot readers now live in separate version files.
  Current readers and writers do not import legacy versions, so retiring a
  migration requires deleting only its version file, tests, and fixtures.
- Redis command producers continue to emit the pre-0.8 pending marker and
  variable-update payload during rolling deployments. Remove that compatibility
  only after all old consumers have been retired for at least one command TTL.
