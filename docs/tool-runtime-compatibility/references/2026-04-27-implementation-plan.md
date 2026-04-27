# Tool Runtime Compatibility Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development or superpowers:executing-plans to
> implement this plan task-by-task.

**Goal:** Pass the tool node execution id to the workflow tool runtime while
preserving legacy tool-node `variable_pool=None` behavior.

**Architecture:** `ToolNode._run()` chooses the historical `variable_pool`
value and restores the node execution id before runtime lookup.
`ToolNode._get_tool_runtime()` then calls the runtime protocol directly with
`node_execution_id`. Graphon releases this protocol change before downstream
adapters adopt it, so runtime adapters are expected to accept the new keyword.

**Tech Stack:** Python 3.12/3.13, `pytest`, `ruff`, `ty`, Graphon node runtime
protocols.

---

## Spec

The repaired behavior must satisfy these requirements:

1. For legacy tool node data where `node_data.version == "1"` and
   `node_data.tool_node_version is None`, `ToolNode.run()` must call the
   workflow tool runtime with `variable_pool=None`.
2. For non-legacy tool node data, `ToolNode.run()` must pass
   `graph_runtime_state.variable_pool` to the workflow tool runtime.
3. `ToolNode.run()` must call `ensure_execution_id()` before obtaining the
   runtime and must pass the restored or created execution id as
   `node_execution_id`.
4. Runtime adapters must accept the `node_execution_id` keyword. Graphon does
   not need a signature-inspection fallback because this package release lands
   before the downstream Dify PR that consumes it.
5. The runtime protocol must continue to advertise
   `node_execution_id: str | None = None` so adapters can be called directly
   with or without the optional value, while `ToolNode` always supplies it.

## File Structure

- Modify: `tests/nodes/tool/test_tool_node.py`
  - Owns regression coverage for legacy `variable_pool` selection and
    execution-id propagation.
- Modify: `src/graphon/nodes/tool/tool_node.py`
  - Restores the legacy `variable_pool` selection in `_run()`.
  - Calls `runtime.get_runtime(..., node_execution_id=...)` directly.
- No change expected: `src/graphon/nodes/runtime.py`
  - The current protocol signature already matches the target API.

## Tasks

- [x] Add/adjust tool-node run coverage so legacy nodes pass
  `variable_pool=None` and current tool-node versions pass the real
  `VariablePool`.
- [x] Ensure the same run coverage proves the restored execution id is passed
  to runtime as `node_execution_id`.
- [x] Add a regression test showing an adapter that does not accept
  `node_execution_id` is no longer treated as compatible.
- [x] Restore the legacy `variable_pool` branch in `ToolNode._run()`.
- [x] Remove `_runtime_accepts_node_execution_id()` and the `inspect`
  signature imports.
- [x] Simplify `_get_tool_runtime()` to a single protocol call shape.
- [x] Run focused tests and lint/type checks.

## Verification Commands

```bash
uv run pytest tests/nodes/tool/test_tool_node.py
uv run ruff check --no-fix src/graphon/nodes/tool/tool_node.py tests/nodes/tool/test_tool_node.py
uv run ty check
```
