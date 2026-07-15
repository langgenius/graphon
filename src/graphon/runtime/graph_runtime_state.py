from __future__ import annotations

import json
import threading
from abc import abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from copy import deepcopy
from typing import TYPE_CHECKING, ClassVar, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from graphon.enums import NodeExecutionType, NodeState, NodeType
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.runtime.container_state import (
    ContainerFrameState,
    ContainerRunState,
    FrameRuntimeData,
)
from graphon.runtime.ready_queue import ReadyQueue
from graphon.runtime.variable_pool import VariablePool

if TYPE_CHECKING:
    from graphon.entities.pause_reason import PauseReason
    from graphon.graph_engine.ready_queue import ReadyTask


class NodeExecutionProtocol(Protocol):
    """Structural interface for persisted per-node execution state."""

    retry_count: int
    execution_id: str

    @abstractmethod
    def increment_retry(self) -> None:
        """Increment the retry counter for the node execution."""
        ...


class GraphExecutionProtocol(Protocol):
    """Structural interface for graph execution aggregate.

    Defines the minimal set of attributes and methods required
    from a GraphExecution entity for runtime orchestration and
    state management.
    """

    workflow_id: str
    started: bool
    completed: bool
    aborted: bool
    paused: bool
    error: Exception | None
    exceptions_count: int
    pause_reasons: list[PauseReason]

    @abstractmethod
    def start(self) -> None:
        """Transition execution into the running state."""
        ...

    @abstractmethod
    def complete(self) -> None:
        """Mark execution as successfully completed."""
        ...

    @abstractmethod
    def abort(self, reason: str) -> None:
        """Abort execution in response to an external stop request."""
        ...

    @abstractmethod
    def pause(self, reason: PauseReason) -> None:
        """Pause execution with a recorded reason."""
        ...

    @abstractmethod
    def fail(self, error: Exception) -> None:
        """Record an unrecoverable error and end execution."""
        ...

    @abstractmethod
    def record_node_failure(self) -> None:
        """Increment the count of node failures observed during execution."""
        ...

    @abstractmethod
    def get_or_create_node_execution(
        self,
        *,
        frame_id: str,
        node_id: str,
    ) -> NodeExecutionProtocol:
        """Return the execution entity for a task, creating it when needed."""
        ...

    @abstractmethod
    def dumps(self) -> str:
        """Serialize execution state into a JSON payload."""
        ...

    @abstractmethod
    def loads(self, data: str) -> None:
        """Restore execution state from a previously serialized payload."""
        ...


class NodeProtocol(Protocol):
    """Node behavior consumed by runtime state and response filtering."""

    id: str
    state: NodeState
    execution_type: NodeExecutionType
    node_type: ClassVar[NodeType]

    @abstractmethod
    def blocks_variable_output(
        self,
        variable_selectors: set[tuple[str, ...]],
    ) -> bool: ...


class EdgeProtocol(Protocol):
    """Edge data consumed by runtime state and response filtering."""

    id: str
    state: NodeState
    tail: str
    head: str
    source_handle: str


class GraphProtocol(Protocol):
    """Graph behavior consumed by runtime state and response filtering."""

    @property
    @abstractmethod
    def nodes(self) -> Mapping[str, NodeProtocol]: ...

    @property
    @abstractmethod
    def edges(self) -> Mapping[str, EdgeProtocol]: ...

    @property
    @abstractmethod
    def root_node(self) -> NodeProtocol: ...

    @abstractmethod
    def get_outgoing_edges(self, node_id: str) -> Sequence[EdgeProtocol]: ...


class _GraphRuntimeStateSnapshot(BaseModel):
    """Validated serialized runtime state snapshot."""

    model_config = ConfigDict(frozen=True)

    version: Literal["1.0"]
    start_at: float
    node_run_steps: int = Field(ge=0)
    llm_usage: LLMUsage
    outputs: dict[str, object]
    variable_pool: VariablePool
    ready_queue: str
    graph_execution: str
    deferred_ready_tasks: str
    container_runs: tuple[ContainerRunState, ...]
    container_frames: tuple[ContainerFrameState, ...]
    graph_node_states: dict[str, NodeState]
    graph_edge_states: dict[str, NodeState]


def _new_ready_queue() -> ReadyQueue:
    from graphon.graph_engine.ready_queue import InMemoryReadyQueue  # noqa: PLC0415

    return InMemoryReadyQueue()


def _new_graph_execution(workflow_id: str = "") -> GraphExecutionProtocol:
    from graphon.graph_engine.domain.graph_execution import (  # noqa: PLC0415
        GraphExecution,
    )

    return GraphExecution(workflow_id=workflow_id)


class GraphRuntimeState:  # noqa: PLR0904
    """Mutable runtime state shared across graph execution components.

    `GraphRuntimeState` encapsulates the runtime state of workflow execution,
    including scheduling details, variable values, and timing information.

    Values that are initialized prior to workflow execution and remain constant
    throughout the execution should be part of `GraphInitParams` instead.
    """

    _container_state_lock: threading.Lock

    def __init__(
        self,
        *,
        variable_pool: VariablePool,
        start_at: float,
        llm_usage: LLMUsage | None = None,
        outputs: dict[str, object] | None = None,
        node_run_steps: int = 0,
        ready_queue: ReadyQueue | None = None,
        deferred_ready_queue: ReadyQueue | None = None,
        graph_execution: GraphExecutionProtocol | None = None,
        execution_context: AbstractContextManager[object] | None = None,
    ) -> None:
        if node_run_steps < 0:
            msg = "node_run_steps must be non-negative"
            raise ValueError(msg)
        self._variable_pool = variable_pool
        self._start_at = start_at
        self._llm_usage = (
            llm_usage if llm_usage is not None else LLMUsage.empty_usage()
        ).model_copy()
        self._outputs = deepcopy(outputs) if outputs is not None else {}
        self._node_run_steps = node_run_steps
        self._graph: GraphProtocol | None = None
        self._ready_queue = (
            ready_queue if ready_queue is not None else _new_ready_queue()
        )
        self._deferred_ready_queue = (
            deferred_ready_queue
            if deferred_ready_queue is not None
            else _new_ready_queue()
        )
        self._graph_execution = (
            graph_execution if graph_execution is not None else _new_graph_execution()
        )
        self._execution_context = (
            execution_context if execution_context is not None else nullcontext()
        )
        self._container_runs: dict[str, ContainerRunState] = {}
        self._container_frames: dict[str, ContainerFrameState] = {}
        self._pending_graph_node_states: dict[str, NodeState] = {}
        self._pending_graph_edge_states: dict[str, NodeState] = {}
        self._container_state_lock = threading.Lock()

    @property
    def variable_pool(self) -> VariablePool:
        return self._variable_pool

    @property
    def ready_queue(self) -> ReadyQueue:
        return self._ready_queue

    @property
    def deferred_ready_queue(self) -> ReadyQueue:
        return self._deferred_ready_queue

    @property
    def graph_execution(self) -> GraphExecutionProtocol:
        return self._graph_execution

    @property
    def execution_context(self) -> AbstractContextManager[object]:
        return self._execution_context

    @property
    def start_at(self) -> float:
        return self._start_at

    @property
    def total_tokens(self) -> int:
        return self._llm_usage.total_tokens

    @property
    def llm_usage(self) -> LLMUsage:
        return self._llm_usage.model_copy()

    def add_llm_usage(self, usage: LLMUsage) -> None:
        if usage.total_tokens <= 0:
            return
        if self._llm_usage.total_tokens == 0:
            self._llm_usage = usage.model_copy()
        else:
            self._llm_usage = self._llm_usage.plus(usage)

    @property
    def outputs(self) -> dict[str, object]:
        return deepcopy(self._outputs)

    def set_output(self, key: str, value: object) -> None:
        self._outputs[key] = deepcopy(value)

    def get_output(self, key: str, default: object = None) -> object:
        return deepcopy(self._outputs.get(key, default))

    def merge_response_outputs(self, outputs: Mapping[str, object]) -> None:
        for key, value in outputs.items():
            if key == "answer":
                existing = self.get_output("answer", "")
                if existing:
                    self.set_output("answer", f"{existing}{value}")
                else:
                    self.set_output("answer", value)
                continue
            self.set_output(key, value)

    @property
    def node_run_steps(self) -> int:
        return self._node_run_steps

    def increment_node_run_steps(self) -> None:
        self._node_run_steps += 1

    def attach_graph(self, graph: GraphProtocol) -> None:
        """Attach the materialized graph to the runtime state."""
        if self._graph is not None and self._graph is not graph:
            msg = "GraphRuntimeState already attached to a different graph instance"
            raise ValueError(msg)
        self._graph = graph
        self._apply_pending_graph_state()

    def _apply_pending_graph_state(self) -> None:
        if self._graph is None:
            return
        for node_id, state in self._pending_graph_node_states.items():
            self._graph.nodes[node_id].state = state
        for edge_id, state in self._pending_graph_edge_states.items():
            self._graph.edges[edge_id].state = state
        self._pending_graph_node_states.clear()
        self._pending_graph_edge_states.clear()

    def dumps(self) -> str:
        """Serialize runtime state into a JSON string."""
        with self._container_state_lock:
            container_runs = tuple(self._container_runs.values())
            container_frames = tuple(self._container_frames.values())
        if self._graph is None:
            graph_node_states = self._pending_graph_node_states
            graph_edge_states = self._pending_graph_edge_states
        else:
            graph_node_states = {
                node_id: node.state for node_id, node in self._graph.nodes.items()
            }
            graph_edge_states = {
                edge_id: edge.state for edge_id, edge in self._graph.edges.items()
            }
        return _GraphRuntimeStateSnapshot(
            version="1.0",
            start_at=self._start_at,
            node_run_steps=self._node_run_steps,
            llm_usage=self._llm_usage,
            outputs=self.outputs,
            variable_pool=self.variable_pool,
            ready_queue=self.ready_queue.dumps(),
            graph_execution=self.graph_execution.dumps(),
            deferred_ready_tasks=self._deferred_ready_queue.dumps(),
            container_runs=container_runs,
            container_frames=container_frames,
            graph_node_states=graph_node_states,
            graph_edge_states=graph_edge_states,
        ).model_dump_json()

    @classmethod
    def from_snapshot(
        cls: type[GraphRuntimeState],
        data: str,
    ) -> GraphRuntimeState:
        """Restore runtime state from a serialized snapshot."""
        snapshot = _GraphRuntimeStateSnapshot.model_validate_json(data)
        ready_queue = _new_ready_queue()
        ready_queue.loads(snapshot.ready_queue)
        deferred_ready_queue = _new_ready_queue()
        deferred_ready_queue.loads(snapshot.deferred_ready_tasks)
        execution_payload = json.loads(snapshot.graph_execution)
        graph_execution = _new_graph_execution(
            workflow_id=execution_payload["workflow_id"],
        )
        graph_execution.loads(snapshot.graph_execution)

        state = cls(
            variable_pool=snapshot.variable_pool,
            start_at=snapshot.start_at,
            llm_usage=snapshot.llm_usage,
            outputs=snapshot.outputs,
            node_run_steps=snapshot.node_run_steps,
            ready_queue=ready_queue,
            deferred_ready_queue=deferred_ready_queue,
            graph_execution=graph_execution,
        )
        state._container_runs = {
            run.invocation_id: run for run in snapshot.container_runs
        }
        state._container_frames = {
            frame.frame_id: frame for frame in snapshot.container_frames
        }
        state._pending_graph_node_states = snapshot.graph_node_states
        state._pending_graph_edge_states = snapshot.graph_edge_states
        return state

    def defer_ready_task(self, task: ReadyTask) -> None:
        self._deferred_ready_queue.put(task)

    def drain_deferred_ready_tasks(self) -> list[ReadyTask]:
        return self._deferred_ready_queue.drain()

    def enqueue_ready_task(self, task: ReadyTask) -> None:
        if self.graph_execution.paused:
            self.defer_ready_task(task)
            return
        self.ready_queue.put(task)

    def drain_ready_tasks_to_deferred(self) -> None:
        tasks = self.ready_queue.drain()
        for task in tasks:
            self.defer_ready_task(task)

    def snapshot_frame(
        self,
        *,
        variable_pool_scope: Literal["local", "parent"] = "local",
        copy_variable_pool: bool = True,
    ) -> FrameRuntimeData:
        graph = self._graph
        if graph is None:
            msg = "graph must be attached before snapshotting a frame"
            raise RuntimeError(msg)
        return FrameRuntimeData(
            variable_pool=(
                (
                    self.variable_pool.model_copy(deep=True)
                    if copy_variable_pool
                    else self.variable_pool
                )
                if variable_pool_scope == "local"
                else "parent"
            ),
            outputs=self.outputs,
            llm_usage=self.llm_usage,
            node_run_steps=self.node_run_steps,
            graph_node_states={
                node_id: node.state for node_id, node in graph.nodes.items()
            },
            graph_edge_states={
                edge_id: edge.state for edge_id, edge in graph.edges.items()
            },
        )

    def put_container_run(self, run: ContainerRunState) -> None:
        with self._container_state_lock:
            self._container_runs[run.invocation_id] = run

    def get_container_run(self, invocation_id: str) -> ContainerRunState:
        with self._container_state_lock:
            return self._container_runs[invocation_id]

    def container_runs(self) -> tuple[ContainerRunState, ...]:
        with self._container_state_lock:
            return tuple(self._container_runs.values())

    def update_container_run_phase_data(
        self,
        invocation_id: str,
        updates: Mapping[str, object],
    ) -> ContainerRunState:
        with self._container_state_lock:
            run = self._container_runs[invocation_id]
            updated_run = run.model_copy(
                update={"phase_data": {**dict(run.phase_data), **dict(updates)}},
            )
            self._container_runs[invocation_id] = updated_run
            return updated_run

    def pop_container_run(self, invocation_id: str) -> ContainerRunState:
        with self._container_state_lock:
            return self._container_runs.pop(invocation_id)

    def put_container_frame(self, frame: ContainerFrameState) -> None:
        with self._container_state_lock:
            self._container_frames[frame.frame_id] = frame

    def get_container_frame(self, frame_id: str) -> ContainerFrameState:
        with self._container_state_lock:
            return self._container_frames[frame_id]

    def container_frames(self) -> tuple[ContainerFrameState, ...]:
        with self._container_state_lock:
            return tuple(self._container_frames.values())

    def pop_container_frame(self, frame_id: str) -> ContainerFrameState:
        with self._container_state_lock:
            return self._container_frames.pop(frame_id)
