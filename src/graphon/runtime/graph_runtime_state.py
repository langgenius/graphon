from __future__ import annotations

import threading
from abc import abstractmethod
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from copy import deepcopy
from typing import TYPE_CHECKING, Annotated, ClassVar, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from graphon.enums import NodeExecutionType, NodeState, NodeType
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.runtime.container_state import (
    ContainerFrameState,
    ContainerRunState,
    FrameRuntimeData,
)
from graphon.runtime.ready_queue import ReadyQueue
from graphon.runtime.variable_pool import VariablePool

from .execution import ROOT_FRAME_ID, GraphExecution

if TYPE_CHECKING:
    from graphon.engine.ready_queue import ReadyTask


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


class _GraphStateSnapshotV1(BaseModel):
    """Node and edge state stored by runtime snapshot version 1.0."""

    model_config = ConfigDict(frozen=True)

    nodes: dict[str, NodeState]
    edges: dict[str, NodeState]


class _GraphRuntimeStateSnapshotV1(BaseModel):
    """Runtime snapshot produced before frame-aware execution."""

    model_config = ConfigDict(frozen=True)

    version: Literal["1.0"]
    start_at: float
    node_run_steps: int = Field(ge=0)
    llm_usage: LLMUsage
    outputs: dict[str, object]
    variable_pool: VariablePool
    ready_queue: str
    graph_execution: str
    paused_nodes: tuple[str, ...]
    deferred_nodes: tuple[str, ...]
    graph_state: _GraphStateSnapshotV1


class _GraphRuntimeStateSnapshot(BaseModel):
    """Validated serialized runtime state snapshot."""

    model_config = ConfigDict(frozen=True)

    version: Literal["2.0", "3.0"]
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


_GRAPH_RUNTIME_STATE_SNAPSHOT_ADAPTER = TypeAdapter(
    Annotated[
        _GraphRuntimeStateSnapshotV1 | _GraphRuntimeStateSnapshot,
        Field(discriminator="version"),
    ],
)


def _new_ready_queue() -> ReadyQueue:
    from graphon.engine.ready_queue import (  # ruff:ignore[import-outside-top-level]
        InMemoryReadyQueue,
    )

    return InMemoryReadyQueue()


class RuntimeState:  # ruff:ignore[too-many-public-methods]
    """Mutable runtime state shared across graph execution components.

    `RuntimeState` encapsulates the runtime state of workflow execution,
    including scheduling details, variable values, and timing information.

    Values that are initialized prior to workflow execution and remain constant
    throughout the execution should be part of `InitParams` instead.
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
        workflow_id: str | None = None,
        graph_execution: GraphExecution | None = None,
        execution_context: AbstractContextManager[object] | None = None,
    ) -> None:
        """Initialize all mutable state owned by one graph execution frame.

        A newly started root runtime supplies ``workflow_id`` so this constructor
        can create the execution aggregate. Child frames and restored runtimes
        instead supply their existing ``graph_execution`` so every frame shares
        one workflow identity and lifecycle. Callers may supply both forms when
        useful at an API boundary, but their workflow IDs must agree.

        Args:
            variable_pool: Variables visible to nodes in this frame.
            start_at: Unix timestamp at which execution started.
            llm_usage: Accumulated language-model usage, copied on input.
            outputs: Current workflow outputs, copied on input.
            node_run_steps: Number of node runs already completed.
            ready_queue: Queue for runnable tasks, or a local queue by default.
            deferred_ready_queue: Queue for tasks held while execution is paused.
            workflow_id: Identity used to create a new execution aggregate.
            graph_execution: Existing aggregate shared by child or restored frames.
            execution_context: Context entered by workers around node execution.

        Raises:
            ValueError: If ``node_run_steps`` is negative, neither identity form
                is supplied, or the two supplied workflow identities disagree.

        """
        if node_run_steps < 0:
            msg = "node_run_steps must be non-negative"
            raise ValueError(msg)
        if graph_execution is None:
            if workflow_id is None:
                msg = "workflow_id or graph_execution is required"
                raise ValueError(msg)
            graph_execution = GraphExecution(workflow_id=workflow_id)
        elif workflow_id is not None and workflow_id != graph_execution.workflow_id:
            msg = "workflow_id must match graph_execution.workflow_id"
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
        self._graph_execution = graph_execution
        self._execution_context = (
            execution_context if execution_context is not None else nullcontext()
        )
        self._container_runs: dict[str, ContainerRunState] = {}
        self._container_frames: dict[str, ContainerFrameState] = {}
        self._pending_graph_node_states: dict[str, NodeState] = {}
        self._pending_graph_edge_states: dict[str, NodeState] = {}
        self._has_pending_graph_state = False
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
    def graph_execution(self) -> GraphExecution:
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

    def restore_graph_state(
        self,
        *,
        node_states: Mapping[str, NodeState],
        edge_states: Mapping[str, NodeState],
    ) -> None:
        """Stage persisted graph states for the graph attached to this runtime.

        Frame restoration constructs runtime state before constructing its
        scoped graph. This method records the complete node and edge mappings;
        :meth:`attach_graph` validates their topology and applies them atomically
        before the frame becomes executable. Staging is rejected after graph
        attachment so callers cannot overwrite a live graph accidentally.

        Args:
            node_states: Persisted node states keyed by node ID.
            edge_states: Persisted edge states keyed by edge ID.

        Raises:
            RuntimeError: If a graph is already attached to this runtime state.

        """
        if self._graph is not None or self._has_pending_graph_state:
            msg = "graph state must be restored before attaching a graph"
            raise RuntimeError(msg)
        self._pending_graph_node_states = dict(node_states)
        self._pending_graph_edge_states = dict(edge_states)
        self._has_pending_graph_state = True

    def attach_graph(self, graph: GraphProtocol) -> None:
        """Attach the materialized graph to the runtime state."""
        if self._graph is not None and self._graph is not graph:
            msg = "RuntimeState already attached to a different graph instance"
            raise ValueError(msg)
        if self._has_pending_graph_state and (
            set(self._pending_graph_node_states) != set(graph.nodes)
            or set(self._pending_graph_edge_states) != set(graph.edges)
        ):
            msg = "Saved graph state does not match rebuilt graph"
            raise RuntimeError(msg)
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
        self._has_pending_graph_state = False

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
            version="3.0",
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
        cls: type[RuntimeState],
        data: str,
        *,
        ready_queue_factory: Callable[[], ReadyQueue] = _new_ready_queue,
    ) -> RuntimeState:
        """Restore runtime state from a serialized snapshot."""
        snapshot = _GRAPH_RUNTIME_STATE_SNAPSHOT_ADAPTER.validate_json(data)

        ready_queue = ready_queue_factory()
        ready_queue.loads(snapshot.ready_queue)
        deferred_ready_queue = ready_queue_factory()
        if isinstance(snapshot, _GraphRuntimeStateSnapshotV1):
            from graphon.engine.ready_queue import (  # ruff:ignore[import-outside-top-level]
                StartTask,
            )

            for node_id in dict.fromkeys((
                *snapshot.paused_nodes,
                *snapshot.deferred_nodes,
            )):
                deferred_ready_queue.put(
                    StartTask(frame_id=ROOT_FRAME_ID, node_id=node_id),
                )
            container_runs: tuple[ContainerRunState, ...] = ()
            container_frames: tuple[ContainerFrameState, ...] = ()
            graph_node_states = snapshot.graph_state.nodes
            graph_edge_states = snapshot.graph_state.edges
        else:
            deferred_ready_queue.loads(snapshot.deferred_ready_tasks)
            container_runs = snapshot.container_runs
            container_frames = snapshot.container_frames
            graph_node_states = snapshot.graph_node_states
            graph_edge_states = snapshot.graph_edge_states

        graph_execution = GraphExecution.from_snapshot(snapshot.graph_execution)

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
        state._container_runs = {run.invocation_id: run for run in container_runs}
        state._container_frames = {frame.frame_id: frame for frame in container_frames}
        state.restore_graph_state(
            node_states=graph_node_states,
            edge_states=graph_edge_states,
        )
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
