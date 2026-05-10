from __future__ import annotations

import importlib
import json
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

from pydantic import BaseModel, Field
from pydantic_core import to_jsonable_python

from graphon.enums import NodeExecutionType, NodeState, NodeType
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.runtime.variable_pool import VariablePool

if TYPE_CHECKING:
    from graphon.entities.graph_init_params import GraphInitParams
    from graphon.entities.pause_reason import PauseReason
    from graphon.graph.graph import Graph
    from graphon.graph_events.node import NodeRunStreamChunkEvent, NodeRunSucceededEvent


class ReadyQueueProtocol(Protocol):
    """Structural interface required from ready queue implementations."""

    def put(self, item: str) -> None:
        """Enqueue the identifier of a node that is ready to run."""
        ...

    def get(self, timeout: float | None = None) -> str:
        """Return the next node identifier,
        blocking until available or timeout expires.
        """
        ...

    def task_done(self) -> None:
        """Signal that the most recently dequeued node has completed processing."""
        ...

    def empty(self) -> bool:
        """Return True when the queue contains no pending nodes."""
        ...

    def qsize(self) -> int:
        """Approximate the number of pending nodes awaiting execution."""
        ...

    def dumps(self) -> str:
        """Serialize the queue contents for persistence."""
        ...

    def loads(self, data: str) -> None:
        """Restore the queue contents from a serialized payload."""
        ...


class NodeExecutionProtocol(Protocol):
    """Structural interface for persisted per-node execution state."""

    state: NodeState
    retry_count: int
    execution_id: str | None

    def mark_started(self, execution_id: str) -> None:
        """Mark the node execution as started."""
        ...

    def mark_taken(self) -> None:
        """Mark the node execution as successfully completed."""
        ...

    def mark_failed(self, error: str) -> None:
        """Mark the node execution as failed with an error."""
        ...

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

    @property
    def node_executions(self) -> Mapping[str, NodeExecutionProtocol]:
        """Return the persisted node execution state keyed by node id."""
        ...

    def start(self) -> None:
        """Transition execution into the running state."""
        ...

    def complete(self) -> None:
        """Mark execution as successfully completed."""
        ...

    def abort(self, reason: str) -> None:
        """Abort execution in response to an external stop request."""
        ...

    def pause(self, reason: PauseReason) -> None:
        """Pause execution with a recorded reason."""
        ...

    def fail(self, error: Exception) -> None:
        """Record an unrecoverable error and end execution."""
        ...

    def record_node_failure(self) -> None:
        """Increment the count of node failures observed during execution."""
        ...

    def get_or_create_node_execution(self, node_id: str) -> NodeExecutionProtocol:
        """Return the execution entity for a node, creating it when needed."""
        ...

    @property
    def is_paused(self) -> bool:
        """Return whether the execution is currently paused."""
        ...

    @property
    def has_error(self) -> bool:
        """Return whether the execution has recorded an error."""
        ...

    def dumps(self) -> str:
        """Serialize execution state into a JSON payload."""
        ...

    def loads(self, data: str) -> None:
        """Restore execution state from a previously serialized payload."""
        ...


class ResponseStreamCoordinatorProtocol(Protocol):
    """Structural interface for response stream coordinator."""

    def register(self, response_node_id: str) -> None:
        """Register a response node so its outputs can be streamed."""
        ...

    def track_node_execution(self, node_id: str, execution_id: str) -> None:
        """Track the current execution id for a node."""
        ...

    def on_edge_taken(self, edge_id: str) -> Sequence[NodeRunStreamChunkEvent]:
        """Update pending response sessions after an edge is taken."""
        ...

    def intercept_event(
        self,
        event: NodeRunStreamChunkEvent | NodeRunSucceededEvent,
    ) -> Sequence[NodeRunStreamChunkEvent]:
        """Translate node events into streamed response events."""
        ...

    def loads(self, data: str) -> None:
        """Restore coordinator state from a serialized payload."""
        ...

    def dumps(self) -> str:
        """Serialize coordinator state for persistence."""
        ...


class NodeProtocol(Protocol):
    """Structural interface for graph nodes."""

    id: str
    state: NodeState
    execution_type: NodeExecutionType
    node_type: ClassVar[NodeType]

    def blocks_variable_output(
        self,
        variable_selectors: set[tuple[str, ...]],
    ) -> bool: ...


class EdgeProtocol(Protocol):
    id: str
    state: NodeState
    tail: str
    head: str
    source_handle: str


class GraphProtocol(Protocol):
    """Structural interface required from graph instances attached
    to the runtime state.
    """

    nodes: Mapping[str, NodeProtocol]
    edges: Mapping[str, EdgeProtocol]
    root_node: NodeProtocol

    def get_outgoing_edges(self, node_id: str) -> Sequence[EdgeProtocol]: ...


class ChildGraphEngineBuilderProtocol(Protocol):
    def build_child_engine(
        self,
        *,
        workflow_id: str,
        graph_init_params: GraphInitParams,
        parent_graph_runtime_state: GraphRuntimeState,
        root_node_id: str,
        variable_pool: VariablePool | None = None,
    ) -> Any: ...


class ChildEngineError(ValueError):
    """Base error type for child-engine creation failures."""


class ChildEngineBuilderNotConfiguredError(ChildEngineError):
    """Raised when child-engine creation is requested without a bound builder."""


class ChildGraphNotFoundError(ChildEngineError):
    """Raised when the requested child graph entry point cannot be resolved."""


class _GraphStateSnapshot(BaseModel):
    """Serializable graph state snapshot for node/edge states."""

    nodes: dict[str, NodeState] = Field(default_factory=dict)
    edges: dict[str, NodeState] = Field(default_factory=dict)


@dataclass(slots=True)
class _GraphRuntimeStateSnapshot:
    """Immutable view of a serialized runtime state snapshot."""

    start_at: float
    total_tokens: int
    node_run_steps: int
    llm_usage: LLMUsage
    outputs: dict[str, object]
    variable_pool: VariablePool
    has_variable_pool: bool
    ready_queue_dump: str | None
    graph_execution_dump: str | None
    response_coordinator_dump: str | None
    paused_nodes: tuple[str, ...]
    deferred_nodes: tuple[str, ...]
    graph_node_states: dict[str, NodeState]
    graph_edge_states: dict[str, NodeState]


@dataclass(slots=True)
class _GraphRuntimeExecutionData:
    """Owned runtime data persisted across the graph execution lifecycle."""

    variable_pool: VariablePool
    start_at: float
    total_tokens: int = 0
    llm_usage: LLMUsage = field(default_factory=LLMUsage.empty_usage)
    outputs: dict[str, Any] = field(default_factory=dict)
    node_run_steps: int = 0

    def __post_init__(self) -> None:
        if self.total_tokens < 0:
            msg = "total_tokens must be non-negative"
            raise ValueError(msg)
        if self.node_run_steps < 0:
            msg = "node_run_steps must be non-negative"
            raise ValueError(msg)
        self.llm_usage = self.llm_usage.model_copy()
        self.outputs = deepcopy(self.outputs)

    def get_llm_usage(self) -> LLMUsage:
        return self.llm_usage.model_copy()

    def set_llm_usage(self, value: LLMUsage) -> None:
        self.llm_usage = value.model_copy()

    def get_outputs(self) -> dict[str, Any]:
        return deepcopy(self.outputs)

    def set_outputs(self, value: dict[str, Any]) -> None:
        self.outputs = deepcopy(value)

    def set_output(self, key: str, value: object) -> None:
        self.outputs[key] = deepcopy(value)

    def get_output(self, key: str, default: object = None) -> object:
        return deepcopy(self.outputs.get(key, default))

    def update_outputs(self, updates: dict[str, object]) -> None:
        for key, value in updates.items():
            self.outputs[key] = deepcopy(value)

    def set_total_tokens(self, value: int) -> None:
        if value < 0:
            msg = "total_tokens must be non-negative"
            raise ValueError(msg)
        self.total_tokens = value

    def add_tokens(self, tokens: int) -> None:
        if tokens < 0:
            msg = "tokens must be non-negative"
            raise ValueError(msg)
        self.total_tokens += tokens

    def set_node_run_steps(self, value: int) -> None:
        if value < 0:
            msg = "node_run_steps must be non-negative"
            raise ValueError(msg)
        self.node_run_steps = value

    def increment_node_run_steps(self) -> None:
        self.node_run_steps += 1

    def apply_snapshot(self, snapshot: _GraphRuntimeStateSnapshot) -> None:
        self.start_at = snapshot.start_at
        self.total_tokens = snapshot.total_tokens
        self.node_run_steps = snapshot.node_run_steps
        self.llm_usage = snapshot.llm_usage.model_copy()
        self.outputs = deepcopy(snapshot.outputs)
        if snapshot.has_variable_pool:
            self.variable_pool = snapshot.variable_pool


@dataclass(slots=True)
class _GraphRuntimeSuspensionState:
    """Owned suspend/resume state that is restored around graph execution."""

    pending_response_coordinator_dump: str | None = None
    pending_graph_execution_workflow_id: str | None = None
    paused_nodes: set[str] = field(default_factory=set)
    deferred_nodes: set[str] = field(default_factory=set)
    pending_graph_node_states: dict[str, NodeState] | None = None
    pending_graph_edge_states: dict[str, NodeState] | None = None

    def register_paused_node(self, node_id: str) -> None:
        self.paused_nodes.add(node_id)

    def get_paused_nodes(self) -> list[str]:
        return list(self.paused_nodes)

    def consume_paused_nodes(self) -> list[str]:
        nodes = list(self.paused_nodes)
        self.paused_nodes.clear()
        return nodes

    def register_deferred_node(self, node_id: str) -> None:
        self.deferred_nodes.add(node_id)

    def get_deferred_nodes(self) -> list[str]:
        return list(self.deferred_nodes)

    def consume_deferred_nodes(self) -> list[str]:
        nodes = list(self.deferred_nodes)
        self.deferred_nodes.clear()
        return nodes

    def apply_snapshot(self, snapshot: _GraphRuntimeStateSnapshot) -> None:
        self.paused_nodes = set(snapshot.paused_nodes)
        self.deferred_nodes = set(snapshot.deferred_nodes)
        self.pending_graph_node_states = snapshot.graph_node_states or None
        self.pending_graph_edge_states = snapshot.graph_edge_states or None

    def snapshot_graph_state(
        self,
        graph: GraphProtocol | Graph | None,
    ) -> _GraphStateSnapshot:
        if graph is None:
            if (
                self.pending_graph_node_states is None
                and self.pending_graph_edge_states is None
            ):
                return _GraphStateSnapshot()
            return _GraphStateSnapshot(
                nodes=self.pending_graph_node_states or {},
                edges=self.pending_graph_edge_states or {},
            )

        nodes = graph.nodes
        edges = graph.edges
        if not isinstance(nodes, Mapping) or not isinstance(edges, Mapping):
            return _GraphStateSnapshot()

        node_states = {}
        for node_id, node in nodes.items():
            if isinstance(node_id, str):
                node_states[node_id] = node.state

        edge_states = {}
        for edge_id, edge in edges.items():
            if isinstance(edge_id, str):
                edge_states[edge_id] = edge.state

        return _GraphStateSnapshot(nodes=node_states, edges=edge_states)

    def apply_pending_graph_state(self, graph: GraphProtocol | Graph | None) -> None:
        if graph is None:
            return
        if self.pending_graph_node_states:
            for node_id, state in self.pending_graph_node_states.items():
                node = graph.nodes.get(node_id)
                if node is not None:
                    node.state = state
        if self.pending_graph_edge_states:
            for edge_id, state in self.pending_graph_edge_states.items():
                edge = graph.edges.get(edge_id)
                if edge is not None:
                    edge.state = state
        self.pending_graph_node_states = None
        self.pending_graph_edge_states = None


class _GraphRuntimeBindings:
    """Owned runtime collaborators and graph attachment lifecycle."""

    def __init__(
        self,
        *,
        runtime_state: GraphRuntimeState,
        ready_queue: ReadyQueueProtocol | None = None,
        graph_execution: GraphExecutionProtocol | None = None,
        response_coordinator: ResponseStreamCoordinatorProtocol | None = None,
        execution_context: AbstractContextManager[object] | None = None,
    ) -> None:
        self._runtime_state = runtime_state
        self.graph: GraphProtocol | Graph | None = None
        self.ready_queue: ReadyQueueProtocol | None = ready_queue
        self.graph_execution: GraphExecutionProtocol | None = graph_execution
        self.response_coordinator: ResponseStreamCoordinatorProtocol | None = (
            response_coordinator
        )
        self.execution_context: AbstractContextManager[object] = (
            execution_context if execution_context is not None else nullcontext(None)
        )
        self.child_engine_builder: ChildGraphEngineBuilderProtocol | None = None

    def attach_graph(
        self,
        graph: GraphProtocol | Graph,
        suspension_state: _GraphRuntimeSuspensionState,
    ) -> None:
        if self.graph is not None and self.graph is not graph:
            msg = "GraphRuntimeState already attached to a different graph instance"
            raise ValueError(msg)

        self.graph = graph

        if self.response_coordinator is None:
            self.response_coordinator = self._runtime_state.create_response_coordinator(
                graph,
            )

        if (
            suspension_state.pending_response_coordinator_dump is not None
            and self.response_coordinator is not None
        ):
            self.response_coordinator.loads(
                suspension_state.pending_response_coordinator_dump,
            )
            suspension_state.pending_response_coordinator_dump = None

        suspension_state.apply_pending_graph_state(graph)

    def configure(
        self,
        suspension_state: _GraphRuntimeSuspensionState,
        *,
        graph: GraphProtocol | Graph | None = None,
    ) -> None:
        if graph is not None:
            self.attach_graph(graph, suspension_state)

        _ = self.get_ready_queue()
        _ = self.get_graph_execution()
        if self.graph is not None:
            _ = self.get_response_coordinator()

    def bind_child_engine_builder(
        self,
        builder: ChildGraphEngineBuilderProtocol,
    ) -> None:
        self.child_engine_builder = builder

    def create_child_engine(
        self,
        *,
        workflow_id: str,
        graph_init_params: GraphInitParams,
        root_node_id: str,
        variable_pool: VariablePool | None = None,
    ) -> Any:
        if self.child_engine_builder is None:
            msg = "Child engine builder is not configured."
            raise ChildEngineBuilderNotConfiguredError(msg)

        return self.child_engine_builder.build_child_engine(
            workflow_id=workflow_id,
            graph_init_params=graph_init_params,
            parent_graph_runtime_state=self._runtime_state,
            root_node_id=root_node_id,
            variable_pool=variable_pool,
        )

    def get_ready_queue(self) -> ReadyQueueProtocol:
        if self.ready_queue is None:
            self.ready_queue = self._runtime_state.create_ready_queue()
        return self.ready_queue

    def get_graph_execution(self) -> GraphExecutionProtocol:
        if self.graph_execution is None:
            self.graph_execution = self._runtime_state.create_graph_execution()
        return self.graph_execution

    def get_response_coordinator(self) -> ResponseStreamCoordinatorProtocol:
        if self.response_coordinator is None:
            if self.graph is None:
                msg = "Graph must be attached before accessing response coordinator"
                raise ValueError(msg)
            self.response_coordinator = self._runtime_state.create_response_coordinator(
                self.graph,
            )
        return self.response_coordinator

    def set_execution_context(
        self,
        value: AbstractContextManager[object] | None,
    ) -> None:
        self.execution_context = value if value is not None else nullcontext(None)

    def restore_ready_queue(self, payload: str | None) -> None:
        if payload is None:
            self.ready_queue = None
            return
        self.ready_queue = self._runtime_state.create_ready_queue()
        self.ready_queue.loads(payload)

    def restore_graph_execution(
        self,
        payload: str | None,
        suspension_state: _GraphRuntimeSuspensionState,
    ) -> None:
        self.graph_execution = None
        suspension_state.pending_graph_execution_workflow_id = None

        if payload is None:
            return

        try:
            execution_payload = json.loads(payload)
            workflow_id = execution_payload.get("workflow_id")
            suspension_state.pending_graph_execution_workflow_id = workflow_id
        except (json.JSONDecodeError, TypeError, AttributeError):
            suspension_state.pending_graph_execution_workflow_id = None

        self.get_graph_execution().loads(payload)

    def restore_response_coordinator(
        self,
        payload: str | None,
        suspension_state: _GraphRuntimeSuspensionState,
    ) -> None:
        if payload is None:
            suspension_state.pending_response_coordinator_dump = None
            self.response_coordinator = None
            return

        if self.graph is not None:
            self.get_response_coordinator().loads(payload)
            suspension_state.pending_response_coordinator_dump = None
            return

        suspension_state.pending_response_coordinator_dump = payload
        self.response_coordinator = None


class GraphRuntimeState:  # noqa: PLR0904
    """Mutable runtime state shared across graph execution components.

    `GraphRuntimeState` encapsulates the runtime state of workflow execution,
    including scheduling details, variable values, and timing information.

    Values that are initialized prior to workflow execution and remain constant
    throughout the execution should be part of `GraphInitParams` instead.
    """

    _execution_data: _GraphRuntimeExecutionData
    _bindings: _GraphRuntimeBindings
    _suspension_state: _GraphRuntimeSuspensionState

    def __init__(
        self,
        *,
        variable_pool: VariablePool,
        start_at: float,
        total_tokens: int = 0,
        llm_usage: LLMUsage | None = None,
        outputs: dict[str, object] | None = None,
        node_run_steps: int = 0,
        ready_queue: ReadyQueueProtocol | None = None,
        graph_execution: GraphExecutionProtocol | None = None,
        response_coordinator: ResponseStreamCoordinatorProtocol | None = None,
        graph: GraphProtocol | Graph | None = None,
        execution_context: AbstractContextManager[object] | None = None,
    ) -> None:
        self._execution_data = _GraphRuntimeExecutionData(
            variable_pool=variable_pool,
            start_at=start_at,
            total_tokens=total_tokens,
            llm_usage=llm_usage or LLMUsage.empty_usage(),
            outputs=outputs or {},
            node_run_steps=node_run_steps,
        )
        self._suspension_state = _GraphRuntimeSuspensionState()
        self._bindings = _GraphRuntimeBindings(
            runtime_state=self,
            ready_queue=ready_queue,
            graph_execution=graph_execution,
            response_coordinator=response_coordinator,
            execution_context=execution_context,
        )
        if graph is not None:
            self.attach_graph(graph)

    @property
    def variable_pool(self) -> VariablePool:
        return self._execution_data.variable_pool

    @property
    def ready_queue(self) -> ReadyQueueProtocol:
        return self._bindings.get_ready_queue()

    @property
    def graph_execution(self) -> GraphExecutionProtocol:
        return self._bindings.get_graph_execution()

    @property
    def response_coordinator(self) -> ResponseStreamCoordinatorProtocol:
        return self._bindings.get_response_coordinator()

    @property
    def execution_context(self) -> AbstractContextManager[object]:
        return self._bindings.execution_context

    @execution_context.setter
    def execution_context(
        self,
        value: AbstractContextManager[object] | None,
    ) -> None:
        self._bindings.set_execution_context(value)

    @property
    def start_at(self) -> float:
        return self._execution_data.start_at

    @start_at.setter
    def start_at(self, value: float) -> None:
        self._execution_data.start_at = value

    @property
    def total_tokens(self) -> int:
        return self._execution_data.total_tokens

    @total_tokens.setter
    def total_tokens(self, value: int) -> None:
        self._execution_data.set_total_tokens(value)

    @property
    def llm_usage(self) -> LLMUsage:
        return self._execution_data.get_llm_usage()

    @llm_usage.setter
    def llm_usage(self, value: LLMUsage) -> None:
        self._execution_data.set_llm_usage(value)

    @property
    def outputs(self) -> dict[str, Any]:
        return self._execution_data.get_outputs()

    @outputs.setter
    def outputs(self, value: dict[str, Any]) -> None:
        self._execution_data.set_outputs(value)

    def set_output(self, key: str, value: object) -> None:
        self._execution_data.set_output(key, value)

    def get_output(self, key: str, default: object = None) -> object:
        return self._execution_data.get_output(key, default)

    def update_outputs(self, updates: dict[str, object]) -> None:
        self._execution_data.update_outputs(updates)

    def merge_response_outputs(self, outputs: Mapping[str, object]) -> None:
        for key, value in outputs.items():
            if key == "answer":
                existing = self._execution_data.get_output("answer", "")
                if existing:
                    self._execution_data.set_output("answer", f"{existing}{value}")
                else:
                    self._execution_data.set_output("answer", value)
                continue
            self._execution_data.set_output(key, value)

    @property
    def node_run_steps(self) -> int:
        return self._execution_data.node_run_steps

    @node_run_steps.setter
    def node_run_steps(self, value: int) -> None:
        self._execution_data.set_node_run_steps(value)

    def increment_node_run_steps(self) -> None:
        self._execution_data.increment_node_run_steps()

    def add_tokens(self, tokens: int) -> None:
        self._execution_data.add_tokens(tokens)

    def attach_graph(self, graph: GraphProtocol | Graph) -> None:
        """Attach the materialized graph to the runtime state."""
        self._bindings.attach_graph(graph, self._suspension_state)

    def configure(
        self,
        *,
        graph: GraphProtocol | Graph | None = None,
    ) -> None:
        """Ensure core collaborators are initialized with the provided context."""
        self._bindings.configure(self._suspension_state, graph=graph)

    def bind_child_engine_builder(
        self,
        builder: ChildGraphEngineBuilderProtocol,
    ) -> None:
        self._bindings.bind_child_engine_builder(builder)

    def create_child_engine(
        self,
        *,
        workflow_id: str,
        graph_init_params: GraphInitParams,
        root_node_id: str,
        variable_pool: VariablePool | None = None,
    ) -> Any:
        """Create a child graph engine whose runtime state derives from this parent."""
        return self._bindings.create_child_engine(
            workflow_id=workflow_id,
            graph_init_params=graph_init_params,
            root_node_id=root_node_id,
            variable_pool=variable_pool,
        )

    def dumps(self) -> str:
        """Serialize runtime state into a JSON string."""
        snapshot: dict[str, Any] = {
            "version": "1.0",
            "start_at": self._execution_data.start_at,
            "total_tokens": self._execution_data.total_tokens,
            "node_run_steps": self._execution_data.node_run_steps,
            "llm_usage": self._execution_data.llm_usage.model_dump(mode="json"),
            "outputs": self._execution_data.get_outputs(),
            "variable_pool": self.variable_pool.model_dump(mode="json"),
            "ready_queue": self.ready_queue.dumps(),
            "graph_execution": self.graph_execution.dumps(),
            "paused_nodes": list(self._suspension_state.paused_nodes),
            "deferred_nodes": list(self._suspension_state.deferred_nodes),
        }

        snapshot["graph_state"] = self._suspension_state.snapshot_graph_state(
            self._bindings.graph,
        )

        if (
            self._bindings.response_coordinator is not None
            and self._bindings.graph is not None
        ):
            snapshot["response_coordinator"] = (
                self._bindings.response_coordinator.dumps()
            )

        return json.dumps(to_jsonable_python(snapshot))

    @classmethod
    def from_snapshot(
        cls: type[GraphRuntimeState],
        data: str | Mapping[str, Any],
    ) -> GraphRuntimeState:
        """Restore runtime state from a serialized snapshot."""
        snapshot = cls._parse_snapshot_payload(data)

        state = cls(
            variable_pool=snapshot.variable_pool,
            start_at=snapshot.start_at,
            total_tokens=snapshot.total_tokens,
            llm_usage=snapshot.llm_usage,
            outputs=snapshot.outputs,
            node_run_steps=snapshot.node_run_steps,
        )
        state._apply_snapshot(snapshot)
        return state

    def loads(self, data: str | Mapping[str, Any]) -> None:
        """Restore runtime state from a serialized snapshot (legacy API)."""
        snapshot = self._parse_snapshot_payload(data)
        self._apply_snapshot(snapshot)

    def register_paused_node(self, node_id: str) -> None:
        """Record a node that should resume when execution is continued."""
        self._suspension_state.register_paused_node(node_id)

    def get_paused_nodes(self) -> list[str]:
        """Retrieve the list of paused nodes without mutating internal state."""
        return self._suspension_state.get_paused_nodes()

    def consume_paused_nodes(self) -> list[str]:
        """Retrieve and clear the list of paused nodes awaiting resume."""
        return self._suspension_state.consume_paused_nodes()

    def register_deferred_node(self, node_id: str) -> None:
        """Record a node that became ready during pause and should resume later."""
        self._suspension_state.register_deferred_node(node_id)

    def get_deferred_nodes(self) -> list[str]:
        """Retrieve deferred nodes without mutating internal state."""
        return self._suspension_state.get_deferred_nodes()

    def consume_deferred_nodes(self) -> list[str]:
        """Retrieve and clear deferred nodes awaiting resume."""
        return self._suspension_state.consume_deferred_nodes()

    # ------------------------------------------------------------------
    # Builders
    # ------------------------------------------------------------------
    def create_ready_queue(self) -> ReadyQueueProtocol:
        """Create the ready queue collaborator used by this runtime state."""
        return self._build_ready_queue()

    def create_graph_execution(self) -> GraphExecutionProtocol:
        """Create the graph execution aggregate used by this runtime state."""
        return self._build_graph_execution()

    def create_response_coordinator(
        self,
        graph: GraphProtocol | Graph,
    ) -> ResponseStreamCoordinatorProtocol:
        """Create the response coordinator bound to the attached graph."""
        return self._build_response_coordinator(graph)

    def _build_ready_queue(self) -> ReadyQueueProtocol:
        # Import lazily to avoid breaching architecture boundaries
        # enforced by import-linter.
        module = importlib.import_module("graphon.graph_engine.ready_queue")
        in_memory_cls = module.InMemoryReadyQueue
        return in_memory_cls()

    def _build_graph_execution(self) -> GraphExecutionProtocol:
        # Lazily import to keep the runtime domain decoupled from graph_engine modules.
        module = importlib.import_module("graphon.graph_engine.domain.graph_execution")
        graph_execution_cls = module.GraphExecution
        workflow_id = self._suspension_state.pending_graph_execution_workflow_id or ""
        self._suspension_state.pending_graph_execution_workflow_id = None
        return graph_execution_cls(workflow_id=workflow_id)

    def _build_response_coordinator(
        self,
        graph: GraphProtocol | Graph,
    ) -> ResponseStreamCoordinatorProtocol:
        # Lazily import to keep the runtime domain decoupled from graph_engine modules.
        module = importlib.import_module("graphon.graph_engine.response_coordinator")
        coordinator_cls = module.ResponseStreamCoordinator
        return coordinator_cls(variable_pool=self.variable_pool, graph=graph)

    # ------------------------------------------------------------------
    # Snapshot helpers
    # ------------------------------------------------------------------
    @classmethod
    def _parse_snapshot_payload(
        cls,
        data: str | Mapping[str, Any],
    ) -> _GraphRuntimeStateSnapshot:
        payload: dict[str, Any]
        payload = json.loads(data) if isinstance(data, str) else dict(data)

        version = payload.get("version")
        if version != "1.0":
            msg = f"Unsupported GraphRuntimeState snapshot version: {version}"
            raise ValueError(msg)

        start_at = float(payload.get("start_at", 0.0))

        total_tokens = int(payload.get("total_tokens", 0))
        if total_tokens < 0:
            msg = "total_tokens must be non-negative"
            raise ValueError(msg)

        node_run_steps = int(payload.get("node_run_steps", 0))
        if node_run_steps < 0:
            msg = "node_run_steps must be non-negative"
            raise ValueError(msg)

        llm_usage = LLMUsage.model_validate(payload.get("llm_usage", {}))

        outputs_payload = deepcopy(payload.get("outputs", {}))

        variable_pool_payload = payload.get("variable_pool")
        has_variable_pool = variable_pool_payload is not None
        variable_pool = (
            VariablePool.model_validate(variable_pool_payload)
            if has_variable_pool
            else VariablePool()
        )

        graph_state_payload = payload.get("graph_state", {}) or {}
        graph_node_states = _coerce_graph_state_map(graph_state_payload, "nodes")
        graph_edge_states = _coerce_graph_state_map(graph_state_payload, "edges")

        return _GraphRuntimeStateSnapshot(
            start_at=start_at,
            total_tokens=total_tokens,
            node_run_steps=node_run_steps,
            llm_usage=llm_usage,
            outputs=outputs_payload,
            variable_pool=variable_pool,
            has_variable_pool=has_variable_pool,
            ready_queue_dump=payload.get("ready_queue"),
            graph_execution_dump=payload.get("graph_execution"),
            response_coordinator_dump=payload.get("response_coordinator"),
            paused_nodes=tuple(map(str, payload.get("paused_nodes", []))),
            deferred_nodes=tuple(map(str, payload.get("deferred_nodes", []))),
            graph_node_states=graph_node_states,
            graph_edge_states=graph_edge_states,
        )

    def _apply_snapshot(self, snapshot: _GraphRuntimeStateSnapshot) -> None:
        self._execution_data.apply_snapshot(snapshot)
        self._bindings.restore_ready_queue(snapshot.ready_queue_dump)
        self._bindings.restore_graph_execution(
            snapshot.graph_execution_dump,
            self._suspension_state,
        )
        self._bindings.restore_response_coordinator(
            snapshot.response_coordinator_dump,
            self._suspension_state,
        )
        self._suspension_state.apply_snapshot(snapshot)
        self._suspension_state.apply_pending_graph_state(self._bindings.graph)


def _coerce_graph_state_map(payload: Any, key: str) -> dict[str, NodeState]:
    if not isinstance(payload, Mapping):
        return {}
    raw_map = payload.get(key, {})
    if not isinstance(raw_map, Mapping):
        return {}
    result: dict[str, NodeState] = {}
    for node_id, raw_state in raw_map.items():
        if not isinstance(node_id, str):
            continue
        try:
            result[node_id] = NodeState(str(raw_state))
        except ValueError:
            continue
    return result
