from __future__ import annotations

import threading
from abc import abstractmethod
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from copy import deepcopy
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal, Protocol

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, TypeAdapter

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


class _RuntimeStateSnapshotV1(BaseModel):
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


class _RuntimeStateSnapshot(BaseModel):
    """Validated serialized runtime state snapshot."""

    model_config = ConfigDict(frozen=True)

    version: Literal["3.0"]
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
    compatibility_marker: object | None = Field(default=None, exclude=True, repr=False)


_V2_SNAPSHOT_MARKER = object()


def _normalize_v2_snapshot(value: object) -> object:
    """Normalize one version 2 runtime snapshot for the version 3 reader.

    Version 2 and version 3 share the same outer payload shape, but version 2
    edge-state keys are positional ``edge_N`` values while version 3 keys are
    public DSL edge IDs. Pydantic runs this function before discriminating the
    snapshot union, so it can reuse the version 3 schema without exposing a
    second long-lived snapshot model. The in-memory marker cannot be supplied by
    JSON and tells :class:`RuntimeState` to defer edge migration until the full
    graph configuration is attached.

    TODO(runtime-snapshot-v2): Remove this validator, ``_V2_SNAPSHOT_MARKER``,
    the excluded ``compatibility_marker`` field, and RuntimeState's legacy
    migration branch together when restoring version 1/2 snapshots is no longer
    supported. None of these details are part of serialized version 3 data or the
    public graph model.

    Args:
        value: Parsed JSON value presented to the snapshot adapter.

    Returns:
        A shallow version 3-compatible copy for version 2 input, otherwise the
        original value unchanged.

    """
    if not isinstance(value, Mapping) or value.get("version") != "2.0":
        return value
    normalized = dict(value)
    normalized["version"] = "3.0"
    normalized["compatibility_marker"] = _V2_SNAPSHOT_MARKER
    return normalized


_RUNTIME_STATE_SNAPSHOT_ADAPTER = TypeAdapter(
    Annotated[
        Annotated[
            _RuntimeStateSnapshotV1 | _RuntimeStateSnapshot,
            Field(discriminator="version"),
        ],
        BeforeValidator(_normalize_v2_snapshot),
    ],
)


def _legacy_edges_by_owner(
    raw_edges: list[object],
    node_owners: Mapping[str, str],
) -> dict[str, dict[str, str]]:
    """Map legacy positional edge IDs to frame-local public edge IDs.

    The ordinal advances only for entries with string ``source`` and ``target``
    values, exactly matching the version 2 graph builder. Public IDs are checked
    within their owning graph rather than globally because sibling graphs may
    intentionally reuse the same edge ID.

    Args:
        raw_edges: Full graph edge list in persisted DSL order.
        node_owners: Direct container owner indexed by node ID.

    Returns:
        Positional-to-public edge mappings grouped by direct container ID.

    Raises:
        RuntimeError: If an edge cannot be assigned unambiguously to one graph.

    """
    edges_by_owner: dict[str, dict[str, str]] = {}
    edge_ordinal = 0
    for edge_config in raw_edges:
        if not isinstance(edge_config, Mapping):
            continue
        source = edge_config.get("source")
        target = edge_config.get("target")
        if not isinstance(source, str) or not isinstance(target, str):
            continue
        legacy_edge_id = f"edge_{edge_ordinal}"
        edge_ordinal += 1
        if not isinstance(edge_config.get("sourceHandle", "source"), str):
            continue
        public_edge_id = edge_config.get("id")
        if not isinstance(public_edge_id, str) or not public_edge_id:
            msg = f"Legacy {legacy_edge_id} has no public DSL edge ID"
            raise RuntimeError(msg)
        source_owner = node_owners.get(source)
        target_owner = node_owners.get(target)
        if source_owner is None or target_owner is None or source_owner != target_owner:
            msg = f"Legacy {legacy_edge_id} does not belong to one graph scope"
            raise RuntimeError(msg)
        scoped_edges = edges_by_owner.setdefault(source_owner, {})
        if public_edge_id in scoped_edges.values():
            msg = (
                f"Duplicate edge ID {public_edge_id!r} in graph scope {source_owner!r}"
            )
            raise RuntimeError(msg)
        scoped_edges[legacy_edge_id] = public_edge_id
    return edges_by_owner


def _legacy_graph_scopes(
    graph_config: Mapping[str, Any],
) -> tuple[dict[str, set[str]], dict[str, dict[str, str]]]:
    """Build frame-local node and edge identities for a legacy snapshot.

    Version 2 stored full-graph state in every frame while current edge IDs are
    local to one graph. This compatibility helper groups nodes and positional
    edges by their direct owner so each frame can be converted independently.

    This helper is intentionally private and used only by the removable version
    1/2 compatibility path described in :func:`_normalize_v2_snapshot`.

    Args:
        graph_config: Full root graph configuration after normal graph import.

    Returns:
        Node IDs by direct container ID and, for each direct container, a mapping
        from legacy positional edge ID to public DSL edge ID.

    Raises:
        TypeError: If graph config does not contain node and edge lists.

    """
    from graphon.graph.scoping import (  # ruff:ignore[import-outside-top-level]
        resolve_container_id,
    )

    raw_nodes = graph_config.get("nodes", [])
    raw_edges = graph_config.get("edges", [])
    if not isinstance(raw_nodes, list) or not isinstance(raw_edges, list):
        msg = "Attached graph config cannot migrate legacy runtime state"
        raise TypeError(msg)

    nodes_by_id = {
        node_id: node_config
        for node_config in raw_nodes
        if isinstance(node_config, Mapping)
        and isinstance((node_id := node_config.get("id")), str)
    }
    node_owners: dict[str, str] = {}
    nodes_by_owner: dict[str, set[str]] = {}
    for node_id, node_config in nodes_by_id.items():
        owner = resolve_container_id(node_config, nodes_by_id=nodes_by_id)
        node_owners[node_id] = owner
        nodes_by_owner.setdefault(owner, set()).add(node_id)
    return nodes_by_owner, _legacy_edges_by_owner(raw_edges, node_owners)


def _select_legacy_scope_state(
    *,
    owner: str,
    node_states: Mapping[str, NodeState],
    edge_states: Mapping[str, NodeState],
    nodes_by_owner: Mapping[str, set[str]],
    edges_by_owner: Mapping[str, Mapping[str, str]],
) -> tuple[dict[str, NodeState], dict[str, NodeState]]:
    """Convert and select one frame's state from a legacy full-graph snapshot.

    Legacy snapshots may contain states for parent, child, and sibling graphs in
    every frame. Only entries owned by ``owner`` are retained. Edge keys are
    converted from their full-graph positional IDs to the public IDs local to
    that frame. All required source entries are checked before any RuntimeState
    field is changed, which lets the caller replace every frame atomically.

    This helper belongs exclusively to the removable version 1/2 compatibility
    path documented in :func:`_normalize_v2_snapshot`.

    Args:
        owner: Direct container node ID, or an empty string for the root graph.
        node_states: Saved node states, possibly covering the complete graph.
        edge_states: Saved positional edge states, possibly covering all scopes.
        nodes_by_owner: Node identities grouped by direct container ID.
        edges_by_owner: Positional-to-public edge mappings grouped by owner.

    Returns:
        Exact node and edge state dictionaries for the requested graph scope.

    Raises:
        RuntimeError: If the legacy snapshot omits required scope state or
            contains identities absent from the attached graph definition.

    """
    expected_nodes = nodes_by_owner.get(owner, set())
    scoped_edges = edges_by_owner.get(owner, {})
    missing_nodes = expected_nodes.difference(node_states)
    missing_edges = set(scoped_edges).difference(edge_states)
    known_nodes = set().union(*nodes_by_owner.values())
    known_edges = {
        legacy_edge_id for scope in edges_by_owner.values() for legacy_edge_id in scope
    }
    unknown_nodes = set(node_states).difference(known_nodes)
    unknown_edges = set(edge_states).difference(known_edges)
    if missing_nodes or missing_edges or unknown_nodes or unknown_edges:
        msg = (
            f"Legacy graph state does not match scope {owner!r}: "
            f"missing_nodes={sorted(missing_nodes)}, "
            f"missing_edges={sorted(missing_edges)}, "
            f"unknown_nodes={sorted(unknown_nodes)}, "
            f"unknown_edges={sorted(unknown_edges)}"
        )
        raise RuntimeError(msg)
    return (
        {node_id: node_states[node_id] for node_id in expected_nodes},
        {
            public_edge_id: edge_states[legacy_edge_id]
            for legacy_edge_id, public_edge_id in scoped_edges.items()
        },
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
        self._legacy_snapshot_version: Literal["1.0", "2.0"] | None = None
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
        scoped graph. This method records the exact node and edge mappings for
        that frame; :meth:`attach_graph` rejects both missing and unrelated
        entries before the frame becomes executable. Legacy full-graph snapshots
        are the only exception and are normalized by the private compatibility
        path before this exact check. Staging is rejected after graph attachment
        so callers cannot overwrite a live graph accidentally.

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
        """Attach the materialized graph and apply any staged snapshot state.

        Current version 3 snapshots must match this graph exactly. Versions 1
        and 2 are the sole compatibility exception: before validation they are
        atomically converted from full-graph ``edge_N`` maps into exact,
        frame-local maps using the attached root graph configuration. Keeping
        that exception behind ``_legacy_snapshot_version`` prevents relaxed
        validation from affecting newly written snapshots or normal callers.

        TODO(runtime-snapshot-v2): Delete the legacy branch together with the
        validator and helpers documented by :func:`_normalize_v2_snapshot` once
        old snapshots no longer need to resume.

        Args:
            graph: Materialized graph owned by this runtime frame.

        Raises:
            ValueError: If another graph instance is already attached.
            RuntimeError: If saved state cannot be migrated or does not exactly
                match the attached graph after migration.

        """
        if self._graph is not None and self._graph is not graph:
            msg = "RuntimeState already attached to a different graph instance"
            raise ValueError(msg)
        if self._legacy_snapshot_version is not None:
            self._migrate_legacy_graph_state(graph)
        if self._has_pending_graph_state and (
            set(graph.nodes) != set(self._pending_graph_node_states)
            or set(graph.edges) != set(self._pending_graph_edge_states)
        ):
            msg = "Saved graph state does not match rebuilt graph"
            raise RuntimeError(msg)
        self._graph = graph
        self._apply_pending_graph_state()

    def _migrate_legacy_graph_state(
        self,
        graph: GraphProtocol,
    ) -> None:
        """Migrate every saved legacy frame against one full graph definition.

        Migration cannot run in the Pydantic validator because positional
        ``edge_N`` keys only become meaningful once the persisted workflow graph
        is rebuilt. The root graph retains the complete container tree, allowing
        this method to map and trim root state plus every saved container frame
        in one pass. All replacement models are computed before assignment, so a
        malformed later frame leaves the RuntimeState wholly in its legacy form.
        Programmatically built graphs have no graph config; when they contain no
        saved child frames, exact node and edge key equality proves that their
        generated ``edge_N`` identities need no translation.

        This method is private compatibility code. Remove it together with
        ``_legacy_snapshot_version`` and the version 2 adapter validator when
        version 1/2 snapshot restoration is retired.

        Args:
            graph: Materialized root graph used to validate and translate state.

        Raises:
            RuntimeError: If graph config is required but unavailable, a saved
                frame has no matching container run, or any scope cannot be
                converted fully.

        """
        graph_config = getattr(graph, "graph_config", None)
        if graph_config is None:
            if not self._container_frames and (
                set(graph.nodes) == set(self._pending_graph_node_states)
                and set(graph.edges) == set(self._pending_graph_edge_states)
            ):
                self._legacy_snapshot_version = None
                return
            msg = "Attached graph must retain graph_config to migrate legacy state"
            raise RuntimeError(msg)
        nodes_by_owner, edges_by_owner = _legacy_graph_scopes(graph_config)
        root_node_states, root_edge_states = _select_legacy_scope_state(
            owner="",
            node_states=self._pending_graph_node_states,
            edge_states=self._pending_graph_edge_states,
            nodes_by_owner=nodes_by_owner,
            edges_by_owner=edges_by_owner,
        )
        if set(graph.nodes) != set(root_node_states) or set(graph.edges) != set(
            root_edge_states
        ):
            msg = "Saved graph state does not match rebuilt graph"
            raise RuntimeError(msg)

        if self._legacy_snapshot_version == "1.0":
            from graphon.engine.ready_queue import (  # ruff:ignore[import-outside-top-level]
                StartTask,
            )

            # V1 stored only node IDs from child pauses. It did not store the
            # child runtime, owning invocation, Loop round, Iteration item, or
            # suspended container continuation. Running such an ID in the old
            # full root graph silently reported success without finishing its
            # container, so reject it before workers can lose the task or hang.
            invalid_tasks: list[str] = []
            for task_queue in (self._ready_queue, self._deferred_ready_queue):
                tasks = task_queue.take_all()
                try:
                    invalid_tasks.extend(
                        (
                            f"{task.frame_id}:{task.node_id}"
                            if isinstance(task, StartTask)
                            else task.kind
                        )
                        for task in tasks
                        if not isinstance(task, StartTask)
                        or task.frame_id != ROOT_FRAME_ID
                        or task.node_id not in graph.nodes
                    )
                finally:
                    for task in tasks:
                        task_queue.put(task)
            if invalid_tasks:
                msg = (
                    "Version 1 snapshot contains child-frame tasks that cannot "
                    "be restored without frame state: "
                    f"{invalid_tasks}"
                )
                raise RuntimeError(msg)

        with self._container_state_lock:
            migrated_frames: dict[str, ContainerFrameState] = {}
            for frame in self._container_frames.values():
                run = self._container_runs.get(frame.parent_invocation_id)
                if run is None:
                    msg = (
                        f"Legacy frame {frame.frame_id!r} has no matching container run"
                    )
                    raise RuntimeError(msg)
                node_states, edge_states = _select_legacy_scope_state(
                    owner=run.node_id,
                    node_states=frame.runtime_data.graph_node_states,
                    edge_states=frame.runtime_data.graph_edge_states,
                    nodes_by_owner=nodes_by_owner,
                    edges_by_owner=edges_by_owner,
                )
                runtime_data = frame.runtime_data.model_copy(
                    update={
                        "graph_node_states": node_states,
                        "graph_edge_states": edge_states,
                    },
                )
                migrated_frames[frame.frame_id] = frame.model_copy(
                    update={"runtime_data": runtime_data},
                )

            self._pending_graph_node_states = root_node_states
            self._pending_graph_edge_states = root_edge_states
            self._container_frames = migrated_frames
            self._legacy_snapshot_version = None

    def _apply_pending_graph_state(self) -> None:
        if self._graph is None or not self._has_pending_graph_state:
            return
        for node_id, node in self._graph.nodes.items():
            node.state = self._pending_graph_node_states[node_id]
        for edge_id, edge in self._graph.edges.items():
            edge.state = self._pending_graph_edge_states[edge_id]
        self._pending_graph_node_states.clear()
        self._pending_graph_edge_states.clear()
        self._has_pending_graph_state = False

    def dumps(self) -> str:
        """Serialize runtime state into a version 3 JSON string.

        A restored version 1/2 state must first attach its graph so positional
        edge IDs can be converted safely. Refusing to serialize before that point
        prevents an unmigrated legacy map from being mislabeled as version 3.

        Returns:
            The complete version 3 runtime snapshot as JSON.

        Raises:
            RuntimeError: If a legacy snapshot has not yet attached its graph.

        """
        if self._legacy_snapshot_version is not None:
            msg = "Legacy runtime state must attach its graph before serialization"
            raise RuntimeError(msg)
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
        return _RuntimeStateSnapshot(
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
        snapshot = _RUNTIME_STATE_SNAPSHOT_ADAPTER.validate_json(data)

        ready_queue = ready_queue_factory()
        ready_queue.loads(snapshot.ready_queue)
        deferred_ready_queue = ready_queue_factory()
        if isinstance(snapshot, _RuntimeStateSnapshotV1):
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
        # Snapshots written before graph attachment contain two empty mappings.
        # They carry no graph state to validate or migrate, so preserve the
        # original ability to attach any valid graph after restoration. Keep
        # this compatibility rule at the deserialization boundary rather than
        # weakening restore_graph_state() for explicit callers.
        if graph_node_states or graph_edge_states:
            if isinstance(snapshot, _RuntimeStateSnapshotV1):
                state._legacy_snapshot_version = "1.0"
            elif snapshot.compatibility_marker is _V2_SNAPSHOT_MARKER:
                state._legacy_snapshot_version = "2.0"
            state.restore_graph_state(
                node_states=graph_node_states,
                edge_states=graph_edge_states,
            )
        return state

    def defer_ready_task(self, task: ReadyTask) -> None:
        self._deferred_ready_queue.put(task)

    def take_deferred_ready_tasks(self) -> list[ReadyTask]:
        """Remove and return every task deferred while execution was paused.

        Returns:
            Deferred ready tasks in the order they were originally queued.

        """
        return self._deferred_ready_queue.take_all()

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
