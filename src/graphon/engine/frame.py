"""Execution frame storage and construction."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal, final

from graphon.engine_events.frame import (
    FrameFinishedEvent,
    FrameStartedEvent,
    FrameVariablesUpdatedEvent,
)
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.graph.graph import Graph
from graphon.runtime.container_state import CustomContainerFrameState, FrameRuntimeData
from graphon.runtime.execution import ROOT_FRAME_ID
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import Variable

from .event.node_failure import NodeFailureHandler
from .event.stream import EventStream
from .scheduler import Scheduler


@dataclass(frozen=True, slots=True)
class ExecutionFrame:
    frame_id: str
    graph: Graph
    state: RuntimeState
    scheduler: Scheduler
    failure_handler: NodeFailureHandler
    container_id: str = ""


@final
class FrameRegistry:
    def __init__(self, event_stream: EventStream | None = None) -> None:
        self._frames: dict[str, ExecutionFrame] = {}
        self._event_stream = event_stream
        self._open_frame_ids: set[str] = set()
        self._frame_errors: dict[str, str] = {}

    def register(self, frame: ExecutionFrame) -> None:
        graph_config = frame.graph.graph_config or {}
        frame.state.variable_pool.node_ids |= frame.graph.nodes.keys() | {
            node_config["id"]
            for node_config in graph_config.get("nodes", [])
            if "id" in node_config
        }
        self._frames[frame.frame_id] = frame
        frame.scheduler.before_schedule = self.start_frame

    def start_frame(
        self,
        frame_id: str,
        *,
        reason: WorkflowStartReason = WorkflowStartReason.INITIAL,
    ) -> None:
        """Publish setup once, including for handlers using the existing scheduler."""
        if self._event_stream is None or frame_id in self._open_frame_ids:
            return
        frame = self[frame_id]
        root_state = self[ROOT_FRAME_ID].state
        execution = root_state.graph_execution
        parent_frame_id = invocation_id = container_execution_id = None
        root_node_id = frame.graph.root_node.id
        index = None
        if frame_id != ROOT_FRAME_ID:
            frame_state = root_state.get_container_frame(frame_id)
            run_state = root_state.get_container_run(frame_state.parent_invocation_id)
            parent_frame_id = run_state.frame_id
            invocation_id = run_state.invocation_id
            self.start_frame(parent_frame_id, reason=reason)
            container_execution_id = execution.get_or_create_node_execution(
                frame_id=parent_frame_id,
                node_id=run_state.node_id,
            ).execution_id
            root_node_id = getattr(frame_state, "root_node_id", None)
            index = getattr(frame_state, "index", None)
            errors = getattr(frame_state, "errors", ())
            if errors:
                self.record_failure(frame_id, errors[0])
        self._open_frame_ids.add(frame_id)
        variables_by_node = frame.state.variable_pool.variable_dictionary
        self._event_stream.collect(
            FrameStartedEvent(
                frame_id=frame_id,
                container_id=frame.container_id,
                parent_frame_id=parent_frame_id,
                invocation_id=invocation_id,
                container_execution_id=container_execution_id,
                root_node_id=root_node_id,
                index=index,
                reason=reason,
                node_execution_ids={
                    node_id: execution.get_or_create_node_execution(
                        frame_id=frame_id,
                        node_id=node_id,
                    ).execution_id
                    for node_id in frame.graph.nodes
                },
                variables=tuple(
                    variable.model_copy(update={"selector": [node_id, name]})
                    for node_id, variables in variables_by_node.items()
                    for name, variable in variables.items()
                ),
            )
        )

    def reset_observations(self) -> None:
        """Restored frames receive fresh baselines without changing persisted IDs."""
        self._open_frame_ids.clear()

    def record_failure(self, frame_id: str, error: str) -> None:
        self._frame_errors.setdefault(frame_id, error)
        if frame_id == ROOT_FRAME_ID:
            return
        root_state = self[ROOT_FRAME_ID].state
        frame_state = root_state.get_container_frame(frame_id)
        if (
            isinstance(frame_state, CustomContainerFrameState)
            and not frame_state.errors
        ):
            root_state.put_container_frame(
                frame_state.model_copy(update={"errors": (error,)}),
            )

    def finish_frame(
        self,
        frame_id: str,
        *,
        outcome: Literal["succeeded", "failed", "aborted"] | None = None,
        error: str | None = None,
    ) -> None:
        if self._event_stream is None or frame_id not in self._open_frame_ids:
            return
        error = self._frame_errors.pop(frame_id, error)
        self._event_stream.collect(
            FrameFinishedEvent(
                frame_id=frame_id,
                container_id=self[frame_id].container_id,
                outcome=outcome or ("failed" if error is not None else "succeeded"),
                error=error,
            )
        )
        self._open_frame_ids.remove(frame_id)

    def finish_frames(self) -> None:
        """Close remaining observations at a terminal run boundary, never a pause."""
        if self._event_stream is None:
            return
        execution = self[ROOT_FRAME_ID].state.graph_execution
        outcome: Literal["succeeded", "failed", "aborted"]
        if execution.aborted:
            outcome = "aborted"
        elif execution.error is not None:
            outcome = "failed"
        elif execution.paused:
            return
        else:
            outcome = "succeeded"
        for frame in reversed(self.frames()):
            self.finish_frame(
                frame.frame_id,
                outcome=outcome,
                error=str(execution.error) if execution.error is not None else None,
            )

    def update_variables(
        self,
        frame_id: str,
        updates: Iterable[tuple[Sequence[str], object]],
    ) -> None:
        """Commit once and publish actual writes to every frame sharing the pool."""
        pool = self[frame_id].state.variable_pool
        frames = [frame for frame in self.frames() if frame.state.variable_pool is pool]
        for frame in frames:
            self.start_frame(frame.frame_id)
        committed_variables: list[Variable] = []
        try:
            for selector, value in updates:
                pool.add(selector, value)
                if self._event_stream is not None:
                    variable = pool.variable_dictionary[selector[0]][selector[1]]
                    committed_variables.append(
                        variable.model_copy(
                            deep=True,
                            update={"selector": list(selector)},
                        )
                    )
        finally:
            # A later rejected write must not hide preceding successful writes.
            if committed_variables and self._event_stream is not None:
                for frame in frames:
                    self._event_stream.collect(
                        FrameVariablesUpdatedEvent(
                            frame_id=frame.frame_id,
                            container_id=frame.container_id,
                            variables=tuple(committed_variables),
                        )
                    )

    def __getitem__(self, frame_id: str) -> ExecutionFrame:
        """Return a registered frame by its required identifier.

        Frame lookups are never optional during execution. Using subscription
        syntax makes the existing ``KeyError`` behavior explicit instead of
        resembling ``dict.get()``, which conventionally returns ``None``.

        Args:
            frame_id: Identifier of the frame to retrieve.

        Returns:
            The registered execution frame.

        """
        return self._frames[frame_id]

    def frames(self) -> tuple[ExecutionFrame, ...]:
        """Return an immutable snapshot of every currently live frame.

        The dispatcher owns frame creation, removal, and command processing, so
        callers can safely traverse this tuple without exposing the registry's
        mutable mapping. The snapshot is used for workflow-level operations that
        must affect root and descendant frames consistently.

        Returns:
            Root and child execution frames registered at call time.

        """
        return tuple(self._frames.values())

    def remove(self, frame_id: str) -> None:
        self.finish_frame(frame_id)
        del self._frames[frame_id]

    def create(
        self,
        *,
        frame_id: str,
        container_id: str,
        graph: Graph,
        state: RuntimeState,
    ) -> ExecutionFrame:
        """Create and register a fully wired frame.

        The runtime state owns graph attachment and any pending snapshot
        restoration. This method only creates the frame-local scheduler and
        failure handler after that attachment succeeds, so an invalid snapshot
        can never leave a partially registered frame behind.

        Args:
            frame_id: Unique frame ID used by scheduled tasks.
            container_id: Direct owning container node ID; root uses ``""``.
            graph: Graph structure executable by this frame.
            state: Mutable runtime state owned by this graph execution.

        Returns:
            The registered, fully wired execution frame.

        """
        state.attach_graph(graph)
        scheduler = Scheduler(
            graph,
            state,
            frame_id,
        )
        frame = ExecutionFrame(
            frame_id=frame_id,
            graph=graph,
            state=state,
            scheduler=scheduler,
            failure_handler=NodeFailureHandler(graph, state.graph_execution),
            container_id=container_id,
        )
        self.register(frame)
        return frame

    def create_child(
        self,
        *,
        frame_id: str,
        parent_frame_id: str,
        container_id: str,
        root_node_id: str,
        variable_pool: VariablePool,
    ) -> ExecutionFrame:
        """Create a child frame from its parent's container-scoped graph.

        The child receives an independent variable pool and runtime counters,
        while queues and graph-wide execution state are inherited from the
        parent. Only the graph config owned by ``container_id`` is materialized.

        Args:
            frame_id: Unique ID for the new child frame.
            parent_frame_id: Frame whose scoped config contains the child.
            container_id: Container node that directly owns the child graph.
            root_node_id: Entry node within the child graph.
            variable_pool: Variable pool owned by the new child state.

        Returns:
            The registered child execution frame.

        """
        parent_state = self[parent_frame_id].state
        state = self._create_child_state(
            parent_state=parent_state,
            variable_pool=variable_pool,
        )
        return self._create_child_with_state(
            frame_id=frame_id,
            parent_frame_id=parent_frame_id,
            container_id=container_id,
            root_node_id=root_node_id,
            state=state,
        )

    def restore_child(
        self,
        *,
        frame_id: str,
        parent_frame_id: str,
        container_id: str,
        root_node_id: str,
        runtime_data: FrameRuntimeData,
        variable_pool: VariablePool,
    ) -> ExecutionFrame:
        """Restore a child frame from container-agnostic runtime data.

        Container handlers extract their own frame-specific fields before
        calling this method. The registry therefore needs no knowledge of Loop,
        Iteration, or downstream container state models. Runtime counters and
        saved graph states are restored before the graph is attached.

        Args:
            frame_id: Identifier of the child frame being restored.
            parent_frame_id: Frame whose scoped config contains the child.
            container_id: Container node that directly owns the child graph.
            root_node_id: Entry node within the restored child graph.
            runtime_data: Generic persisted data for the child runtime.
            variable_pool: Resolved variable pool owned by the child.

        Returns:
            The registered and restored child execution frame.

        """
        parent_state = self[parent_frame_id].state
        state = self._create_child_state(
            parent_state=parent_state,
            variable_pool=variable_pool,
            runtime_data=runtime_data,
        )
        return self._create_child_with_state(
            frame_id=frame_id,
            parent_frame_id=parent_frame_id,
            container_id=container_id,
            root_node_id=root_node_id,
            state=state,
        )

    def _create_child_with_state(
        self,
        *,
        frame_id: str,
        parent_frame_id: str,
        container_id: str,
        root_node_id: str,
        state: RuntimeState,
    ) -> ExecutionFrame:
        """Build a scoped child graph around an already prepared state.

        The parent's retained graph config and node factory are the only graph
        construction inputs. Rebinding the factory prevents child nodes from
        observing the parent's mutable runtime state.

        Args:
            frame_id: Unique ID for the new child frame.
            parent_frame_id: Frame whose graph owns the container config.
            container_id: Container node that scopes the child graph.
            root_node_id: Entry node within the child graph.
            state: Prepared runtime state to bind to child nodes.

        Returns:
            The registered child execution frame.

        Raises:
            RuntimeError: If the parent lacks graph config or a node factory.

        """
        parent_graph = self[parent_frame_id].graph
        graph_config = parent_graph.graph_config
        if graph_config is None:
            msg = "Parent graph does not carry graph_config for frame creation."
            raise RuntimeError(msg)
        node_factory = parent_graph.node_factory
        if node_factory is None:
            msg = "Parent graph does not carry node_factory for frame creation."
            raise RuntimeError(msg)

        rebound_factory = node_factory.with_runtime_state(state)
        graph = Graph.init(
            graph_config=graph_config,
            node_factory=rebound_factory,
            root_node_id=root_node_id,
            container_id=container_id,
        )
        state.variable_pool.node_ids |= self[
            parent_frame_id
        ].state.variable_pool.node_ids
        return self.create(
            frame_id=frame_id,
            container_id=container_id,
            graph=graph,
            state=state,
        )

    @staticmethod
    def _create_child_state(
        *,
        parent_state: RuntimeState,
        variable_pool: VariablePool,
        runtime_data: FrameRuntimeData | None = None,
    ) -> RuntimeState:
        """Create a child runtime state with parent-owned shared services.

        Fresh children start with empty counters and outputs. Restored children
        receive their saved usage, outputs, steps, and pending graph states.
        Both paths reuse the parent's ready queues and graph-wide execution so
        scheduling and terminal status remain coordinated across frames.

        Args:
            parent_state: Runtime state of the frame owning the child container.
            variable_pool: Independent variable pool for the child frame.
            runtime_data: Optional persisted child data to restore.

        Returns:
            An unattached runtime state ready for child graph construction.

        """
        state = RuntimeState(
            variable_pool=variable_pool,
            start_at=parent_state.start_at,
            llm_usage=None if runtime_data is None else runtime_data.llm_usage,
            outputs=None if runtime_data is None else dict(runtime_data.outputs),
            node_run_steps=0 if runtime_data is None else runtime_data.node_run_steps,
            ready_queue=parent_state.ready_queue,
            deferred_ready_queue=parent_state.deferred_ready_queue,
            graph_execution=parent_state.graph_execution,
        )
        if runtime_data is not None:
            state.restore_graph_state(
                node_states=runtime_data.graph_node_states,
                edge_states=runtime_data.graph_edge_states,
            )
        return state
