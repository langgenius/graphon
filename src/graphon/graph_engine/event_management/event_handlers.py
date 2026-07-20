"""Event handler implementations for different event types."""

import logging
from collections.abc import Iterator, Mapping
from functools import singledispatchmethod
from typing import final

from graphon.enums import ErrorStrategy, NodeExecutionType, NodeState
from graphon.graph_events.agent import NodeRunAgentLogEvent
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.iteration import (
    NodeRunIterationFailedEvent,
    NodeRunIterationNextEvent,
    NodeRunIterationStartedEvent,
    NodeRunIterationSucceededEvent,
)
from graphon.graph_events.loop import (
    NodeRunLoopFailedEvent,
    NodeRunLoopNextEvent,
    NodeRunLoopStartedEvent,
    NodeRunLoopSucceededEvent,
)
from graphon.graph_events.node import (
    NodeRunExceptionEvent,
    NodeRunFailedEvent,
    NodeRunModelPollingProgressEvent,
    NodeRunPauseRequestedEvent,
    NodeRunReasoningChunkEvent,
    NodeRunRetrieverResourceEvent,
    NodeRunRetryEvent,
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
    NodeRunVariableUpdatedEvent,
)
from graphon.runtime.graph_runtime_state import GraphExecutionProtocol

from ..container_handlers import ContainerHandler
from ..entities.tasks import TaskEvent
from ..frames import ExecutionFrame, FrameRegistry
from ..ready_queue import ROOT_FRAME_ID, StartTask
from .event_manager import EventManager

logger = logging.getLogger(__name__)


@final
class EventHandler:
    """Registry of event handlers for different event types.

    This centralizes the business logic for handling specific events,
    keeping it separate from the routing and collection infrastructure.
    """

    def __init__(
        self,
        graph_execution: GraphExecutionProtocol,
        event_collector: EventManager,
        frame_registry: FrameRegistry,
        container_handlers: Mapping[str, ContainerHandler],
    ) -> None:
        """Initialize the event handler registry.

        Args:
            graph_execution: Graph execution aggregate
            event_collector: Event manager for collecting events
            frame_registry: Registry of frame-local execution collaborators
            container_handlers: Engine-owned container frame handlers by kind

        """
        self._graph_execution = graph_execution
        self._event_collector = event_collector
        self._frame_registry = frame_registry
        self._container_handlers = container_handlers

    def dispatch(self, task_event: TaskEvent) -> None:
        """Handle any task-scoped node event.

        Args:
            task_event: The frame-scoped event to handle

        """
        self._dispatch_event(frame_id=task_event.frame_id, event=task_event.event)
        frame = self._frame_registry.get(task_event.frame_id)
        handler = self._container_handler_for_frame(frame.frame_id)
        if handler is not None:
            handler.complete_frame(frame)

    def snapshot_frames(self) -> None:
        """Persist live child frames after workers have drained for a pause."""
        root_runtime_state = self._frame_registry.get(
            ROOT_FRAME_ID,
        ).graph_runtime_state
        for frame_state in root_runtime_state.container_frames():
            frame = self._frame_registry.get(frame_state.frame_id)
            variable_pool_scope = (
                "parent"
                if isinstance(frame_state.runtime_data.variable_pool, str)
                else "local"
            )
            root_runtime_state.put_container_frame(
                frame_state.model_copy(
                    update={
                        "runtime_data": frame.graph_runtime_state.snapshot_frame(
                            variable_pool_scope=variable_pool_scope,
                        ),
                    },
                ),
            )

    def _dispatch_event(self, *, frame_id: str, event: GraphNodeEventBase) -> None:
        frame = self._frame_registry.get(frame_id)
        for container_frame, handler in self._container_ancestors(frame_id):
            handler.prepare_frame_event(frame=container_frame, event=event)
        self._dispatch(event, frame=frame)

    @singledispatchmethod
    def _dispatch(self, event: GraphNodeEventBase, *, frame: ExecutionFrame) -> None:
        self._collect(frame=frame, event=event)
        logger.warning("Unhandled event type: %s", type(event).__name__)

    def _collect(self, *, frame: ExecutionFrame, event: GraphNodeEventBase) -> None:
        handler = self._container_handler_for_frame(frame.frame_id)
        if handler is not None and not handler.should_collect(event=event):
            return
        self._event_collector.collect(event)

    @_dispatch.register
    def _(
        self,
        event: (
            NodeRunIterationStartedEvent
            | NodeRunIterationNextEvent
            | NodeRunIterationSucceededEvent
            | NodeRunIterationFailedEvent
            | NodeRunLoopStartedEvent
            | NodeRunLoopNextEvent
            | NodeRunLoopSucceededEvent
            | NodeRunLoopFailedEvent
            | NodeRunAgentLogEvent
            | NodeRunModelPollingProgressEvent
            | NodeRunRetrieverResourceEvent
            | NodeRunReasoningChunkEvent
            | NodeRunStreamChunkEvent
        ),
        *,
        frame: ExecutionFrame,
    ) -> None:
        self._collect(frame=frame, event=event)

    @_dispatch.register
    def _(self, event: NodeRunStartedEvent, *, frame: ExecutionFrame) -> None:
        """Handle node started event.

        Args:
            event: The node started event

        """
        # Track execution in domain model
        node_execution = self._graph_execution.get_or_create_node_execution(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )
        is_initial_attempt = node_execution.retry_count == 0
        frame.graph_runtime_state.increment_node_run_steps()

        # Collect the event only for the first attempt; retries remain silent
        if is_initial_attempt:
            self._collect(frame=frame, event=event)

    @_dispatch.register
    def _(self, event: NodeRunVariableUpdatedEvent, *, frame: ExecutionFrame) -> None:
        """Apply a node-requested variable mutation before downstream observers run.

        The event is collected like other node events so parent/container engines can
        forward the updated payload to outer layers, including persistence listeners.
        """
        frame.graph_runtime_state.variable_pool.add(
            event.variable.selector,
            event.variable,
        )
        self._collect(frame=frame, event=event)

    @_dispatch.register
    def _(self, event: NodeRunSucceededEvent, *, frame: ExecutionFrame) -> None:
        node = frame.graph.nodes[event.node_id]
        self._complete_node(
            frame=frame,
            event=event,
            follow_branch=node.execution_type == NodeExecutionType.BRANCH,
        )

    @_dispatch.register
    def _(self, event: NodeRunPauseRequestedEvent, *, frame: ExecutionFrame) -> None:
        """Handle pause requests emitted by nodes."""
        self._graph_execution.pause(event.reason)
        frame.state_manager.finish_execution(event.node_id)
        frame.graph.nodes[event.node_id].state = NodeState.UNKNOWN
        frame.graph_runtime_state.defer_ready_task(
            StartTask(frame_id=frame.frame_id, node_id=event.node_id)
        )
        frame.state_manager.track_unfinished(event.node_id)
        self._collect(frame=frame, event=event)

    @_dispatch.register
    def _(self, event: NodeRunFailedEvent, *, frame: ExecutionFrame) -> None:
        """Handle node failure using error handler.

        Args:
            event: The node failed event

        """
        # Update domain model
        self._graph_execution.record_node_failure()

        frame.graph_runtime_state.add_llm_usage(event.node_run_result.llm_usage)

        result = frame.error_handler.handle_node_failure(
            frame_id=frame.frame_id,
            event=event,
        )

        if result is not None:
            # Process the resulting event (retry, exception, etc.)
            self._dispatch_event(frame_id=frame.frame_id, event=result)
        else:
            handler = self._container_handler_for_frame(frame.frame_id)
            if handler is not None:
                handler.record_frame_failure(frame=frame, event=event)
            else:
                self._graph_execution.fail(RuntimeError(event.error))
            self._collect(frame=frame, event=event)
            frame.state_manager.finish_execution(event.node_id)

    @_dispatch.register
    def _(self, event: NodeRunExceptionEvent, *, frame: ExecutionFrame) -> None:
        node = frame.graph.nodes[event.node_id]
        if node.error_strategy == ErrorStrategy.DEFAULT_VALUE:
            follow_branch = False
        elif node.error_strategy == ErrorStrategy.FAIL_BRANCH:
            follow_branch = True
        else:
            msg = f"Unsupported error strategy: {node.error_strategy}"
            raise NotImplementedError(msg)

        self._complete_node(
            frame=frame,
            event=event,
            follow_branch=follow_branch,
        )

    @_dispatch.register
    def _(self, event: NodeRunRetryEvent, *, frame: ExecutionFrame) -> None:
        """Handle node retry event.

        Args:
            event: The node retry event

        """
        node_execution = self._graph_execution.get_or_create_node_execution(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )
        node_execution.increment_retry()

        # Finish the previous attempt before re-queuing the node
        frame.state_manager.finish_execution(event.node_id)

        # Emit retry event for observers
        self._collect(frame=frame, event=event)

        # Re-queue node for execution
        frame.state_manager.enqueue_node(event.node_id)

    def _complete_node(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunSucceededEvent | NodeRunExceptionEvent,
        follow_branch: bool,
    ) -> None:
        frame.graph_runtime_state.add_llm_usage(event.node_run_result.llm_usage)
        self._store_node_outputs(
            frame=frame,
            node_id=event.node_id,
            outputs=event.node_run_result.outputs,
        )

        if follow_branch:
            ready_nodes, edge_events = frame.edge_processor.handle_branch_completion(
                event.node_id,
                event.node_run_result.edge_source_handle,
            )
        else:
            ready_nodes, edge_events = frame.edge_processor.process_node_success(
                event.node_id
            )
        for edge_event in edge_events:
            self._event_collector.collect(edge_event)
        for node_id in ready_nodes:
            frame.state_manager.enqueue_node(node_id)

        node = frame.graph.nodes[event.node_id]
        if node.execution_type == NodeExecutionType.RESPONSE:
            frame.graph_runtime_state.merge_response_outputs(
                event.node_run_result.outputs,
            )
        frame.state_manager.finish_execution(event.node_id)
        self._collect(frame=frame, event=event)

    def _store_node_outputs(
        self,
        *,
        frame: ExecutionFrame,
        node_id: str,
        outputs: Mapping[str, object],
    ) -> None:
        """Store node outputs in the variable pool.

        Args:
            node_id: Identifier of the node whose outputs are being stored.
            outputs: Mapping of output names to values produced by the node.

        """
        for variable_name, variable_value in outputs.items():
            frame.graph_runtime_state.variable_pool.add(
                (node_id, variable_name),
                variable_value,
            )

    def _container_handler_for_frame(self, frame_id: str) -> ContainerHandler | None:
        if frame_id == ROOT_FRAME_ID:
            return None
        root_runtime_state = self._frame_registry.get(
            ROOT_FRAME_ID,
        ).graph_runtime_state
        frame_state = root_runtime_state.get_container_frame(frame_id)
        return self._container_handlers[frame_state.kind]

    def _container_ancestors(
        self,
        frame_id: str,
    ) -> Iterator[tuple[ExecutionFrame, ContainerHandler]]:
        root_runtime_state = self._frame_registry.get(
            ROOT_FRAME_ID,
        ).graph_runtime_state
        while frame_id != ROOT_FRAME_ID:
            frame_state = root_runtime_state.get_container_frame(frame_id)
            yield (
                self._frame_registry.get(frame_id),
                self._container_handlers[frame_state.kind],
            )
            run_state = root_runtime_state.get_container_run(
                frame_state.parent_invocation_id,
            )
            frame_id = run_state.frame_id
