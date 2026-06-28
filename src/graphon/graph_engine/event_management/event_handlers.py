"""Event handler implementations for different event types."""

import logging
from collections.abc import Mapping
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
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.runtime.graph_runtime_state import GraphExecutionProtocol

from ..container_execution import ContainerExecution
from ..entities.tasks import TaskEvent
from ..frames import ExecutionFrame, FrameRegistry
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
        container_execution: ContainerExecution,
    ) -> None:
        """Initialize the event handler registry.

        Args:
            graph_execution: Graph execution aggregate
            event_collector: Event manager for collecting events
            frame_registry: Registry of frame-local execution collaborators
            container_execution: Engine-owned container frame coordinator

        """
        self._graph_execution = graph_execution
        self._event_collector = event_collector
        self._frame_registry = frame_registry
        self._container_execution = container_execution

    def dispatch(self, task_event: TaskEvent) -> None:
        """Handle any task-scoped node event.

        Args:
            task_event: The frame-scoped event to handle

        """
        event = task_event.event
        self._dispatch_event(frame_id=task_event.frame_id, event=event)
        frame = self._frame_registry.get(task_event.frame_id)
        self._container_execution.complete_frame(frame)

    def _dispatch_event(self, *, frame_id: str, event: GraphNodeEventBase) -> None:
        frame = self._frame_registry.get(frame_id)
        self._container_execution.prepare_frame_event(frame=frame, event=event)
        if isinstance(event, NodeRunVariableUpdatedEvent):
            self._dispatch(event, frame=frame)
            return None

        return self._dispatch(event, frame=frame)

    @singledispatchmethod
    def _dispatch(self, event: GraphNodeEventBase, *, frame: ExecutionFrame) -> None:
        self._collect(frame=frame, event=event)
        logger.warning("Unhandled event type: %s", type(event).__name__)

    def _collect(self, *, frame: ExecutionFrame, event: GraphNodeEventBase) -> None:
        if self._container_execution.should_collect(frame=frame, event=event):
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
        node_execution.mark_started()
        frame.graph_runtime_state.increment_node_run_steps()

        # Collect the event only for the first attempt; retries remain silent
        if is_initial_attempt:
            self._collect(frame=frame, event=event)

    @_dispatch.register
    def _(self, event: NodeRunStreamChunkEvent, *, frame: ExecutionFrame) -> None:
        """Handle stream chunk event with full processing.

        Args:
            event: The stream chunk event

        """
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
        """Handle node success by coordinating subsystems.

        This method coordinates between different subsystems to process
        node completion, handle edges, and trigger downstream execution.

        Args:
            event: The node succeeded event

        """
        # Update domain model
        node_execution = self._graph_execution.get_or_create_node_execution(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )
        node_execution.mark_taken()

        self._accumulate_node_usage(
            frame=frame,
            usage=event.node_run_result.llm_usage,
        )

        # Store outputs in variable pool
        self._store_node_outputs(
            frame=frame,
            node_id=event.node_id,
            outputs=event.node_run_result.outputs,
        )

        # Process edges and get ready nodes
        node = frame.graph.nodes[event.node_id]
        if node.execution_type == NodeExecutionType.BRANCH:
            ready_nodes, edge_events = frame.edge_processor.handle_branch_completion(
                event.node_id,
                event.node_run_result.edge_source_handle,
            )
        else:
            ready_nodes, edge_events = frame.edge_processor.process_node_success(
                event.node_id
            )

        # Collect traversal events from edge processing
        for edge_event in edge_events:
            self._event_collector.collect(edge_event)

        # Enqueue ready nodes
        if self._graph_execution.is_paused:
            for node_id in ready_nodes:
                frame.graph_runtime_state.register_deferred_node(node_id)
        else:
            for node_id in ready_nodes:
                frame.state_manager.enqueue_node(
                    frame_id=frame.frame_id,
                    node_id=node_id,
                )
                frame.state_manager.start_execution(
                    frame_id=frame.frame_id,
                    node_id=node_id,
                )

        # Update execution tracking
        frame.state_manager.finish_execution(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )

        # Handle response node outputs
        if node.execution_type == NodeExecutionType.RESPONSE:
            self._update_response_outputs(
                frame=frame,
                outputs=event.node_run_result.outputs,
            )

        # Collect the event
        self._collect(frame=frame, event=event)

    @_dispatch.register
    def _(self, event: NodeRunPauseRequestedEvent, *, frame: ExecutionFrame) -> None:
        """Handle pause requests emitted by nodes."""
        pause_reason = event.reason
        self._graph_execution.pause(pause_reason)
        frame.state_manager.finish_execution(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )
        if event.node_id in frame.graph.nodes:
            frame.graph.nodes[event.node_id].state = NodeState.UNKNOWN
        frame.graph_runtime_state.register_paused_node(event.node_id)
        self._collect(frame=frame, event=event)

    @_dispatch.register
    def _(self, event: NodeRunFailedEvent, *, frame: ExecutionFrame) -> None:
        """Handle node failure using error handler.

        Args:
            event: The node failed event

        """
        # Update domain model
        node_execution = self._graph_execution.get_or_create_node_execution(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )
        node_execution.mark_failed(event.error)
        self._graph_execution.record_node_failure()

        self._accumulate_node_usage(
            frame=frame,
            usage=event.node_run_result.llm_usage,
        )

        result = frame.error_handler.handle_node_failure(
            frame_id=frame.frame_id,
            event=event,
        )

        if result:
            # Process the resulting event (retry, exception, etc.)
            self._dispatch_event(frame_id=frame.frame_id, event=result)
        else:
            if self._container_execution.record_frame_failure(
                frame=frame,
                event=event,
            ):
                self._collect(frame=frame, event=event)
                frame.state_manager.finish_execution(
                    frame_id=frame.frame_id,
                    node_id=event.node_id,
                )
                return
            # Abort execution
            self._graph_execution.fail(RuntimeError(event.error))
            self._collect(frame=frame, event=event)
            frame.state_manager.finish_execution(
                frame_id=frame.frame_id,
                node_id=event.node_id,
            )

    @_dispatch.register
    def _(self, event: NodeRunExceptionEvent, *, frame: ExecutionFrame) -> None:
        """Handle node exception event (fail-branch strategy).

        Args:
            event: The node exception event

        """
        # Node continues via fail-branch/default-value, treat as completion
        node_execution = self._graph_execution.get_or_create_node_execution(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )
        node_execution.mark_taken()

        self._accumulate_node_usage(
            frame=frame,
            usage=event.node_run_result.llm_usage,
        )

        # Persist outputs produced by the exception strategy (e.g. default values)
        self._store_node_outputs(
            frame=frame,
            node_id=event.node_id,
            outputs=event.node_run_result.outputs,
        )

        node = frame.graph.nodes[event.node_id]

        if node.error_strategy == ErrorStrategy.DEFAULT_VALUE:
            ready_nodes, edge_events = frame.edge_processor.process_node_success(
                event.node_id
            )
        elif node.error_strategy == ErrorStrategy.FAIL_BRANCH:
            ready_nodes, edge_events = frame.edge_processor.handle_branch_completion(
                event.node_id,
                event.node_run_result.edge_source_handle,
            )
        else:
            msg = f"Unsupported error strategy: {node.error_strategy}"
            raise NotImplementedError(msg)

        for edge_event in edge_events:
            self._event_collector.collect(edge_event)

        for node_id in ready_nodes:
            frame.state_manager.enqueue_node(
                frame_id=frame.frame_id,
                node_id=node_id,
            )
            frame.state_manager.start_execution(
                frame_id=frame.frame_id,
                node_id=node_id,
            )

        # Update response outputs if applicable
        if node.execution_type == NodeExecutionType.RESPONSE:
            self._update_response_outputs(
                frame=frame,
                outputs=event.node_run_result.outputs,
            )

        frame.state_manager.finish_execution(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )

        # Collect the exception event for observers
        self._collect(frame=frame, event=event)

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
        frame.state_manager.finish_execution(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )

        # Emit retry event for observers
        self._collect(frame=frame, event=event)

        # Re-queue node for execution
        frame.state_manager.enqueue_node(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )
        frame.state_manager.start_execution(
            frame_id=frame.frame_id,
            node_id=event.node_id,
        )

    def _accumulate_node_usage(
        self,
        *,
        frame: ExecutionFrame,
        usage: LLMUsage,
    ) -> None:
        """Accumulate token usage into the shared runtime state."""
        if usage.total_tokens <= 0:
            return

        frame.graph_runtime_state.add_tokens(usage.total_tokens)

        current_usage = frame.graph_runtime_state.llm_usage
        if current_usage.total_tokens == 0:
            frame.graph_runtime_state.llm_usage = usage
        else:
            frame.graph_runtime_state.llm_usage = current_usage.plus(usage)

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

    def _update_response_outputs(
        self,
        *,
        frame: ExecutionFrame,
        outputs: Mapping[str, object],
    ) -> None:
        """Update response outputs for response nodes."""
        frame.graph_runtime_state.merge_response_outputs(outputs)
