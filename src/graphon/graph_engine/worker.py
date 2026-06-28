"""Worker - Thread implementation for queue-based node execution

Workers pull node IDs from the ready_queue, execute nodes, and push events
to the event_queue for the dispatcher to process.
"""

import logging
import queue
import threading
import time
from collections.abc import Sequence
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, datetime
from typing import final, override

from graphon.enums import NodeExecutionType, WorkflowNodeExecutionStatus
from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_engine.frames import FrameRegistry
from graphon.graph_engine.layers.base import GraphEngineLayer
from graphon.graph_engine.ready_queue import ReadyQueue, ReadyTask
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import (
    NodeRunFailedEvent,
    NodeRunStartedEvent,
    is_node_result_event,
)
from graphon.node_events.base import NodeRunResult
from graphon.nodes.base.node import Node

logger = logging.getLogger(__name__)
WORKER_IDLE_THRESHOLD_SECONDS = 0.2


@final
class Worker(threading.Thread):
    """Worker thread that executes nodes from the ready queue.

    Workers continuously pull node IDs from the ready_queue, execute the
    corresponding nodes, and push the resulting events to the event_queue
    for the dispatcher to process.
    """

    def __init__(
        self,
        ready_queue: ReadyQueue,
        event_queue: queue.Queue[TaskEvent],
        frame_registry: FrameRegistry,
        layers: Sequence[GraphEngineLayer],
        worker_id: int = 0,
        execution_context: AbstractContextManager[object] | None = None,
    ) -> None:
        """Initialize worker thread.

        Args:
            ready_queue: Ready queue containing node IDs ready for execution
            event_queue: Queue for pushing task-scoped execution events
            frame_registry: Registry containing frame-local graphs to execute
            layers: Graph engine layers for node execution hooks
            worker_id: Unique identifier for this worker
            execution_context: Optional execution context for context preservation

        """
        super().__init__(name=f"GraphWorker-{worker_id}", daemon=True)
        self._ready_queue = ready_queue
        self._event_queue = event_queue
        self._frame_registry = frame_registry
        self._worker_id = worker_id
        self._execution_context = execution_context
        self._stop_event = threading.Event()
        self._layers = layers if layers is not None else []
        self._last_task_time = time.time()
        self._current_node_started_at: datetime | None = None

    def stop(self) -> None:
        """Signal the worker to stop processing."""
        self._stop_event.set()

    @property
    def is_idle(self) -> bool:
        """Check if the worker is currently idle."""
        # Worker is idle if it hasn't processed a task recently.
        return (time.time() - self._last_task_time) > WORKER_IDLE_THRESHOLD_SECONDS

    @property
    def idle_duration(self) -> float:
        """Get the duration in seconds since the worker last processed a task."""
        return time.time() - self._last_task_time

    @property
    def worker_id(self) -> int:
        """Get the worker's ID."""
        return self._worker_id

    @override
    def run(self) -> None:
        """Main worker loop.

        Continuously pulls node IDs from ready_queue, executes them,
        and pushes events to event_queue until stopped.
        """
        while not self._stop_event.is_set():
            # Try to get a node ID from the ready queue (with timeout)
            try:
                task = self._ready_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            self._last_task_time = time.time()
            node = self._frame_registry.get_node(task)
            try:
                self._current_node_started_at = None
                self._execute_node(task=task, node=node)
                self._ready_queue.task_done()
            except Exception as e:
                logger.exception(
                    "Worker failed while executing node %s",
                    node.id,
                )
                self._event_queue.put(
                    TaskEvent(
                        frame_id=task.frame_id,
                        event=self._build_fallback_failure_event(
                            node,
                            e,
                            started_at=self._current_node_started_at,
                        ),
                    )
                )
            finally:
                self._current_node_started_at = None

    def _execute_node(self, *, task: ReadyTask, node: Node) -> None:
        """Execute a single node and handle its events.

        Args:
            node: The node instance to execute

        """
        self._bind_node_execution_id(task=task, node=node)
        frame_id = task.frame_id

        context = self._execution_context
        if context is None:
            context = nullcontext()

        error: Exception | None = None
        result_event: GraphNodeEventBase | None = None

        with context:
            self._invoke_node_run_start_hooks(node)
            try:
                execution_type = node.execution_type
                if execution_type == NodeExecutionType.CONTAINER:
                    self._consume_container_start_event(
                        frame_id=frame_id,
                        node=node,
                    )
                else:
                    result_event = self._consume_node_events(
                        frame_id=frame_id,
                        node=node,
                    )
            except Exception as exc:
                error = exc
                raise
            finally:
                self._invoke_node_run_end_hooks(node, error, result_event)

    def _bind_node_execution_id(self, *, task: ReadyTask, node: Node) -> None:
        frame = self._frame_registry.get(task.frame_id)
        graph_execution = frame.graph_runtime_state.graph_execution
        node_execution = graph_execution.get_or_create_node_execution(
            frame_id=task.frame_id,
            node_id=task.node_id,
        )
        node.bind_execution_id(node_execution.execution_id)

    def _consume_container_start_event(self, *, frame_id: str, node: Node) -> None:
        node_events = node.run()
        try:
            event = next(node_events)
        finally:
            node_events.close()

        if not isinstance(event, NodeRunStartedEvent):
            msg = f"Container node {node.id} did not emit a start event first."
            raise TypeError(msg)
        if event.id == node.execution_id:
            self._current_node_started_at = event.start_at
        self._event_queue.put(TaskEvent(frame_id=frame_id, event=event))

    def _consume_node_events(
        self,
        *,
        frame_id: str,
        node: Node,
    ) -> GraphNodeEventBase | None:
        result_event: GraphNodeEventBase | None = None
        for event in node.run():
            if isinstance(event, NodeRunStartedEvent) and event.id == node.execution_id:
                self._current_node_started_at = event.start_at
            self._event_queue.put(TaskEvent(frame_id=frame_id, event=event))
            if is_node_result_event(event):
                result_event = event
        return result_event

    def _invoke_node_run_start_hooks(self, node: Node) -> None:
        """Invoke on_node_run_start hooks for all layers."""
        for layer in self._layers:
            try:
                layer.on_node_run_start(node)
            except Exception:
                logger.exception(
                    "Layer %s failed in on_node_run_start for node %s",
                    type(layer).__name__,
                    node.id,
                )
                continue

    def _invoke_node_run_end_hooks(
        self,
        node: Node,
        error: Exception | None,
        result_event: GraphNodeEventBase | None = None,
    ) -> None:
        """Invoke on_node_run_end hooks for all layers."""
        for layer in self._layers:
            try:
                layer.on_node_run_end(node, error, result_event)
            except Exception:
                logger.exception(
                    "Layer %s failed in on_node_run_end for node %s",
                    type(layer).__name__,
                    node.id,
                )
                continue

    def _build_fallback_failure_event(
        self,
        node: Node,
        error: Exception,
        *,
        started_at: datetime | None = None,
    ) -> NodeRunFailedEvent:
        """Build a failure event when worker execution aborts before node output."""
        failure_time = datetime.now(UTC).replace(tzinfo=None)
        error_message = str(error)
        return NodeRunFailedEvent(
            id=node.execution_id,
            node_id=node.id,
            node_type=node.node_type,
            in_iteration_id=None,
            error=error_message,
            start_at=started_at or failure_time,
            finished_at=failure_time,
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error=error_message,
                error_type=type(error).__name__,
            ),
        )
