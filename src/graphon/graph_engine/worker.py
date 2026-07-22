"""Worker - Thread implementation for queue-based node execution

Workers pull node IDs from the ready_queue, execute nodes, and push events
to the event_queue for the dispatcher to process.
"""

import logging
import queue
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, datetime
from typing import final, override
from uuid import uuid4

from graphon.enums import NodeType, WorkflowNodeExecutionStatus
from graphon.graph_engine.container_handlers import ContainerHandler
from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_engine.frames import FrameRegistry
from graphon.graph_engine.layers.base import GraphEngineLayer
from graphon.graph_engine.ready_queue import (
    ROOT_FRAME_ID,
    ReadyQueue,
    ReadyTask,
    StartTask,
)
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import (
    NodeRunFailedEvent,
    NodeRunStartedEvent,
    is_node_result_event,
)
from graphon.node_events.base import NodeRunResult
from graphon.nodes.base.node import Node
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
)
from graphon.runtime.container_state import create_container_run_state

logger = logging.getLogger(__name__)
WORKER_IDLE_THRESHOLD_SECONDS = 0.2

NodeEventStream = Iterator[GraphNodeEventBase | ContainerAwaitRequest]


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
        container_handlers: Mapping[NodeType, ContainerHandler],
        task_claim_lock: threading.Lock,
        task_claiming: threading.Event,
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
        self._execution_context = (
            execution_context if execution_context is not None else nullcontext()
        )
        self._stop_event = threading.Event()
        self._layers = layers
        self._container_handlers = container_handlers
        self._task_claim_lock = task_claim_lock
        self._task_claiming = task_claiming
        self._last_task_time = time.time()
        self._current_node_started_at: datetime | None = None
        self._current_node: Node | None = None
        self._current_frame_id = ROOT_FRAME_ID
        self._has_current_task = threading.Event()

    def stop(self) -> None:
        """Signal the worker to stop processing."""
        self._stop_event.set()

    @property
    def is_idle(self) -> bool:
        """Check if the worker is currently idle."""
        return (
            not self._has_current_task.is_set()
            and (time.time() - self._last_task_time) > WORKER_IDLE_THRESHOLD_SECONDS
        )

    @property
    def idle_duration(self) -> float:
        """Get the duration in seconds since the worker last processed a task."""
        return time.time() - self._last_task_time

    @property
    def has_current_task(self) -> bool:
        """Return True while the worker owns a queue task."""
        return self._has_current_task.is_set()

    @override
    def run(self) -> None:
        """Main worker loop.

        Continuously pulls node IDs from ready_queue, executes them,
        and pushes events to event_queue until stopped.
        """
        while not self._stop_event.is_set():
            with self._task_claim_lock:
                if not self._task_claiming.is_set():
                    return
                try:
                    task = self._ready_queue.get(timeout=0)
                except queue.Empty:
                    task_claimed = False
                else:
                    self._last_task_time = time.time()
                    self._has_current_task.set()
                    task_claimed = True
            if not task_claimed:
                self._stop_event.wait(0.1)
                continue
            try:
                self._execute_task(task)
            except Exception as e:
                if self._current_node is None:
                    raise
                node = self._current_node
                logger.exception(
                    "Worker failed while executing node %s",
                    node.id,
                )
                self._event_queue.put(
                    TaskEvent(
                        frame_id=self._current_frame_id,
                        event=self._build_fallback_failure_event(
                            node,
                            e,
                            started_at=self._current_node_started_at,
                        ),
                    )
                )
            finally:
                self._ready_queue.task_done()
                self._current_node_started_at = None
                self._current_node = None
                self._current_frame_id = ROOT_FRAME_ID
                self._has_current_task.clear()

    def _execute_task(self, task: ReadyTask) -> None:
        if isinstance(task, StartTask):
            self._current_frame_id = task.frame_id
            node = self._frame_registry.get(task.frame_id).graph.nodes[task.node_id]
            self._current_node = node
            self._execute_node(frame_id=task.frame_id, node=node)
            return
        root_runtime_state = self._frame_registry.get(ROOT_FRAME_ID).graph_runtime_state
        run_state = root_runtime_state.get_container_run(task.invocation_id)
        self._current_frame_id = run_state.frame_id
        node = self._frame_registry.get(run_state.frame_id).graph.nodes[
            run_state.node_id
        ]
        self._bind_execution_id(frame_id=run_state.frame_id, node=node)
        self._current_node = node
        self._current_node_started_at = run_state.started_at
        try:
            suspended = self._run_node_events(
                invocation_id=run_state.invocation_id,
                node=node,
                node_events=node.resume_container(
                    result=task.result,
                    started_at=run_state.started_at,
                ),
            )
        except Exception:
            root_runtime_state.pop_container_run(run_state.invocation_id)
            raise
        if suspended:
            return
        root_runtime_state.pop_container_run(run_state.invocation_id)

    def _execute_node(self, *, frame_id: str, node: Node) -> None:
        """Execute a single node and handle its events.

        Args:
            node: The node instance to execute

        """
        self._bind_execution_id(frame_id=frame_id, node=node)

        self._run_node_events(
            invocation_id=None,
            node=node,
            node_events=node.run(),
        )

    def _bind_execution_id(self, *, frame_id: str, node: Node) -> None:
        frame = self._frame_registry.get(frame_id)
        node_execution = (
            frame.graph_runtime_state.graph_execution.get_or_create_node_execution(
                frame_id=frame_id,
                node_id=node.id,
            )
        )
        node.bind_execution_id(node_execution.execution_id)

    def _run_node_events(
        self,
        *,
        invocation_id: str | None,
        node: Node,
        node_events: NodeEventStream,
    ) -> bool:
        error: Exception | None = None
        result_event: GraphNodeEventBase | None = None
        suspended = False
        with self._execution_context:
            if invocation_id is None:
                self._invoke_node_run_start_hooks(node)
            try:
                result_event, suspended = self._consume_node_events(
                    invocation_id=invocation_id,
                    node=node,
                    node_events=node_events,
                )
            except Exception as exc:
                error = exc
                raise
            else:
                return suspended
            finally:
                if not suspended:
                    self._invoke_node_run_end_hooks(node, error, result_event)

    def _consume_node_events(
        self,
        *,
        invocation_id: str | None,
        node: Node,
        node_events: NodeEventStream,
    ) -> tuple[GraphNodeEventBase | None, bool]:
        result_event: GraphNodeEventBase | None = None
        for event in node_events:
            if isinstance(event, ContainerAwaitRequest):
                started_at = self._current_node_started_at
                if started_at is None:
                    msg = "container await request emitted before node start"
                    raise RuntimeError(msg)
                root_runtime_state = self._frame_registry.get(
                    ROOT_FRAME_ID,
                ).graph_runtime_state
                new_invocation = invocation_id is None
                if new_invocation:
                    invocation_id = str(uuid4())
                    root_runtime_state.put_container_run(
                        create_container_run_state(
                            invocation_id=invocation_id,
                            frame_id=self._current_frame_id,
                            node_id=node.id,
                            started_at=started_at,
                            request=event,
                        )
                    )
                try:
                    self._container_handlers[node.node_type].start_await(
                        invocation_id=invocation_id,
                        request=event,
                    )
                except Exception:
                    if new_invocation:
                        root_runtime_state.pop_container_run(invocation_id)
                    raise
                return None, True
            if isinstance(event, NodeRunStartedEvent) and event.id == node.execution_id:
                self._current_node_started_at = event.start_at
            self._event_queue.put(
                TaskEvent(frame_id=self._current_frame_id, event=event)
            )
            if is_node_result_event(event):
                result_event = event
        return result_event, False

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
