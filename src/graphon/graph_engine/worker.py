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

from graphon.enums import WorkflowNodeExecutionStatus
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
    ContainerRunResult,
    IterationFrameRequest,
    LoopFrameRequest,
)
from graphon.runtime.container_state import ContainerRunState

logger = logging.getLogger(__name__)
WORKER_IDLE_THRESHOLD_SECONDS = 0.2

NodeEventStream = Iterator[GraphNodeEventBase | ContainerAwaitRequest]


class _Suspended:
    pass


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
        container_handlers: Mapping[str, ContainerHandler],
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
        self._container_handlers = container_handlers
        self._last_task_time = time.time()
        self._current_node_started_at: datetime | None = None
        self._current_node: Node | None = None
        self._current_frame_id = ROOT_FRAME_ID

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
            try:
                self._current_node_started_at = None
                self._current_node = None
                self._current_frame_id = ROOT_FRAME_ID
                self._execute_task(task)
                self._ready_queue.task_done()
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
                self._current_node_started_at = None
                self._current_node = None
                self._current_frame_id = ROOT_FRAME_ID

    def _execute_task(self, task: ReadyTask) -> None:
        if isinstance(task, StartTask):
            self._current_frame_id = task.frame_id
            node = self._frame_registry.get_node(task)
            self._current_node = node
            self._execute_node(task=task, node=node)
            return
        root_runtime_state = self._frame_registry.get(ROOT_FRAME_ID).graph_runtime_state
        run_state = root_runtime_state.claim_container_run(task.invocation_id)
        self._current_frame_id = run_state.frame_id
        node = self._frame_registry.get(run_state.frame_id).graph.nodes[
            run_state.node_id
        ]
        node.bind_execution_id(run_state.execution_id)
        self._current_node = node
        self._current_node_started_at = run_state.started_at
        try:
            suspended = self._resume_node(
                invocation_id=task.invocation_id,
                run_state=run_state,
                node=node,
                result=task.result,
            )
        except Exception:
            root_runtime_state.pop_container_run(task.invocation_id)
            raise
        if suspended:
            root_runtime_state.release_container_run_claim(task.invocation_id)
            return
        root_runtime_state.pop_container_run(task.invocation_id)

    def _execute_node(self, *, task: StartTask, node: Node) -> None:
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
        suspended = False

        with context:
            self._invoke_node_run_start_hooks(node)
            try:
                outcome = self._consume_node_events(
                    invocation_id=str(uuid4()),
                    frame_id=frame_id,
                    node_id=task.node_id,
                    node=node,
                    node_events=node.run(),
                    previous_phase_data={},
                )
                if isinstance(outcome, _Suspended):
                    suspended = True
                    return
                result_event = outcome
            except Exception as exc:
                error = exc
                raise
            finally:
                if not suspended:
                    self._invoke_node_run_end_hooks(node, error, result_event)

    def _resume_node(
        self,
        *,
        invocation_id: str,
        run_state: ContainerRunState,
        node: Node,
        result: ContainerRunResult,
    ) -> bool:
        context = self._execution_context
        if context is None:
            context = nullcontext()

        error: Exception | None = None
        result_event: GraphNodeEventBase | None = None
        suspended = False
        with context:
            try:
                outcome = self._consume_node_events(
                    invocation_id=invocation_id,
                    frame_id=run_state.frame_id,
                    node_id=run_state.node_id,
                    node=node,
                    node_events=node.resume_container(
                        phase_data=run_state.phase_data,
                        result=result,
                        started_at=run_state.started_at,
                    ),
                    previous_phase_data=run_state.phase_data,
                )
                if isinstance(outcome, _Suspended):
                    suspended = True
                    return True
                result_event = outcome
            except Exception as exc:
                error = exc
                raise
            finally:
                if not suspended:
                    self._invoke_node_run_end_hooks(
                        node,
                        error,
                        result_event,
                    )
        return False

    def _bind_node_execution_id(self, *, task: StartTask, node: Node) -> None:
        frame = self._frame_registry.get(task.frame_id)
        graph_execution = frame.graph_runtime_state.graph_execution
        node_execution = graph_execution.get_or_create_node_execution(
            frame_id=task.frame_id,
            node_id=task.node_id,
        )
        node.bind_execution_id(node_execution.execution_id)

    def _consume_node_events(
        self,
        *,
        invocation_id: str,
        frame_id: str,
        node_id: str,
        node: Node,
        node_events: NodeEventStream,
        previous_phase_data: Mapping[str, object],
    ) -> GraphNodeEventBase | _Suspended | None:
        result_event: GraphNodeEventBase | None = None
        next_item = next(node_events)
        while True:
            event = next_item
            if isinstance(event, ContainerAwaitRequest):
                started_at = self._current_node_started_at or datetime.now(
                    UTC,
                ).replace(tzinfo=None)
                root_runtime_state = self._frame_registry.get(
                    ROOT_FRAME_ID,
                ).graph_runtime_state
                phase_data = _container_phase_data(event)
                if previous_phase_data:
                    phase_data = {**dict(previous_phase_data), **phase_data}
                root_runtime_state.put_container_run(
                    ContainerRunState(
                        invocation_id=invocation_id,
                        kind=event.kind,
                        frame_id=frame_id,
                        node_id=node_id,
                        execution_id=node.execution_id,
                        started_at=started_at,
                        phase_data=phase_data,
                    )
                )
                self._container_handlers[event.kind].start_await(
                    frame_id=frame_id,
                    node_id=node_id,
                    invocation_id=invocation_id,
                    request=event,
                )
                return _Suspended()
            if isinstance(event, NodeRunStartedEvent) and event.id == node.execution_id:
                self._current_node_started_at = event.start_at
            self._event_queue.put(TaskEvent(frame_id=frame_id, event=event))
            if is_node_result_event(event):
                result_event = event
            try:
                next_item = next(node_events)
            except StopIteration:
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


def _container_phase_data(request: ContainerAwaitRequest) -> dict[str, object]:
    if isinstance(request, LoopFrameRequest):
        return {
            "inputs": dict(request.inputs),
            "loop_count": request.loop_count,
            "root_node_id": request.root_node_id,
            "loop_variable_selectors": {
                key: list(value)
                for key, value in request.loop_variable_selectors.items()
            },
            "loop_node_ids": tuple(sorted(request.loop_node_ids)),
        }
    if isinstance(request, IterationFrameRequest):
        return {
            "inputs": dict(request.inputs),
            "items": request.items,
            "root_node_id": request.root_node_id,
            "output_selector": list(request.output_selector),
            "error_handle_mode": request.error_handle_mode,
            "flatten_output": request.flatten_output,
            "parallel_nums": request.parallel_nums,
        }
    raise TypeError(type(request).__name__)
