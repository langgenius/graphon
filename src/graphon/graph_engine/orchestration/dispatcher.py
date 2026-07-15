"""Main dispatcher for processing events from workers."""

import logging
import queue
import threading
from typing import final

from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import (
    NodeRunExceptionEvent,
    NodeRunFailedEvent,
    NodeRunModelPollingProgressEvent,
    NodeRunSucceededEvent,
)
from graphon.runtime.graph_runtime_state import GraphExecutionProtocol

from ..command_processing import CommandProcessor
from ..event_management import EventManager
from ..event_management.event_handlers import EventHandler
from ..graph_state_manager import GraphStateManager
from ..worker_management import WorkerPool

logger = logging.getLogger(__name__)


@final
class Dispatcher:
    """Main dispatcher that processes events from the event queue.

    This runs in a separate thread and coordinates event processing
    with timeout and completion detection.
    """

    _COMMAND_TRIGGER_EVENTS = (
        NodeRunSucceededEvent,
        NodeRunFailedEvent,
        NodeRunExceptionEvent,
        NodeRunModelPollingProgressEvent,
    )

    def __init__(
        self,
        event_queue: queue.Queue[TaskEvent],
        event_handler: EventHandler,
        graph_execution: GraphExecutionProtocol,
        state_manager: GraphStateManager,
        command_processor: CommandProcessor,
        worker_pool: WorkerPool,
        event_emitter: EventManager,
    ) -> None:
        """Initialize the dispatcher.

        Args:
            event_queue: Queue of events from workers
            event_handler: Event handler registry for processing events
            graph_execution: Aggregate tracking graph execution state
            state_manager: Root frame execution state manager
            command_processor: Processor for external engine commands
            worker_pool: Pool executing ready node tasks
            event_emitter: Event manager to signal completion

        """
        self._event_queue = event_queue
        self._event_handler = event_handler
        self._graph_execution = graph_execution
        self._state_manager = state_manager
        self._command_processor = command_processor
        self._worker_pool = worker_pool
        self._event_emitter = event_emitter

        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the dispatcher thread."""
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._dispatcher_loop,
            name="GraphDispatcher",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the dispatcher thread."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _dispatcher_loop(self) -> None:
        """Main dispatcher loop."""
        try:
            paused = self._run_until_exit()
            self._drain_after_exit(paused)
        except Exception as error:
            logger.exception("Dispatcher error")
            self._graph_execution.fail(error)
        finally:
            if not self._graph_execution.paused and not self._graph_execution.completed:
                self._graph_execution.complete()
            self._event_emitter.mark_complete()

    def _run_until_exit(self) -> bool:
        self._process_commands()
        while not self._stop_event.is_set():
            if (
                self._graph_execution.aborted
                or self._graph_execution.error is not None
                or self._state_manager.is_execution_complete()
            ):
                return False
            if self._graph_execution.paused:
                self._state_manager.drain_ready_tasks_to_deferred()
                self._worker_pool.drain()
                return True
            self._worker_pool.check_and_scale()
            self._dispatch_next_event()
        return False

    def _dispatch_next_event(self) -> None:
        try:
            task_event = self._event_queue.get(timeout=0.1)
        except queue.Empty:
            self._process_commands()
            return
        self._event_handler.dispatch(task_event)
        self._event_queue.task_done()
        self._process_commands(task_event.event)

    def _drain_after_exit(self, paused: bool) -> None:
        self._process_commands()
        if paused:
            self._drain_events_until_idle()
            self._event_handler.snapshot_frames()
        else:
            self._drain_event_queue()

    def _process_commands(self, event: GraphNodeEventBase | None = None) -> None:
        if event is None or isinstance(event, self._COMMAND_TRIGGER_EVENTS):
            self._command_processor.process_commands()

    def _drain_event_queue(self) -> None:
        while True:
            try:
                task_event = self._event_queue.get(block=False)
            except queue.Empty:
                return
            self._event_handler.dispatch(task_event)
            self._event_queue.task_done()

    def _drain_events_until_idle(self) -> None:
        while not self._stop_event.is_set():
            try:
                task_event = self._event_queue.get(timeout=0.1)
            except queue.Empty:
                if not self._worker_pool.has_current_tasks():
                    break
                continue
            self._event_handler.dispatch(task_event)
            self._event_queue.task_done()
            self._process_commands(task_event.event)
        self._drain_event_queue()
