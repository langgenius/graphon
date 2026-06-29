"""Main dispatcher for processing events from workers."""

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import final

from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import (
    NodeRunExceptionEvent,
    NodeRunFailedEvent,
    NodeRunModelPollingProgressEvent,
    NodeRunSucceededEvent,
)

from ..event_management import EventManager
from ..event_management.event_handlers import EventHandler
from .execution_coordinator import ExecutionCoordinator

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _DispatcherLoopOutcome:
    paused: bool = False


@dataclass(slots=True)
class _DispatcherLifecycle:
    event_queue: queue.Queue[TaskEvent]
    event_handler: EventHandler
    execution_coordinator: ExecutionCoordinator
    stop_event: threading.Event
    event_emitter: EventManager | None = None

    _COMMAND_TRIGGER_EVENTS = (
        NodeRunSucceededEvent,
        NodeRunFailedEvent,
        NodeRunExceptionEvent,
        NodeRunModelPollingProgressEvent,
    )

    def run(self) -> None:
        try:
            outcome = self._run_until_exit()
            self._drain_after_exit(outcome)
        except Exception as error:
            logger.exception("Dispatcher error")
            self.execution_coordinator.mark_failed(error)
        finally:
            self._mark_complete()

    def _run_until_exit(self) -> _DispatcherLoopOutcome:
        self._process_commands()
        while not self.stop_event.is_set():
            if self._execution_finished:
                return _DispatcherLoopOutcome()
            if self.execution_coordinator.paused:
                return _DispatcherLoopOutcome(paused=True)

            self.execution_coordinator.check_scaling()
            self._dispatch_next_event()

        return _DispatcherLoopOutcome()

    @property
    def _execution_finished(self) -> bool:
        return (
            self.execution_coordinator.aborted
            or self.execution_coordinator.execution_complete
        )

    def _dispatch_next_event(self) -> None:
        try:
            task_event = self.event_queue.get(timeout=0.1)
        except queue.Empty:
            self._process_commands()
            time.sleep(0.1)
            return

        self.event_handler.dispatch(task_event)
        self.event_queue.task_done()
        self._process_commands(task_event.event)

    def _drain_after_exit(self, outcome: _DispatcherLoopOutcome) -> None:
        self._process_commands()
        if outcome.paused:
            self._drain_events_until_idle()
            return
        self._drain_event_queue()

    def _process_commands(self, event: GraphNodeEventBase | None = None) -> None:
        if event is None or isinstance(event, self._COMMAND_TRIGGER_EVENTS):
            self.execution_coordinator.process_commands()

    def _drain_event_queue(self) -> None:
        while True:
            try:
                task_event = self.event_queue.get(block=False)
                self.event_handler.dispatch(task_event)
                self.event_queue.task_done()
            except queue.Empty:
                break

    def _drain_events_until_idle(self) -> None:
        while not self.stop_event.is_set():
            try:
                task_event = self.event_queue.get(timeout=0.1)
                self.event_handler.dispatch(task_event)
                self.event_queue.task_done()
                self._process_commands(task_event.event)
            except queue.Empty:
                if not self.execution_coordinator.has_executing_nodes():
                    break
        self._drain_event_queue()

    def _mark_complete(self) -> None:
        self.execution_coordinator.mark_complete()
        if self.event_emitter:
            self.event_emitter.mark_complete()


@final
class Dispatcher:
    """Main dispatcher that processes events from the event queue.

    This runs in a separate thread and coordinates event processing
    with timeout and completion detection.
    """

    def __init__(
        self,
        event_queue: queue.Queue[TaskEvent],
        event_handler: EventHandler,
        execution_coordinator: ExecutionCoordinator,
        event_emitter: EventManager | None = None,
    ) -> None:
        """Initialize the dispatcher.

        Args:
            event_queue: Queue of events from workers
            event_handler: Event handler registry for processing events
            execution_coordinator: Coordinator for execution flow
            event_emitter: Optional event manager to signal completion

        """
        self._event_queue = event_queue
        self._event_handler = event_handler
        self._execution_coordinator = execution_coordinator
        self._event_emitter = event_emitter

        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._start_time: float | None = None

    def start(self) -> None:
        """Start the dispatcher thread."""
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._start_time = time.time()
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
        lifecycle = _DispatcherLifecycle(
            event_queue=self._event_queue,
            event_handler=self._event_handler,
            execution_coordinator=self._execution_coordinator,
            stop_event=self._stop_event,
            event_emitter=self._event_emitter,
        )
        lifecycle.run()
