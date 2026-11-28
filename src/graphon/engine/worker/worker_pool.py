"""Fixed-size worker pool."""

import logging
import queue
import threading
from typing import final

from graphon.engine.frame import FrameRegistry
from graphon.file.protocols import WorkflowFileRuntimeProtocol
from graphon.runtime.ready_queue import ReadyQueue, ReadyTask

from ..layer import Layer
from .worker import DispatchTask, Worker

logger = logging.getLogger(__name__)


@final
class WorkerPool:
    """Manage the fixed number of workers configured for an engine."""

    def __init__(
        self,
        ready_queue: ReadyQueue,
        dispatch_queue: queue.Queue[DispatchTask],
        frame_registry: FrameRegistry,
        layers: list[Layer],
        workers: int,
        file_runtime: WorkflowFileRuntimeProtocol | None = None,
        event_driven_wakeup: bool = False,
    ) -> None:
        """Initialize the fixed-size worker pool.

        Args:
            ready_queue: Ready queue protocol for nodes ready for execution
            dispatch_queue: Queue for worker dispatch tasks.
            frame_registry: Registry containing frame-local graphs to execute
            layers: Engine layers for node execution hooks
            workers: Fixed number of worker threads to create
            file_runtime: File adapter supplied by the engine.

        Raises:
            ValueError: If ``workers`` is not a positive integer.

        """
        if not isinstance(workers, int) or isinstance(workers, bool) or workers < 1:
            msg = "workers must be a positive integer"
            raise ValueError(msg)

        self._ready_queue = ready_queue
        self._dispatch_queue = dispatch_queue
        self._frame_registry = frame_registry
        self._file_runtime = file_runtime
        self._layers = layers
        self._worker_count = workers
        self._event_driven_wakeup = event_driven_wakeup

        # Worker management
        self._workers: list[Worker] = []
        self._lock = threading.Lock()
        self._task_acquisition_lock = threading.Lock()
        self._task_acquisition_condition = threading.Condition(
            self._task_acquisition_lock
        )
        self._task_acquisition_enabled = threading.Event()

    def notify_task_available(self) -> None:
        """Wake one idle worker when a task has been placed in the ready queue."""
        with self._task_acquisition_condition:
            self._task_acquisition_condition.notify()

    def start(self) -> None:
        """Start the worker pool."""
        with self._lock:
            if self._workers:
                return

            self._task_acquisition_enabled.set()
            logger.debug("Starting worker pool: %d workers", self._worker_count)
            for worker_id in range(self._worker_count):
                self._create_worker(worker_id)

    def stop(self) -> None:
        """Stop all workers in the pool."""
        with self._lock:
            self._task_acquisition_enabled.clear()
            worker_count = len(self._workers)

            if worker_count > 0:
                logger.debug("Stopping worker pool: %d workers", worker_count)

            # Stop all workers
            condition = getattr(self, "_task_acquisition_condition", None)
            lock = condition if condition is not None else self._task_acquisition_lock
            with lock:
                for worker in self._workers:
                    worker.stop()
                if condition is not None:
                    condition.notify_all()

            # Wait for workers to finish
            for worker in self._workers:
                if worker.is_alive():
                    worker.join(timeout=2.0)

            self._workers.clear()

    def pause(self) -> list[ReadyTask]:
        """Begin a cooperative pause and return tasks that have not started.

        New task acquisition is disabled atomically with removing pending tasks
        from the ready queue. Workers that already own a task continue running;
        idle workers are stopped so the caller can wait only for active work.

        Returns:
            Ready tasks that were still queued when the pause began.

        """
        with self._lock:
            self._task_acquisition_enabled.clear()
            condition = getattr(self, "_task_acquisition_condition", None)
            lock = condition if condition is not None else self._task_acquisition_lock
            with lock:
                pending_tasks = self._ready_queue.take_all()
                for worker in self._workers:
                    if not worker.has_current_task:
                        worker.stop()
                if condition is not None:
                    condition.notify_all()
            return pending_tasks

    def has_current_tasks(self) -> bool:
        with self._lock:
            return any(worker.has_current_task for worker in self._workers)

    def _create_worker(self, worker_id: int) -> None:
        """Create and start a new worker."""
        worker = Worker(
            ready_queue=self._ready_queue,
            dispatch_queue=self._dispatch_queue,
            frame_registry=self._frame_registry,
            layers=self._layers,
            worker_id=worker_id,
            file_runtime=self._file_runtime,
            task_acquisition_lock=self._task_acquisition_lock,
            task_acquisition_enabled=self._task_acquisition_enabled,
            task_acquisition_condition=(
                self._task_acquisition_condition
                if self._event_driven_wakeup
                else None
            ),
        )

        worker.start()
        self._workers.append(worker)
