"""Simple worker pool that consolidates functionality.

This is a simpler implementation that merges WorkerPool, ActivityTracker,
DynamicScaler, and WorkerFactory into a single class.
"""

import logging
import queue
import threading
from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import final

from graphon.graph_engine.container_handlers import ContainerHandler
from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_engine.frames import FrameRegistry
from graphon.graph_engine.ready_queue import ROOT_FRAME_ID, ReadyQueue, ReadyTask

from ..config import GraphEngineConfig
from ..layers.base import GraphEngineLayer
from ..worker import Worker

logger = logging.getLogger(__name__)
SMALL_GRAPH_NODE_THRESHOLD = 10
MEDIUM_GRAPH_NODE_THRESHOLD = 50


@final
class WorkerPool:
    """Simple worker pool with integrated management.

    This class consolidates all worker management functionality into
    a single, simpler implementation without excessive abstraction.
    """

    def __init__(
        self,
        ready_queue: ReadyQueue,
        event_queue: queue.Queue[TaskEvent],
        frame_registry: FrameRegistry,
        layers: list[GraphEngineLayer],
        config: GraphEngineConfig,
        container_handlers: Mapping[str, ContainerHandler],
        execution_context: AbstractContextManager[object] | None = None,
    ) -> None:
        """Initialize the simple worker pool.

        Args:
            ready_queue: Ready queue protocol for nodes ready for execution
            event_queue: Queue for worker events
            frame_registry: Registry containing frame-local graphs to execute
            layers: Graph engine layers for node execution hooks
            config: GraphEngine worker pool configuration
            container_handlers: Engine-owned container frame handlers by kind
            execution_context: Optional execution context for context preservation

        """
        self._ready_queue = ready_queue
        self._event_queue = event_queue
        self._frame_registry = frame_registry
        self._execution_context = execution_context
        self._layers = layers
        self._config = config

        # Worker management
        self._workers: list[Worker] = []
        self._worker_counter = 0
        self._lock = threading.Lock()
        self._task_claim_lock = threading.Lock()
        self._task_claiming = threading.Event()
        self._running = False

        self._container_handlers = container_handlers

    def start(self) -> None:
        """Start the worker pool."""
        with self._lock:
            if self._running:
                return

            self._running = True
            self._task_claiming.set()
            node_count = len(
                self._frame_registry.get(ROOT_FRAME_ID).graph.nodes,
            )
            if node_count < SMALL_GRAPH_NODE_THRESHOLD:
                initial_count = self._config.min_workers
            elif node_count < MEDIUM_GRAPH_NODE_THRESHOLD:
                initial_count = min(
                    self._config.min_workers + 1,
                    self._config.max_workers,
                )
            else:
                initial_count = min(
                    self._config.min_workers + 2,
                    self._config.max_workers,
                )

            logger.debug(
                "Starting worker pool: %d workers (nodes=%d, min=%d, max=%d)",
                initial_count,
                node_count,
                self._config.min_workers,
                self._config.max_workers,
            )
            for _ in range(initial_count):
                self._create_worker()

    def stop(self) -> None:
        """Stop all workers in the pool."""
        with self._lock:
            self._running = False
            with self._task_claim_lock:
                self._task_claiming.clear()
            worker_count = len(self._workers)

            if worker_count > 0:
                logger.debug("Stopping worker pool: %d workers", worker_count)

            # Stop all workers
            for worker in self._workers:
                worker.stop()

            # Wait for workers to finish
            for worker in self._workers:
                if worker.is_alive():
                    worker.join(timeout=2.0)

            self._workers.clear()

    def drain(self) -> list[ReadyTask]:
        """Atomically stop task claims and remove unclaimed ready work."""
        with self._lock:
            self._running = False
            with self._task_claim_lock:
                self._task_claiming.clear()
                tasks = self._ready_queue.drain()
                for worker in self._workers:
                    if not worker.has_current_task:
                        worker.stop()
            return tasks

    def has_current_tasks(self) -> bool:
        with self._lock:
            return any(worker.has_current_task for worker in self._workers)

    def _create_worker(self) -> None:
        """Create and start a new worker."""
        worker_id = self._worker_counter
        self._worker_counter += 1

        worker = Worker(
            ready_queue=self._ready_queue,
            event_queue=self._event_queue,
            frame_registry=self._frame_registry,
            layers=self._layers,
            worker_id=worker_id,
            execution_context=self._execution_context,
            container_handlers=self._container_handlers,
            task_claim_lock=self._task_claim_lock,
            task_claiming=self._task_claiming,
        )

        worker.start()
        self._workers.append(worker)

    def _remove_worker(self, worker: Worker) -> None:
        """Remove a specific worker from the pool."""
        # Stop the worker
        worker.stop()

        # Wait for it to finish
        if worker.is_alive():
            worker.join(timeout=2.0)

        self._workers.remove(worker)

    def _try_scale_up(
        self,
        queue_depth: int,
        current_count: int,
        active_count: int,
    ) -> bool:
        """Try to scale up workers if needed.

        Args:
            queue_depth: Current queue depth
            current_count: Current number of workers

        Returns:
            True if scaled up, False otherwise

        """
        available_count = current_count - active_count
        backlog = max(queue_depth - available_count, 0)
        if backlog > self._config.scale_up_threshold and (
            current_count < self._config.max_workers
        ):
            self._create_worker()

            logger.debug(
                "Scaled up workers: %d -> %d (backlog=%d exceeded threshold=%d)",
                current_count,
                len(self._workers),
                backlog,
                self._config.scale_up_threshold,
            )
            return True
        return False

    def _try_scale_down(
        self,
        queue_depth: int,
        current_count: int,
        active_count: int,
        idle_count: int,
    ) -> bool:
        """Try to scale down workers if we have excess capacity.

        Args:
            queue_depth: Current queue depth
            current_count: Current number of workers
            active_count: Number of active workers
            idle_count: Number of idle workers

        Returns:
            True if scaled down, False otherwise

        """
        # Skip if we're at minimum or have no idle workers
        if current_count <= self._config.min_workers or idle_count == 0:
            return False

        # Check if we have excess capacity
        has_excess_capacity = (
            queue_depth <= active_count  # Active workers can handle current queue
            or idle_count > active_count  # More idle than active workers
        )

        if not has_excess_capacity:
            return False

        for worker in self._workers:
            if (
                worker.is_idle
                and worker.idle_duration >= self._config.scale_down_idle_time
            ):
                remaining_workers = current_count - 1
                if (
                    remaining_workers >= self._config.min_workers
                    and remaining_workers >= max(1, queue_depth // 2)
                ):
                    self._remove_worker(worker)
                    logger.debug(
                        "Scaled down workers: %d -> %d (removed 1 idle worker after "
                        "%.1fs, queue_depth=%d, active=%d, idle=%d)",
                        current_count,
                        len(self._workers),
                        self._config.scale_down_idle_time,
                        queue_depth,
                        active_count,
                        idle_count - 1,
                    )
                    return True

        return False

    def check_and_scale(self) -> None:
        """Check and perform scaling if needed."""
        with self._lock:
            if not self._running:
                return

            current_count = len(self._workers)
            queue_depth = self._ready_queue.qsize()

            # Active ownership is immediate; idle status includes the scale-down delay.
            active_count = sum(1 for worker in self._workers if worker.has_current_task)
            idle_count = sum(1 for worker in self._workers if worker.is_idle)

            # Try to scale up if queue is backing up
            self._try_scale_up(queue_depth, current_count, active_count)

            # Try to scale down if we have excess capacity
            self._try_scale_down(queue_depth, current_count, active_count, idle_count)
