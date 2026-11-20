from __future__ import annotations

import queue
import threading
import time

from graphon.engine.frame import FrameRegistry
from graphon.engine.worker import WorkerPool
from graphon.runtime.ready_queue import InMemoryReadyQueue, ReadyTask


class _CountingReadyQueue(InMemoryReadyQueue):
    def __init__(self) -> None:
        super().__init__()
        self.get_calls = 0
        self._count_lock = threading.Lock()

    def get(self, timeout: float | None = None) -> ReadyTask:
        with self._count_lock:
            self.get_calls += 1
        return super().get(timeout=timeout)


def test_event_driven_worker_pool_does_not_poll_while_idle() -> None:
    """Idle workers block on a Condition instead of polling ready_queue every 10 ms."""
    ready_queue = _CountingReadyQueue()
    dispatch_queue: queue.Queue = queue.Queue()
    pool = WorkerPool(
        ready_queue=ready_queue,
        dispatch_queue=dispatch_queue,
        frame_registry=FrameRegistry(),
        layers=[],
        workers=3,
        event_driven_wakeup=True,
    )
    pool.start()
    try:
        # Let workers settle onto the condition variable, then wait 150 ms.
        # Under 10 ms polling this would accumulate ~15+ calls; event-driven
        # wakeup performs only the initial non-blocking check per worker.
        time.sleep(0.15)
        assert ready_queue.get_calls <= 3
    finally:
        start = time.monotonic()
        pool.stop()
        elapsed = time.monotonic() - start
        assert elapsed < 1.0


def test_event_driven_worker_pool_wakes_on_notify_and_pause() -> None:
    """notify_task_available and pause wake blocked workers immediately."""
    ready_queue = _CountingReadyQueue()
    dispatch_queue: queue.Queue = queue.Queue()
    pool = WorkerPool(
        ready_queue=ready_queue,
        dispatch_queue=dispatch_queue,
        frame_registry=FrameRegistry(),
        layers=[],
        workers=2,
        event_driven_wakeup=True,
    )
    pool.start()
    try:
        time.sleep(0.05)
        calls_before = ready_queue.get_calls
        pool.notify_task_available()
        time.sleep(0.05)
        assert ready_queue.get_calls == calls_before + 1

        start = time.monotonic()
        pending = pool.pause()
        elapsed = time.monotonic() - start
        assert pending == []
        assert elapsed < 1.0
    finally:
        pool.stop()
