"""In-memory implementation of the ready queue protocol.

This implementation wraps Python's standard queue.Queue and adds
serialization capabilities for state storage.
"""

import queue
from typing import final

from graphon.runtime.ready_queue import ReadyQueue

from .protocol import ReadyQueueState, ReadyTask


@final
class InMemoryReadyQueue(ReadyQueue):
    """In-memory ready queue implementation with serialization support.

    This implementation uses Python's queue.Queue internally and provides
    methods to serialize and restore the queue state.
    """

    def __init__(self, maxsize: int = 0) -> None:
        """Initialize the in-memory ready queue.

        Args:
            maxsize: Maximum size of the queue (0 for unlimited)

        """
        self._queue: queue.Queue[ReadyTask] = queue.Queue(maxsize=maxsize)

    def put(self, item: ReadyTask) -> None:
        """Add a task to the ready queue.

        Args:
            item: The task to add to the queue

        """
        self._queue.put(item)

    def get(self, timeout: float | None = None) -> ReadyTask:
        """Retrieve and remove a task from the queue.

        Args:
            timeout: Maximum time to wait for an item (None for blocking)

        Returns:
            The task retrieved from the queue

        """
        if timeout is None:
            return self._queue.get(block=True)
        return self._queue.get(timeout=timeout)

    def task_done(self) -> None:
        """Indicate that a previously retrieved task is complete.

        Used by worker threads to signal task completion for
        join() synchronization.
        """
        self._queue.task_done()

    def empty(self) -> bool:
        """Check if the queue is empty.

        Returns:
            True if the queue has no items, False otherwise

        """
        return self._queue.empty()

    def qsize(self) -> int:
        """Get the approximate size of the queue.

        Returns:
            The approximate number of items in the queue

        """
        return self._queue.qsize()

    def drain(self) -> list[ReadyTask]:
        """Remove and return all queued tasks in FIFO order."""
        items: list[ReadyTask] = []
        with self._queue.mutex:
            while self._queue.queue:
                items.append(self._queue.queue.popleft())
            self._queue.unfinished_tasks -= len(items)
            if self._queue.unfinished_tasks == 0:
                self._queue.all_tasks_done.notify_all()
        return items

    def dumps(self) -> str:
        """Serialize the queue state to a JSON string for storage.

        Returns:
            A JSON string containing the serialized queue state

        """
        with self._queue.mutex:
            items = list(self._queue.queue)
        state = ReadyQueueState(
            type="InMemoryReadyQueue",
            version="1.0",
            items=items,
        )
        return state.model_dump_json()

    def loads(self, data: str) -> None:
        """Restore the queue state from a JSON string.

        Args:
            data: The JSON string containing the serialized queue state to restore

        Raises:
            ValueError: If the serialized queue type or version is unsupported.

        """
        state = ReadyQueueState.model_validate_json(data)

        if state.type != "InMemoryReadyQueue":
            msg = f"Invalid serialized data type: {state.type}"
            raise ValueError(msg)

        if state.version != "1.0":
            msg = f"Unsupported version: {state.version}"
            raise ValueError(msg)

        # Clear the current queue
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break

        # Restore items
        for item in state.items:
            self._queue.put(item)
