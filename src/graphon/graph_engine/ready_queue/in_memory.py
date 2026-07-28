"""In-memory implementation of the ready queue protocol.

This implementation wraps Python's standard queue.Queue and adds
serialization capabilities for state storage.
"""

import queue
from typing import Annotated, final

from pydantic import Field, TypeAdapter

from .protocol import (
    ROOT_FRAME_ID,
    ReadyQueueState,
    ReadyQueueStateV1,
    ReadyTask,
    StartTask,
)

_READY_QUEUE_STATE_ADAPTER = TypeAdapter(
    Annotated[
        ReadyQueueStateV1 | ReadyQueueState,
        Field(discriminator="version"),
    ],
)


@final
class InMemoryReadyQueue:
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
        return self._queue.get(timeout=timeout)

    def task_done(self) -> None:
        """Indicate that a previously retrieved task is complete.

        Used by worker threads to signal task completion for
        join() synchronization.
        """
        self._queue.task_done()

    def qsize(self) -> int:
        """Get the approximate size of the queue.

        Returns:
            The approximate number of items in the queue

        """
        return self._queue.qsize()

    def drain(self) -> list[ReadyTask]:
        """Remove and return all queued tasks in FIFO order."""
        with self._queue.mutex:
            items: list[ReadyTask] = list(self._queue.queue)
            self._queue.queue.clear()
            self._queue.unfinished_tasks -= len(items)
            if items:
                self._queue.not_full.notify_all()
            if self._queue.unfinished_tasks == 0:
                self._queue.all_tasks_done.notify_all()
        return items

    def dumps(self) -> str:
        """Serialize the queue state to a JSON string for storage.

        Returns:
            A JSON string containing the serialized queue state

        """
        with self._queue.mutex:
            items: list[ReadyTask] = list(self._queue.queue)
        state = ReadyQueueState(
            version="2.0",
            items=tuple(items),
        )
        return state.model_dump_json()

    def loads(self, data: str) -> None:
        """Restore the queue state from a JSON string.

        Args:
            data: The JSON string containing the serialized queue state to restore

        """
        state = _READY_QUEUE_STATE_ADAPTER.validate_json(data)
        if isinstance(state, ReadyQueueStateV1):
            items: tuple[ReadyTask, ...] = tuple(
                StartTask(frame_id=ROOT_FRAME_ID, node_id=node_id)
                for node_id in state.items
            )
        else:
            items = state.items

        self.drain()

        # Restore items
        for item in items:
            self._queue.put(item)
