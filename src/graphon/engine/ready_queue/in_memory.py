"""In-memory implementation of the ready queue protocol.

This implementation wraps Python's standard queue.Queue and adds
serialization capabilities for state storage.
"""

import queue
from typing import Annotated, Literal, final

from pydantic import BaseModel, Field, TypeAdapter

from graphon.runtime.execution import ROOT_FRAME_ID

from .entities import (
    ReadyTask,
    StartTask,
)


class _ReadyQueueStateV1(BaseModel):
    """Ready queue state produced before frame-aware tasks."""

    type: Literal["InMemoryReadyQueue"]
    version: Literal["1.0"]
    items: tuple[str, ...]


class _ReadyQueueState(BaseModel):
    """Validated serialized state for the in-memory ready queue."""

    version: Literal["2.0"]
    items: tuple[ReadyTask, ...]


_READY_QUEUE_STATE_ADAPTER = TypeAdapter(
    Annotated[
        _ReadyQueueStateV1 | _ReadyQueueState,
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
        self._queue: queue.Queue[ReadyTask] = (
            queue.Queue() if maxsize <= 0 else queue.Queue(maxsize=maxsize)
        )

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
        # Queue has no public atomic drain API. Limit removals to the size observed
        # at entry so newly unblocked producers are not chased; concurrent consumers
        # can still make this a best-effort drain.
        items: list[ReadyTask] = []
        for _ in range(self._queue.qsize()):
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            items.append(item)
            self._queue.task_done()
        return items

    def dumps(self) -> str:
        """Serialize the queue state to a JSON string for storage.

        Returns:
            A JSON string containing the serialized queue state

        """
        # Queue has no public snapshot API. This drain/restore cycle is not atomic;
        # callers must quiesce producers and consumers while serializing.
        items = self.drain()
        try:
            state = _ReadyQueueState(
                version="2.0",
                items=tuple(items),
            )
        finally:
            for item in items:
                self._queue.put(item)
        return state.model_dump_json()

    def loads(self, data: str) -> None:
        """Restore the queue state from a JSON string.

        Args:
            data: The JSON string containing the serialized queue state to restore

        """
        state = _READY_QUEUE_STATE_ADAPTER.validate_json(data)
        if isinstance(state, _ReadyQueueStateV1):
            items: tuple[ReadyTask, ...] = tuple(
                StartTask(frame_id=ROOT_FRAME_ID, node_id=node_id)
                for node_id in state.items
            )
        else:
            items = state.items

        # Replacing contents through public Queue operations is not atomic. Callers
        # must quiesce producers and consumers; otherwise a partial restore may be
        # observed, and bounded puts may block.
        self.drain()

        # Restore items
        for item in items:
            self._queue.put(item)
