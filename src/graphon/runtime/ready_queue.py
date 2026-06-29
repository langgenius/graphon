"""Runtime ready queue protocol."""

from abc import abstractmethod
from typing import Any, Protocol


class ReadyQueue(Protocol):
    """Structural interface required from ready queue implementations.

    Implementations may be in-memory or persistence-backed, but they must
    provide the same queue semantics and serialization surface.
    """

    @abstractmethod
    def put(self, item: Any) -> None:
        """Add a ready item to the ready queue.

        Args:
            item: The implementation-defined item to add to the queue.
        """
        ...

    @abstractmethod
    def get(self, timeout: float | None = None) -> Any:
        """Retrieve and remove the next ready item from the queue.

        Args:
            timeout: Maximum time to wait for an item. ``None`` blocks until an
                item becomes available.

        Returns:
            The implementation-defined item retrieved from the queue.
        """
        ...

    @abstractmethod
    def task_done(self) -> None:
        """Indicate that a previously retrieved task is complete.

        Used by worker threads to signal task completion for join-style
        synchronization.
        """
        ...

    @abstractmethod
    def empty(self) -> bool:
        """Check whether the queue contains any pending items.

        This method must be safe to call concurrently with other queue operations,
        including put and get.

        NOTE: Because the queue can be modified by other threads between the check
        and the subsequent use, this method is prone to TOCTOU errors.

        Returns:
            ``True`` when the queue has no pending items, otherwise ``False``.
        """
        ...

    @abstractmethod
    def qsize(self) -> int:
        """Return the approximate number of pending items awaiting execution.

        This method must be safe to call concurrently with other queue operations,
        including put and get.

        NOTE: Because the queue can be modified by other threads between the check
        and the subsequent use, this method is prone to TOCTOU errors.

        Returns:
            The approximate number of items currently in the queue.
        """
        ...

    @abstractmethod
    def drain(self) -> list[Any]:
        """Remove and return all pending items in FIFO order."""
        ...

    @abstractmethod
    def dumps(self) -> str:
        """Serialize the queue contents for persistence.

        Returns:
            A serialized representation of the queue state that can be stored
            and later restored.
        """
        ...

    @abstractmethod
    def loads(self, data: str) -> None:
        """Restore the queue contents from a serialized payload.

        Args:
            data: The serialized queue state to restore.
        """
        ...
