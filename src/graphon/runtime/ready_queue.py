"""Runtime ready queue protocol."""

from abc import abstractmethod
from typing import Protocol


class ReadyQueueProtocol(Protocol):
    """Structural interface required from ready queue implementations."""

    @abstractmethod
    def put(self, item: str) -> None:
        """Enqueue the identifier of a node that is ready to run."""
        ...

    @abstractmethod
    def get(self, timeout: float | None = None) -> str:
        """Return the next node identifier.

        Block until available or until the timeout expires.
        """
        ...

    @abstractmethod
    def task_done(self) -> None:
        """Signal that the most recently dequeued node has completed processing."""
        ...

    @abstractmethod
    def empty(self) -> bool:
        """Return True when the queue contains no pending nodes."""
        ...

    @abstractmethod
    def qsize(self) -> int:
        """Approximate the number of pending nodes awaiting execution."""
        ...

    @abstractmethod
    def dumps(self) -> str:
        """Serialize the queue contents for persistence."""
        ...

    @abstractmethod
    def loads(self, data: str) -> None:
        """Restore the queue contents from a serialized payload."""
        ...
