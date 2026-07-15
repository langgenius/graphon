"""NodeExecution entity representing a node's execution state."""

from dataclasses import dataclass


@dataclass
class NodeExecution:
    """Entity representing the execution state of a single node.

    This is a mutable entity that tracks the runtime state of a node
    during graph execution.
    """

    execution_id: str
    retry_count: int = 0

    def increment_retry(self) -> None:
        """Increment the retry count for this node."""
        self.retry_count += 1
