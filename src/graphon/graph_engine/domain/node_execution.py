"""NodeExecution entity representing a node's execution state."""

from dataclasses import dataclass

from graphon.enums import NodeState


@dataclass
class NodeExecution:
    """Entity representing the execution state of a single node.

    This is a mutable entity that tracks the runtime state of a node
    during graph execution.
    """

    node_id: str
    execution_id: str
    state: NodeState = NodeState.UNKNOWN
    retry_count: int = 0
    error: str | None = None

    def mark_started(self) -> None:
        """Mark the node as started."""
        self.state = NodeState.TAKEN

    def mark_taken(self) -> None:
        """Mark the node as successfully completed."""
        self.state = NodeState.TAKEN
        self.error = None

    def mark_failed(self, error: str) -> None:
        """Mark the node as failed with an error."""
        self.error = error

    def mark_skipped(self) -> None:
        """Mark the node as skipped."""
        self.state = NodeState.SKIPPED

    def increment_retry(self) -> None:
        """Increment the retry count for this node."""
        self.retry_count += 1
