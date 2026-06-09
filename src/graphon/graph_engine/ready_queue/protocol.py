"""ReadyQueue protocol for GraphEngine node execution queue.

This protocol defines the interface for managing the queue of nodes ready
for execution, supporting both in-memory and persistent storage scenarios.
"""

from collections.abc import Sequence

from pydantic import BaseModel, Field

from graphon.runtime.ready_queue import ReadyQueueProtocol


class ReadyQueueState(BaseModel):
    """Pydantic model for serialized ready queue state.

    This defines the structure of the data returned by dumps()
    and expected by loads() for ready queue serialization.
    """

    type: str = Field(
        description="Queue implementation type (e.g., 'InMemoryReadyQueue')",
    )
    version: str = Field(description="Serialization format version")
    items: Sequence[str] = Field(
        default_factory=list,
        description="List of node IDs in the queue",
    )


ReadyQueue = ReadyQueueProtocol
