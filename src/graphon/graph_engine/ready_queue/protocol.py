"""Serialized state models for GraphEngine ready queue implementations."""

from collections.abc import Sequence

from pydantic import BaseModel, Field


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
