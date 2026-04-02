"""Factory for creating ReadyQueue instances from serialized state."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from .in_memory import InMemoryReadyQueue
from .protocol import ReadyQueueState

if TYPE_CHECKING:
    from .protocol import ReadyQueue

_READY_QUEUE_BUILDERS: dict[str, tuple[Callable[[], ReadyQueue], str]] = {
    "InMemoryReadyQueue": (InMemoryReadyQueue, "1.0"),
}


def create_ready_queue_from_state(state: ReadyQueueState) -> ReadyQueue:
    """Create a ReadyQueue instance from a serialized state.

    Args:
        state: The serialized queue state (Pydantic model, dict, or JSON string),
            or None for a new empty queue

    Returns:
        A ReadyQueue instance initialized with the given state

    Raises:
        ValueError: If the queue type is unknown or version is unsupported

    """
    ready_queue_config = _READY_QUEUE_BUILDERS.get(state.type)
    if ready_queue_config is None:
        msg = f"Unknown ready queue type: {state.type}"
        raise ValueError(msg)

    queue_builder, supported_version = ready_queue_config
    if state.version != supported_version:
        msg = f"Unsupported {state.type} version: {state.version}"
        raise ValueError(msg)

    queue = queue_builder()
    # Always pass as JSON string to loads()
    queue.loads(state.model_dump_json())
    return queue
