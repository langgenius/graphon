from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from .snapshot import FrameSnapshot, ReadyQueueFactory, restore_frame_snapshot

if TYPE_CHECKING:
    from .state import RuntimeState


class Snapshot(FrameSnapshot):
    """Legacy frame-scoped snapshot predating workflow-wide root statistics."""

    version: Literal["3.0"]


def loads(
    data: str,
    *,
    state_type: type[RuntimeState],
    ready_queue_factory: ReadyQueueFactory,
) -> RuntimeState:
    """Validate and restore an exact version 3 RuntimeState snapshot.

    Args:
        data: Serialized JSON whose version must be exactly ``"3.0"``.
        state_type: RuntimeState class selected by the public classmethod caller.
        ready_queue_factory: Factory used to restore both task queues.

    Returns:
        Runtime state using current frame-local graph identities.

    """
    return restore_frame_snapshot(
        Snapshot.model_validate_json(data),
        state_type=state_type,
        ready_queue_factory=ready_queue_factory,
        merge_child_statistics=True,
    )
