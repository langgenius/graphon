"""Thread-safe collection and delivery of engine events."""

import logging
import threading
from collections import deque
from collections.abc import Callable, Generator
from datetime import UTC, datetime
from typing import final

from graphon.engine_events.base import EngineEvent

from ..layer import Layer

_logger = logging.getLogger(__name__)


@final
class EventStream:
    """Collect, buffer, and stream engine events."""

    def __init__(
        self,
        layers: list[Layer],
        graph_id: str = "",
        execution_id: str = "",
        next_sequence: Callable[[], int] | None = None,
    ) -> None:
        self._graph_id = graph_id
        self._execution_id = execution_id
        self._events: deque[EngineEvent] = deque()
        self._condition = threading.Condition()
        self._layers = layers
        self._execution_complete = False
        self._next_sequence = next_sequence
        self._local_sequence = 0

    def notify_layers(self, event: EngineEvent) -> None:
        """Stamp an unbuffered lifecycle event and notify registered layers."""
        with self._condition:
            self._stamp(event)
            self._notify_layers(event)

    def collect(self, event: EngineEvent) -> None:
        """Buffer one event and wake its consumer."""
        with self._condition:
            if self._execution_complete:
                msg = "Cannot collect events after execution is complete"
                raise RuntimeError(msg)
            self._stamp(event)
            self._events.append(event)
            # Layers observe stream order before the consumer can wake.
            self._notify_layers(event)
            self._condition.notify()

    def mark_complete(self) -> None:
        """Mark execution complete and wake all waiting consumers."""
        with self._condition:
            self._execution_complete = True
            self._condition.notify_all()

    def reset(self) -> None:
        """Discard buffered events and completion state from the previous run."""
        with self._condition:
            self._events.clear()
            self._execution_complete = False

    def emit_events(self) -> Generator[EngineEvent, None, None]:
        """Yield events in collection order, releasing each after consumption."""
        while True:
            with self._condition:
                self._condition.wait_for(
                    lambda: self._events or self._execution_complete
                )
                if not self._events:
                    return
                event = self._events.popleft()
            yield event  # ruff:ignore[unnecessary-assign-before-yield]

    def _stamp(self, event: EngineEvent) -> None:
        event.graph_id = self._graph_id
        event.execution_id = self._execution_id
        if self._next_sequence is None:
            self._local_sequence += 1
            event.sequence = self._local_sequence
        else:
            event.sequence = self._next_sequence()
        event.emitted_at = datetime.now(UTC)

    def _notify_layers(self, event: EngineEvent) -> None:
        for layer in self._layers:
            try:
                layer.on_event(event)
            except Exception:
                _logger.exception("Error in layer on_event, layer_type=%s", type(layer))
