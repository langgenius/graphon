"""Thread-safe collection and delivery of engine events."""

import logging
import threading
import time
from collections.abc import Generator
from typing import final

from graphon.engine_events.base import EngineEvent

from ..layer import Layer

_logger = logging.getLogger(__name__)


@final
class EventStream:
    """Collect, buffer, and stream engine events.

    The stream is the single event boundary between the engine and external
    consumers. It also notifies the engine's layers as events arrive.
    """

    def __init__(self, layers: list[Layer]) -> None:
        """Initialize an event stream bound to the engine's live layer list.

        The list is retained by reference so layers registered after engine
        construction are visible to the stream without a second configuration
        phase. Collected events are buffered until :meth:`emit_events` yields
        them, while lifecycle events can notify the same layers without being
        added to that buffer.

        Args:
            layers: Mutable list of layers owned by the engine.

        """
        self._events: list[EngineEvent] = []
        self._lock = threading.Lock()
        self._layers = layers
        self._execution_complete = threading.Event()

    def notify_layers(self, event: EngineEvent) -> None:
        """Notify all layers about an event without buffering it.

        Layer exceptions are caught and logged so one extension cannot disrupt
        event delivery to the remaining layers or the engine itself.

        Args:
            event: Event to send to every registered layer.

        """
        for layer in self._layers:
            try:
                layer.on_event(event)
            except Exception:
                _logger.exception("Error in layer on_event, layer_type=%s", type(layer))

    def collect(self, event: EngineEvent) -> None:
        """Thread-safe method to collect an event.

        Args:
            event: The event to collect

        """
        with self._lock:
            self._events.append(event)

        # NOTE: `notify_layers` is intentionally called outside the critical section
        # to minimize lock contention and avoid blocking other readers or writers.
        self.notify_layers(event)

    def _get_new_events(self, start_index: int) -> list[EngineEvent]:
        """Get new events starting from a specific index.

        Args:
            start_index: The index to start from

        Returns:
            List of new events

        """
        with self._lock:
            return list(self._events[start_index:])

    def _event_count(self) -> int:
        """Get the current count of collected events.

        Returns:
            Number of collected events

        """
        with self._lock:
            return len(self._events)

    def mark_complete(self) -> None:
        """Mark execution as complete to stop the event emission generator."""
        self._execution_complete.set()

    def reset(self) -> None:
        """Discard events and completion state from the previous engine run."""
        with self._lock:
            self._events.clear()
            self._execution_complete.clear()

    def emit_events(self) -> Generator[EngineEvent, None, None]:
        """Generator that yields events as they're collected.

        Yields:
            EngineEvent instances as they're processed

        """
        yielded_count = 0

        while (
            not self._execution_complete.is_set() or yielded_count < self._event_count()
        ):
            # Get new events since last yield
            new_events = self._get_new_events(yielded_count)

            # Yield any new events
            for event in new_events:
                yield event
                yielded_count += 1

            # Small sleep to avoid busy waiting
            if not self._execution_complete.is_set() and not new_events:
                time.sleep(0.001)
