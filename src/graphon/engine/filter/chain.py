from collections.abc import Iterable

from graphon.engine.filter.protocol import (
    EngineEventFilter,
    EngineEventFilterContext,
)
from graphon.engine_events.base import EngineEvent


def filter_engine_events(
    events: Iterable[EngineEvent],
    *,
    context: EngineEventFilterContext,
    filters: Iterable[EngineEventFilter],
) -> Iterable[EngineEvent]:
    """Apply filters in order; from_engine keeps event numbers across runs."""
    filter_list = list(filters)
    for event_filter in filter_list:
        event_filter.initialize(context)

    last_sequence = 0

    def set_event_sequence(event: EngineEvent) -> EngineEvent:
        nonlocal last_sequence
        last_sequence = (
            context.next_sequence(event.sequence)
            if context.next_sequence is not None
            else max(last_sequence + 1, event.sequence)
        )
        if event.sequence == last_sequence:
            return event
        return event.model_copy(update={"sequence": last_sequence})

    for event in events:
        for output_event in _apply_filters(event, filter_list):
            yield set_event_sequence(output_event)

    for index, event_filter in enumerate(filter_list):
        for event in event_filter.flush():
            for output_event in _apply_filters(event, filter_list[index + 1 :]):
                yield set_event_sequence(output_event)


def _apply_filters(
    event: EngineEvent,
    filters: list[EngineEventFilter],
) -> Iterable[EngineEvent]:
    pending_events = [event]
    for event_filter in filters:
        pending_events = [
            output_event
            for pending_event in pending_events
            for output_event in event_filter.on_event(pending_event)
        ]
        if not pending_events:
            break
    yield from pending_events
