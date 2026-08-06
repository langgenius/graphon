from concurrent.futures import ThreadPoolExecutor
from datetime import UTC
from threading import Barrier, Event, Thread
from unittest.mock import MagicMock

import pytest

from graphon.engine.event.stream import EventStream
from graphon.engine.layer import Layer
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.graph import GraphRunStartedEvent


def test_emit_events_waits_for_completion_and_drains_pending_events() -> None:
    stream = EventStream([], graph_id="graph", execution_id="execution")
    event = GraphRunStartedEvent()
    emitted: list[EngineEvent] = []
    completed = Event()

    def consume() -> None:
        emitted.extend(stream.emit_events())
        completed.set()

    consumer = Thread(target=consume)
    consumer.start()
    stream.collect(event)
    stream.mark_complete()

    assert completed.wait(timeout=1)
    consumer.join()
    assert emitted == [event]


def test_concurrent_collection_assigns_sequences_in_emission_order() -> None:
    layer = MagicMock(spec=Layer)
    stream = EventStream([layer], graph_id="graph", execution_id="execution")
    events = [GraphRunStartedEvent() for _ in range(8)]
    barrier = Barrier(len(events))

    def collect(event: EngineEvent) -> None:
        barrier.wait()
        stream.collect(event)

    with ThreadPoolExecutor(max_workers=len(events)) as executor:
        list(executor.map(collect, events))

    stream.mark_complete()
    emitted = list(stream.emit_events())

    assert [event.sequence for event in emitted] == list(range(1, len(events) + 1))
    assert len({event.id for event in emitted}) == len(events)
    assert all(event.graph_id == "graph" for event in emitted)
    assert all(event.execution_id == "execution" for event in emitted)
    assert all(event.emitted_at.tzinfo is UTC for event in emitted)
    notified = [call.args[0] for call in layer.on_event.call_args_list]
    assert all(
        layer_event is emitted_event
        for layer_event, emitted_event in zip(notified, emitted, strict=True)
    )


def test_layer_observes_event_before_consumer() -> None:
    consumer_received = Event()
    emitted: list[EngineEvent] = []
    layer = MagicMock(spec=Layer)
    stream = EventStream([layer], graph_id="graph", execution_id="execution")

    def consume() -> None:
        emitted.append(next(stream.emit_events()))
        consumer_received.set()

    consumer = Thread(target=consume, daemon=True)
    observed_during_callback: list[bool] = []

    def on_event(_: EngineEvent) -> None:
        consumer.start()
        observed_during_callback.append(consumer_received.wait(timeout=0.1))

    layer.on_event.side_effect = on_event
    event = GraphRunStartedEvent()

    stream.collect(event)
    consumer.join(timeout=1)

    assert observed_during_callback == [False]
    assert emitted == [event]


def test_consumed_event_is_removed_from_stream_buffer() -> None:
    stream = EventStream([], graph_id="graph", execution_id="execution")
    event = GraphRunStartedEvent()
    stream.collect(event)
    emitted = stream.emit_events()

    assert next(emitted) is event
    assert not stream._events

    stream.mark_complete()
    assert list(emitted) == []


def test_collect_buffers_before_synchronously_notifying_layers() -> None:
    failing_layer = MagicMock(spec=Layer)
    failing_layer.on_event.side_effect = RuntimeError("layer failed")
    recording_layer = MagicMock(spec=Layer)
    stream = EventStream(
        [failing_layer, recording_layer],
        graph_id="graph",
        execution_id="execution",
    )
    buffered_when_notified: list[tuple[EngineEvent, ...]] = []
    recording_layer.on_event.side_effect = lambda _: buffered_when_notified.append(
        tuple(stream._events)
    )
    event = GraphRunStartedEvent()

    stream.collect(event)

    failing_layer.on_event.assert_called_once_with(event)
    recording_layer.on_event.assert_called_once_with(event)
    assert buffered_when_notified == [(event,)]


def test_reset_discards_pending_events_without_resetting_sequence() -> None:
    stream = EventStream([], graph_id="graph", execution_id="execution")
    lifecycle_event = GraphRunStartedEvent()
    stale_event = GraphRunStartedEvent()
    stream.notify_layers(lifecycle_event)
    stream.collect(stale_event)
    stream.mark_complete()

    stream.reset()
    current_event = GraphRunStartedEvent()
    stream.collect(current_event)
    stream.mark_complete()

    assert list(stream.emit_events()) == [current_event]
    assert [lifecycle_event.sequence, stale_event.sequence, current_event.sequence] == [
        1,
        2,
        3,
    ]


def test_completion_rejects_late_collection_but_allows_terminal_notification() -> None:
    stream = EventStream([], graph_id="graph", execution_id="execution")
    stream.mark_complete()
    terminal_event = GraphRunStartedEvent()
    late_event = GraphRunStartedEvent()

    stream.notify_layers(terminal_event)

    with pytest.raises(RuntimeError, match="after execution is complete"):
        stream.collect(late_event)
    assert terminal_event.sequence == 1
    assert late_event.sequence == 0
