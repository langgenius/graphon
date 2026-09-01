from graphon.engine.event.stream import EventStream
from graphon.engine_events.graph import (
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)


def test_event_stream_reset_starts_a_new_fifo_run() -> None:
    stream = EventStream([])
    stream.collect(GraphRunStartedEvent())
    stream.mark_complete()

    stream.reset()
    started = GraphRunStartedEvent()
    succeeded = GraphRunSucceededEvent()

    stream.collect(started)
    stream.collect(succeeded)
    stream.mark_complete()

    assert list(stream.emit_events()) == [started, succeeded]
