import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
from textwrap import dedent

import pytest

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


@pytest.mark.parametrize("reset", [False, True])
def test_event_stream_cooperates_with_gevent_patched_after_import(reset: bool) -> None:
    script = dedent(
        f"""
        from graphon.engine.event.stream import EventStream
        from graphon.engine_events.graph import GraphRunStartedEvent

        stream = EventStream([]) if {reset!r} else None

        from gevent import monkey
        monkey.patch_all()
        import gevent

        if stream is None:
            stream = EventStream([])
        else:
            stream.reset()

        event = GraphRunStartedEvent()

        def produce():
            stream.collect(event)
            stream.mark_complete()

        gevent.spawn(produce)
        assert list(stream.emit_events()) == [event]
        """
    )
    # Isolate monkey patching; a native blocking queue must fail, not hang pytest.
    subprocess.run([sys.executable, "-c", script], check=True, timeout=10)  # ruff: ignore[subprocess-without-shell-equals-true]
