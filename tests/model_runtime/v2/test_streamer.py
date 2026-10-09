from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager

import pytest

from graphon.model_runtime.v2 import (
    CallContext,
    ContractRef,
    ModelRef,
    ModelRequest,
    ModelResult,
    ModelStreamer,
    StreamChunk,
    StreamCompleted,
    StreamEvent,
)


class TextStreamer:
    def __init__(self) -> None:
        self.closed = False

    @contextmanager
    def stream(
        self, request: ModelRequest, *, context: CallContext
    ) -> Iterator[Iterator[StreamEvent]]:
        # The plugin implementation owns both assembly and resource cleanup.
        events: tuple[StreamEvent, ...] = (
            StreamChunk(sequence=0, value="Hel"),
            StreamCompleted(
                result=ModelResult(
                    request_id=context.request_id,
                    model=request.model,
                    contract=request.contract,
                    output="Hello",
                )
            ),
        )
        try:
            yield iter(events)
        finally:
            self.closed = True


def open_text_stream(
    streamer: ModelStreamer, request: ModelRequest, context: CallContext
) -> AbstractContextManager[Iterator[StreamEvent]]:
    return streamer.stream(request, context=context)


@pytest.mark.parametrize("exit_mode", ["complete", "early", "exception"])
def test_stream_only_implementation_releases_resources_on_every_exit(
    exit_mode: str,
) -> None:
    text_streamer = TextStreamer()
    streamer: ModelStreamer = text_streamer
    request = ModelRequest(
        model=ModelRef(plugin_id="plugin", provider="provider", model="deployment"),
        contract=ContractRef(id="text", revision="1"),
        input="Hello",
    )
    context = CallContext(
        connection_id="configured", request_id="stream", timeout_seconds=1
    )
    if exit_mode == "exception":
        with (  # ruff: ignore[pytest-raises-with-multiple-statements]
            pytest.raises(RuntimeError, match="consumer stopped"),
            open_text_stream(streamer, request, context) as events,
        ):
            assert isinstance(next(events), StreamChunk)
            message = "consumer stopped"
            raise RuntimeError(message)
    else:
        with open_text_stream(streamer, request, context) as events:
            assert isinstance(next(events), StreamChunk)
            if exit_mode == "complete":
                terminal = next(events)
                assert isinstance(terminal, StreamCompleted)
                assert terminal.result.output == "Hello"
                assert terminal.result.request_id == context.request_id
                assert terminal.result.model == request.model
                assert terminal.result.contract == request.contract
                assert list(events) == []
    assert text_streamer.closed
