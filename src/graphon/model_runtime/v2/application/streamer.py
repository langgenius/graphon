from collections.abc import Iterator
from contextlib import AbstractContextManager
from typing import Protocol

from graphon.model_runtime.v2.application.context import CallContext
from graphon.model_runtime.v2.domain.exchange import ModelRequest
from graphon.model_runtime.v2.domain.stream_events import StreamEvent


class ModelStreamer(Protocol):
    def stream(
        self, request: ModelRequest, *, context: CallContext
    ) -> AbstractContextManager[Iterator[StreamEvent]]: ...
