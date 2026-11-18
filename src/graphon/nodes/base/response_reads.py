from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

from graphon.engine_events.node import ResponseRead

_RESPONSE_READS: ContextVar[list[ResponseRead] | None] = ContextVar(
    "response_reads", default=None
)


def current_response_reads() -> list[ResponseRead] | None:
    return _RESPONSE_READS.get()


@contextmanager
def collect_response_reads() -> Generator[list[ResponseRead]]:
    """Collect reads only while this worker evaluates one response attempt."""
    reads: list[ResponseRead] = []
    token = _RESPONSE_READS.set(reads)
    try:
        yield reads
    finally:
        _RESPONSE_READS.reset(token)
