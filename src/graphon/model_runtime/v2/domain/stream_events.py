from dataclasses import dataclass

from graphon.model_runtime.v2.domain.errors import ModelError
from graphon.model_runtime.v2.domain.exchange import ModelResult
from graphon.model_runtime.v2.domain.json_values import JsonValue, copy_json, copy_usage


def require_sequence(sequence: int) -> None:
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
        message = "Stream sequence must be a nonnegative integer"
        raise ValueError(message)


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamChunk:
    sequence: int
    value: JsonValue
    usage: JsonValue = 0

    def __post_init__(self) -> None:
        require_sequence(self.sequence)
        object.__setattr__(self, "value", copy_json(self.value))
        object.__setattr__(self, "usage", copy_usage(self.usage))


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamUsage:
    sequence: int
    usage: JsonValue = 0

    def __post_init__(self) -> None:
        require_sequence(self.sequence)
        object.__setattr__(self, "usage", copy_usage(self.usage))


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamCompleted:
    result: ModelResult


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamFailed:
    error: ModelError
    usage: JsonValue = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "usage", copy_usage(self.usage))


type StreamEvent = StreamChunk | StreamUsage | StreamCompleted | StreamFailed
