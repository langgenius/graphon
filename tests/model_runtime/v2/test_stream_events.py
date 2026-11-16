from copy import deepcopy
from typing import Any

import pytest

from graphon.model_runtime.v2 import (
    ContractRef,
    JsonObject,
    JsonValue,
    ModelError,
    ModelRef,
    ModelResult,
    StreamChunk,
    StreamCompleted,
    StreamEvent,
    StreamFailed,
    StreamUsage,
)


def test_stream_fixtures_distinguish_terminal_output_failure_and_interruption() -> None:
    chunk = StreamChunk(
        sequence=0, value={"item_id": "t1", "append": "Hel"}, usage={"delta": 3}
    )
    usage_event = StreamUsage(sequence=1, usage={"provider_only": 4})
    result = ModelResult(
        request_id="request",
        model=ModelRef(plugin_id="plugin", provider="provider", model="deployment"),
        contract=ContractRef(id="text", revision="1"),
        output="Hello",
    )
    completed = StreamCompleted(result=result)
    failed = StreamFailed(error=ModelError(code="timeout", message="Stream timed out"))
    # Plugin fixtures supply complete output; V2 does not assemble the preview.
    success: tuple[StreamEvent, ...] = (chunk, usage_event, completed)
    failure: tuple[StreamEvent, ...] = (chunk, failed)
    interrupted: tuple[StreamEvent, ...] = (chunk,)
    assert (chunk.sequence, usage_event.sequence) == (0, 1)
    assert success[-1] == completed
    assert completed.result.output == "Hello"
    assert completed.result.usage == 0
    assert failed.usage == 0
    assert failure[-1] == failed
    assert not any(isinstance(event, StreamCompleted) for event in interrupted)
    for record, field_name in (
        (chunk, "sequence"),
        (usage_event, "usage"),
        (completed, "result"),
        (failed, "error"),
    ):
        with pytest.raises(AttributeError):
            setattr(record, field_name, None)


def test_stream_records_own_json_payloads() -> None:
    items: list[JsonValue] = [None, False, 3]
    payload: JsonObject = {"items": items}
    expected = deepcopy(payload)
    chunk = StreamChunk(sequence=0, value=payload, usage=payload)
    usage_event = StreamUsage(sequence=1, usage=payload)
    failed = StreamFailed(
        error=ModelError(code="provider_error", message="Failed"), usage=payload
    )
    items.append("source mutation")
    for record, field_name in (
        (chunk, "value"),
        (chunk, "usage"),
        (usage_event, "usage"),
        (failed, "usage"),
    ):
        field_value = getattr(record, field_name)
        assert field_value == expected


@pytest.mark.parametrize(
    "usage", [None, {}, [], False, "", 0, 0.0, {"units": [None, False]}]
)
def test_each_stream_usage_envelope_preserves_values_and_types(usage: Any) -> None:
    events = (
        StreamChunk(sequence=0, value="preview", usage=usage),
        StreamUsage(sequence=1, usage=usage),
        StreamFailed(
            error=ModelError(code="provider_error", message="Failed"), usage=usage
        ),
    )
    expected = 0 if usage is None else usage
    for event in events:
        assert event.usage == expected
        assert type(event.usage) is type(expected)
    event_without_usage = StreamUsage(sequence=0)
    assert event_without_usage.usage == 0
    assert type(event_without_usage.usage) is int


@pytest.mark.parametrize("sequence", [-1, True, 0.5, "0"])
def test_nonterminal_stream_events_require_nonnegative_integer_sequences(
    sequence: Any,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        StreamChunk(sequence=sequence, value=None)
    with pytest.raises((TypeError, ValueError)):
        StreamUsage(sequence=sequence)
