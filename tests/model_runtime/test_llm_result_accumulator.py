import json

import pytest

from graphon.model_runtime.entities.llm_entities import (
    LLMResultChunk,
    LLMResultChunkDelta,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    TextPromptMessageContent,
)
from graphon.model_runtime.model_providers.base import large_language_model


def test_accumulated_result_preserves_message_through_json() -> None:
    accumulator = large_language_model.LLMResultAccumulator()
    tool_call = AssistantPromptMessage.ToolCall(
        id="call-1",
        type="function",
        function=AssistantPromptMessage.ToolCall.ToolCallFunction(
            name="lookup",
            arguments='{"id":9007199254740993}',
        ),
    )
    block = TextPromptMessageContent(data="", opaque_body={"signature": "sig"})
    messages = [
        AssistantPromptMessage(content="before"),
        AssistantPromptMessage(
            content=[block], tool_calls=[tool_call], opaque_body=False
        ),
        AssistantPromptMessage(content="after"),
    ]
    for index, message in enumerate(messages):
        accumulator.consume(
            LLMResultChunk(
                model="test-model",
                delta=LLMResultChunkDelta(index=index, message=message),
            )
        )

    result = accumulator.to_result(model="test-model", prompt_messages=[])
    restored_message = AssistantPromptMessage.model_validate_json(
        result.message.model_dump_json()
    )

    assert restored_message.content == [
        TextPromptMessageContent(data="before"),
        block,
        TextPromptMessageContent(data="after"),
    ]
    assert restored_message.tool_calls == [tool_call]
    assert restored_message.opaque_body is False


@pytest.mark.parametrize("batch_sizes", [[1, 1], [2]])
@pytest.mark.parametrize("final_tokens", [0, 7])
def test_accumulated_result_preserves_complete_calls_and_final_usage(
    batch_sizes: list[int], final_tokens: int
) -> None:
    calls = [
        AssistantPromptMessage.ToolCall(
            id=f"call-{index}",
            type="function",
            function=AssistantPromptMessage.ToolCall.ToolCallFunction(
                name="lookup", arguments=arguments
            ),
        )
        for index, arguments in enumerate(['{ "id": 9007199254740993 }', '{"id":2}'])
    ]
    original_calls_json = [call.model_dump_json() for call in calls]
    accumulator = large_language_model.LLMResultAccumulator()
    offset = 0
    for size in batch_sizes:
        accumulator.consume(
            LLMResultChunk(
                model="test-model",
                delta=LLMResultChunkDelta(
                    index=offset,
                    message=AssistantPromptMessage(
                        tool_calls=calls[offset : offset + size]
                    ),
                    usage=LLMUsage.empty_usage().model_copy(
                        update={"total_tokens": 10}
                    ),
                ),
            )
        )
        offset += size
    final_usage = LLMUsage.empty_usage().model_copy(
        update={"prompt_tokens": final_tokens, "total_tokens": final_tokens}
    )
    for usage in [final_usage, None]:
        accumulator.consume(
            LLMResultChunk(
                model="test-model",
                delta=LLMResultChunkDelta(
                    index=offset, message=AssistantPromptMessage(), usage=usage
                ),
            )
        )

    result = accumulator.to_result(model="test-model", prompt_messages=[])

    assert [
        call.model_dump_json() for call in result.message.tool_calls
    ] == original_calls_json
    assert [call.model_dump_json() for call in calls] == original_calls_json
    assert result.usage == final_usage


@pytest.mark.parametrize(
    ("initial_ids", "incoming_ids"),
    [
        ([], ["new", "new"]),
        (["known"], ["new", "known"]),
        ([], ["new", ""]),
        (["known"], ["new", ""]),
    ],
)
def test_accumulated_result_rejects_invalid_call_ids_before_appending(
    initial_ids: list[str], incoming_ids: list[str]
) -> None:
    accumulator = large_language_model.LLMResultAccumulator()

    def make_chunk(call_ids: list[str]) -> LLMResultChunk:
        return LLMResultChunk(
            model="test-model",
            delta=LLMResultChunkDelta(
                index=0,
                message=AssistantPromptMessage(
                    tool_calls=[
                        AssistantPromptMessage.ToolCall(
                            id=call_id,
                            type="function",
                            function=AssistantPromptMessage.ToolCall.ToolCallFunction(
                                name="lookup", arguments="{}"
                            ),
                        )
                        for call_id in call_ids
                    ]
                ),
            ),
        )

    accumulator.consume(make_chunk(initial_ids))
    incoming_chunk = make_chunk(incoming_ids)
    with pytest.raises(ValueError):  # ruff: ignore[pytest-raises-too-broad] - Only the exception type is contractual.
        accumulator.consume(incoming_chunk)

    result = accumulator.to_result(model="test-model", prompt_messages=[])
    assert [call.id for call in result.message.tool_calls] == initial_ids


@pytest.mark.parametrize("from_json", [False, True])
def test_accumulated_result_rejects_null_call_ids_after_parsing(
    from_json: bool,
) -> None:
    payload = {
        "model": "test-model",
        "delta": {
            "index": 0,
            "message": {
                "tool_calls": [
                    {
                        "id": None,
                        "type": "function",
                        "function": {"name": "lookup", "arguments": "{}"},
                    }
                ]
            },
        },
    }
    chunk = (
        LLMResultChunk.model_validate_json(json.dumps(payload))
        if from_json
        else LLMResultChunk.model_validate(payload)
    )
    accumulator = large_language_model.LLMResultAccumulator()

    with pytest.raises(ValueError):  # ruff: ignore[pytest-raises-too-broad] - Only the exception type is contractual.
        accumulator.consume(chunk)
