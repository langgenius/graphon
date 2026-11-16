from graphon.model_runtime.entities.llm_entities import (
    LLMResultChunk,
    LLMResultChunkDelta,
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
        TextPromptMessageContent(data="", opaque_body={"signature": "sig"}),
        TextPromptMessageContent(data="after"),
    ]
    assert restored_message.tool_calls == [tool_call]
    assert restored_message.opaque_body is False
