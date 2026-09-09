from graphon.model_runtime.entities.llm_entities import (
    LLMResultChunk,
    LLMResultChunkDelta,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    ImagePromptMessageContent,
    TextPromptMessageContent,
    UserPromptMessage,
)


def test_user_prompt_message_get_text_content_keeps_only_text_items() -> None:
    message = UserPromptMessage(
        content=[
            TextPromptMessageContent(data="hello"),
            ImagePromptMessageContent(
                format="png",
                mime_type="image/png",
                url="https://example.com/image.png",
            ),
            TextPromptMessageContent(data=" world"),
        ],
    )

    assert message.get_text_content() == "hello world"


def test_prompt_message_normalizes_dict_content_items_for_serialization() -> None:
    message = UserPromptMessage.model_validate({
        "content": [{"type": "text", "data": "hello"}],
    })

    assert isinstance(message.content, list)
    assert isinstance(message.content[0], TextPromptMessageContent)
    assert message.model_dump(mode="json")["content"] == [
        {"type": "text", "data": "hello", "opaque_body": None},
    ]


def test_assistant_prompt_message_opaque_body_defaults_to_none() -> None:
    message = AssistantPromptMessage(content="ok")

    assert message.opaque_body is None


def test_assistant_prompt_message_opaque_body_survives_json_round_trip() -> None:
    opaque_body = {
        "assistant_blocks": [{"type": "thinking", "signature": "sig-1"}],
    }
    message = AssistantPromptMessage(content="ok", opaque_body=opaque_body)

    restored = AssistantPromptMessage.model_validate_json(message.model_dump_json())

    assert restored.opaque_body == opaque_body


def test_assistant_prompt_message_accepts_payload_without_opaque_body() -> None:
    message = AssistantPromptMessage.model_validate(
        {"role": "assistant", "content": "ok", "tool_calls": []},
    )

    assert message.opaque_body is None


def test_prompt_message_content_opaque_body_survives_json_round_trip() -> None:
    content = TextPromptMessageContent(
        data="hello",
        opaque_body={"thought_signature": "sig-1"},
    )
    message = AssistantPromptMessage(content=[content])

    restored = AssistantPromptMessage.model_validate_json(message.model_dump_json())

    assert isinstance(restored.content, list)
    assert isinstance(restored.content[0], TextPromptMessageContent)
    assert restored.content[0].opaque_body == {"thought_signature": "sig-1"}


def test_prompt_message_content_accepts_dict_with_opaque_body() -> None:
    message = UserPromptMessage.model_validate(
        {"content": [{"type": "text", "data": "hello", "opaque_body": {"key": "v"}}]},
    )

    assert isinstance(message.content, list)
    assert isinstance(message.content[0], TextPromptMessageContent)
    assert message.content[0].opaque_body == {"key": "v"}


def test_llm_result_chunk_json_round_trip_preserves_opaque_body() -> None:
    """Simulate the plugin daemon -> core deserialization boundary."""
    opaque_body = {
        "assistant_blocks": [
            {"type": "thinking", "thinking": "...", "signature": "sig-1"},
            {"type": "redacted_thinking", "data": "enc-1"},
        ],
    }
    chunk = LLMResultChunk(
        model="test-model",
        delta=LLMResultChunkDelta(
            index=0,
            message=AssistantPromptMessage(content="ok", opaque_body=opaque_body),
            usage=LLMUsage.empty_usage(),
        ),
    )

    restored = LLMResultChunk.model_validate_json(chunk.model_dump_json())

    assert restored.delta.message.opaque_body == opaque_body
