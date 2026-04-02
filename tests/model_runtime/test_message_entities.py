from graphon.model_runtime.entities.message_entities import (
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
    message = UserPromptMessage(
        content=[{"type": "text", "data": "hello"}],
    )

    assert isinstance(message.content, list)
    assert isinstance(message.content[0], TextPromptMessageContent)
    assert message.model_dump(mode="json")["content"] == [
        {"type": "text", "data": "hello"},
    ]
