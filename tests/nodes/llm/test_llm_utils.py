from unittest.mock import MagicMock

import pytest
from pydantic import JsonValue

from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    ImagePromptMessageContent,
    TextPromptMessageContent,
)
from graphon.nodes.llm import llm_utils
from graphon.prompt_entities import MemoryConfig

from ...helpers import build_variable_pool


def _build_model_instance() -> MagicMock:
    model_schema = MagicMock()
    model_schema.supports_prompt_content_type.return_value = True
    return MagicMock(get_model_schema=MagicMock(return_value=model_schema))


@pytest.mark.parametrize("text", ["answer", ""])
@pytest.mark.parametrize("opaque_body", [{"signature": "s"}, {}, [], "", 0, False])
def test_fetch_prompt_messages_preserves_history_content_opaque_body(
    text: str,
    opaque_body: JsonValue,
) -> None:
    block = TextPromptMessageContent(data=text, opaque_body=opaque_body)
    history_message = AssistantPromptMessage(content=[block])
    memory = MagicMock()
    memory.get_history_prompt_messages.return_value = [history_message]
    model_instance = _build_model_instance()
    model_instance.get_model_schema().model_properties = {}

    prompt_messages, _ = llm_utils.fetch_prompt_messages(
        prompt_template=[],
        sys_files=[],
        memory=memory,
        memory_config=MemoryConfig(window=MemoryConfig.WindowConfig(enabled=False)),
        model_instance=model_instance,
        vision_detail=ImagePromptMessageContent.DETAIL.HIGH,
        variable_pool=build_variable_pool(),
        jinja2_variables=[],
    )

    assert len(prompt_messages) == 1
    assert prompt_messages[0].content == [block]


def test_fetch_prompt_messages_preserves_opaque_body_without_content() -> None:
    memory = MagicMock()
    memory.get_history_prompt_messages.return_value = [
        AssistantPromptMessage(content=[], opaque_body={}),
    ]
    model_instance = _build_model_instance()
    model_instance.get_model_schema().model_properties = {}

    prompt_messages, _ = llm_utils.fetch_prompt_messages(
        prompt_template=[],
        sys_files=[],
        memory=memory,
        memory_config=MemoryConfig(window=MemoryConfig.WindowConfig(enabled=False)),
        model_instance=model_instance,
        vision_detail=ImagePromptMessageContent.DETAIL.HIGH,
        variable_pool=build_variable_pool(),
        jinja2_variables=[],
    )

    assert prompt_messages == [AssistantPromptMessage(content=[], opaque_body={})]
