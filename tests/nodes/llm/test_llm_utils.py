from unittest.mock import MagicMock

import pytest
from pydantic import JsonValue

from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    ImagePromptMessageContent,
    PromptMessageRole,
    TextPromptMessageContent,
)
from graphon.nodes.llm import llm_utils
from graphon.nodes.llm.entities import LLMNodeChatModelMessage
from graphon.nodes.llm.exc import NoPromptFoundError
from graphon.prompt_entities import MemoryConfig
from graphon.template_rendering import TemplateRenderError

from ...helpers import build_variable_pool


def _model_instance() -> MagicMock:
    model_schema = MagicMock()
    model_schema.supports_prompt_content_type.return_value = True
    return MagicMock(get_model_schema=MagicMock(return_value=model_schema))


def test_fetch_prompt_messages_renders_basic_variables_and_context() -> None:
    prompt_messages, stop = llm_utils.fetch_prompt_messages(
        prompt_template=[
            LLMNodeChatModelMessage(
                text="Hello {{#start.name#}} from {{#context#}}",
                role=PromptMessageRole.USER,
                edition_type="basic",
            ),
        ],
        sys_files=[],
        context="Graphon",
        model_instance=_model_instance(),
        stop=["done"],
        vision_detail=ImagePromptMessageContent.DETAIL.HIGH,
        variable_pool=build_variable_pool(
            variables=[(("start", "name"), "Ada")],
        ),
        jinja2_variables=[],
    )

    assert prompt_messages[0].content == "Hello Ada from Graphon"
    assert stop == ["done"]


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
    model_instance = _model_instance()
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
    model_instance = _model_instance()
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


def test_fetch_prompt_messages_rejects_empty_jinja_prompt() -> None:
    with pytest.raises(NoPromptFoundError):
        llm_utils.fetch_prompt_messages(
            prompt_template=[
                LLMNodeChatModelMessage(
                    text="",
                    jinja2_text="",
                    role=PromptMessageRole.USER,
                    edition_type="jinja2",
                ),
            ],
            sys_files=[],
            model_instance=_model_instance(),
            vision_detail=ImagePromptMessageContent.DETAIL.HIGH,
            variable_pool=build_variable_pool(),
            jinja2_variables=[],
        )


def test_fetch_prompt_messages_requires_jinja_renderer() -> None:
    with pytest.raises(TemplateRenderError):
        llm_utils.fetch_prompt_messages(
            prompt_template=[
                LLMNodeChatModelMessage(
                    text="",
                    jinja2_text="{{ value }}",
                    role=PromptMessageRole.USER,
                    edition_type="jinja2",
                ),
            ],
            sys_files=[],
            model_instance=_model_instance(),
            vision_detail=ImagePromptMessageContent.DETAIL.HIGH,
            variable_pool=build_variable_pool(),
            jinja2_variables=[],
        )
