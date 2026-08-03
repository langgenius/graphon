from unittest.mock import MagicMock

import pytest

from graphon.model_runtime.entities.message_entities import (
    ImagePromptMessageContent,
    PromptMessageRole,
)
from graphon.nodes.llm import llm_utils
from graphon.nodes.llm.entities import LLMNodeChatModelMessage
from graphon.nodes.llm.exc import NoPromptFoundError
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
