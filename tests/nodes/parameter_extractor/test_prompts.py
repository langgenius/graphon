import time
from unittest.mock import Mock

from graphon.enums import BuiltinNodeTypes
from graphon.model_runtime.entities.llm_entities import LLMMode
from graphon.model_runtime.entities.message_entities import PromptMessageRole
from graphon.nodes.parameter_extractor.parameter_extractor_node import (
    ParameterExtractorNode,
)
from graphon.nodes.parameter_extractor.prompts import (
    CHAT_GENERATE_JSON_PROMPT,
    CHAT_GENERATE_JSON_USER_MESSAGE_TEMPLATE,
    COMPLETION_GENERATE_JSON_PROMPT,
    FUNCTION_CALLING_EXTRACTOR_NAME,
    FUNCTION_CALLING_EXTRACTOR_SYSTEM_PROMPT,
    FUNCTION_CALLING_EXTRACTOR_USER_TEMPLATE,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from ...helpers import build_graph_init_params, build_variable_pool


def _build_parameter_extractor_node() -> tuple[ParameterExtractorNode, object]:
    variable_pool = build_variable_pool(variables=[(("start", "rule"), "strictly")])
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=time.perf_counter(),
    )
    init_params = build_graph_init_params(graph_config={"nodes": [], "edges": []})
    node = ParameterExtractorNode(
        node_id="extractor",
        config={
            "id": "extractor",
            "data": {
                "type": BuiltinNodeTypes.PARAMETER_EXTRACTOR,
                "title": "Parameter Extractor",
                "model": {
                    "provider": "test",
                    "name": "test-model",
                    "mode": LLMMode.CHAT,
                },
                "query": ["start", "query"],
                "parameters": [
                    {
                        "name": "location",
                        "type": "string",
                        "description": "The target location",
                        "required": True,
                    },
                ],
                "instruction": "Follow {{#start.rule#}} instructions.",
                "reasoning_mode": "function_call",
            },
        },
        graph_init_params=init_params,
        graph_runtime_state=runtime_state,
        model_instance=Mock(),
        prompt_message_serializer=Mock(),
    )
    return node, variable_pool


def test_function_calling_system_prompt_formats_without_missing_placeholders():
    rendered = FUNCTION_CALLING_EXTRACTOR_SYSTEM_PROMPT.format(
        histories="previous messages",
        instruction="Follow the schema.",
    )

    assert FUNCTION_CALLING_EXTRACTOR_NAME in rendered
    assert "{FUNCTION_CALLING_EXTRACTOR_NAME}" not in rendered  # noqa: RUF027
    assert "`extract_parameter`" not in rendered
    assert "previous messages" in rendered
    assert "Follow the schema." in rendered


def test_parameter_extractor_runtime_prompts_format_with_expected_arguments():
    structure = '{"type":"object"}'
    input_text = "weather in sf"
    instruction = "Return valid JSON."
    histories = "user: hi"

    rendered_prompts = [
        FUNCTION_CALLING_EXTRACTOR_USER_TEMPLATE.format(
            content=input_text,
            structure=structure,
        ),
        CHAT_GENERATE_JSON_USER_MESSAGE_TEMPLATE.format(
            structure=structure,
            text=input_text,
        ),
        CHAT_GENERATE_JSON_PROMPT.format(histories=histories, instruction=instruction),
        COMPLETION_GENERATE_JSON_PROMPT.format(
            histories=histories,
            text=input_text,
            instruction=instruction,
        ),
    ]

    assert FUNCTION_CALLING_EXTRACTOR_NAME in rendered_prompts[0]
    assert structure in rendered_prompts[0]
    assert input_text in rendered_prompts[0]
    assert structure in rendered_prompts[1]
    assert input_text in rendered_prompts[1]
    assert histories in rendered_prompts[2]
    assert instruction in rendered_prompts[2]
    assert histories in rendered_prompts[3]
    assert input_text in rendered_prompts[3]
    assert instruction in rendered_prompts[3]


def test_function_calling_prompt_template_renders_system_message():
    node, variable_pool = _build_parameter_extractor_node()

    prompt_messages = node._get_function_calling_prompt_template(
        node_data=node.node_data,
        query="Extract the location from this request.",
        variable_pool=variable_pool,
        memory=None,
    )

    assert len(prompt_messages) == 2
    assert prompt_messages[0].role == PromptMessageRole.SYSTEM
    assert FUNCTION_CALLING_EXTRACTOR_NAME in prompt_messages[0].text
    assert "{FUNCTION_CALLING_EXTRACTOR_NAME}" not in prompt_messages[0].text  # noqa: RUF027
    assert "`extract_parameter`" not in prompt_messages[0].text
    assert "Follow strictly instructions." in prompt_messages[0].text
    assert prompt_messages[1].role == PromptMessageRole.USER
    assert prompt_messages[1].text == "Extract the location from this request."
