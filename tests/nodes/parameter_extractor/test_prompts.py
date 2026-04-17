import time
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest

from graphon.model_runtime.entities.llm_entities import LLMMode, LLMUsage
from graphon.model_runtime.entities.message_entities import PromptMessageRole
from graphon.nodes.llm import llm_utils
from graphon.nodes.llm.entities import ModelConfig
from graphon.nodes.parameter_extractor import parameter_extractor_node
from graphon.nodes.parameter_extractor.entities import (
    ParameterConfig,
    ParameterExtractorNodeData,
)
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
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.types import SegmentType

from ...helpers import build_graph_init_params, build_variable_pool


def _call_parameter_extractor_constructor(**kwargs: object) -> Any:
    constructor = cast(Any, ParameterExtractorNode)
    return constructor(**kwargs)


def _build_dependencies(**kwargs: object) -> object:
    constructor = vars(parameter_extractor_node)["_ParameterExtractorNodeDependencies"]
    return constructor(**kwargs)


def _build_parameter_extractor_node() -> tuple[ParameterExtractorNode, VariablePool]:
    variable_pool = build_variable_pool(variables=[(("start", "rule"), "strictly")])
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=time.perf_counter(),
    )
    init_params = build_graph_init_params(graph_config={"nodes": [], "edges": []})
    model_instance = Mock(
        provider="test",
        model_name="test-model",
        parameters={},
        stop=(),
    )
    prompt_message_serializer = Mock()
    node = cast(
        ParameterExtractorNode,
        _call_parameter_extractor_constructor(
            node_id="extractor",
            data=ParameterExtractorNodeData(
                title="Parameter Extractor",
                model=ModelConfig(
                    provider="test",
                    name="test-model",
                    mode=LLMMode.CHAT,
                ),
                query=["start", "query"],
                parameters=[
                    ParameterConfig(
                        name="location",
                        type=SegmentType.STRING,
                        description="The target location",
                        required=True,
                    ),
                ],
                instruction="Follow {{#start.rule#}} instructions.",
                reasoning_mode="function_call",
            ),
            graph_init_params=init_params,
            graph_runtime_state=runtime_state,
            dependencies=_build_dependencies(
                model_instance=model_instance,
                prompt_message_serializer=prompt_message_serializer,
            ),
        ),
    )
    return node, variable_pool


def test_function_calling_system_prompt_formats_without_missing_placeholders() -> None:
    rendered = FUNCTION_CALLING_EXTRACTOR_SYSTEM_PROMPT.format(
        histories="previous messages",
        instruction="Follow the schema.",
    )

    assert FUNCTION_CALLING_EXTRACTOR_NAME in rendered
    assert "{FUNCTION_CALLING_EXTRACTOR_NAME}" not in rendered  # noqa: RUF027
    assert "`extract_parameter`" not in rendered
    assert "previous messages" in rendered
    assert "Follow the schema." in rendered


def test_parameter_extractor_runtime_prompts_format_with_expected_arguments() -> None:
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


def test_function_calling_prompt_template_renders_system_message() -> None:
    node, variable_pool = _build_parameter_extractor_node()

    prompt_messages = node.get_function_calling_prompt_template(
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


def test_prepare_run_context_exposes_model_identity_in_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, variable_pool = _build_parameter_extractor_node()
    variable_pool.add(("start", "query"), "weather in sf")

    monkeypatch.setattr(
        llm_utils,
        "resolve_completion_params_variables",
        lambda parameters, _: parameters,
    )
    monkeypatch.setattr(
        node,
        "_fetch_llm_model_schema",
        lambda **_: SimpleNamespace(features=[]),
    )
    monkeypatch.setattr(node, "_build_run_prompt", lambda **_: ([], []))

    run_context = node._prepare_run_context()  # noqa: SLF001

    assert run_context.inputs["query"] == "weather in sf"
    assert run_context.inputs["model_provider"] == "test"
    assert run_context.inputs["model_name"] == "test-model"


def test_parameter_extractor_run_emits_model_identity_in_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, variable_pool = _build_parameter_extractor_node()
    variable_pool.add(("start", "query"), "weather in sf")

    monkeypatch.setattr(
        llm_utils,
        "resolve_completion_params_variables",
        lambda parameters, _: parameters,
    )
    monkeypatch.setattr(
        node,
        "_fetch_llm_model_schema",
        lambda **_: SimpleNamespace(features=[]),
    )
    monkeypatch.setattr(node, "_build_run_prompt", lambda **_: ([], []))

    invoke_result = SimpleNamespace(
        usage=LLMUsage.empty_usage(),
        message=SimpleNamespace(
            get_text_content=lambda: "{}",
            tool_calls=[],
        ),
    )
    monkeypatch.setattr(node.model_instance, "invoke_llm", lambda **_: invoke_result)

    result = node._run()  # noqa: SLF001

    assert result.inputs["query"] == "weather in sf"
    assert result.inputs["model_provider"] == "test"
    assert result.inputs["model_name"] == "test-model"


def test_parameter_extractor_accepts_dependency_bundle() -> None:
    variable_pool = build_variable_pool(variables=[])
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=time.perf_counter(),
    )
    init_params = build_graph_init_params(graph_config={"nodes": [], "edges": []})
    model_instance = Mock()
    prompt_message_serializer = Mock()
    memory = Mock()

    node = cast(
        ParameterExtractorNode,
        _call_parameter_extractor_constructor(
            node_id="extractor",
            data=ParameterExtractorNodeData(
                title="Parameter Extractor",
                model=ModelConfig(
                    provider="test",
                    name="test-model",
                    mode=LLMMode.CHAT,
                ),
                query=["start", "query"],
                parameters=[],
                reasoning_mode="function_call",
            ),
            graph_init_params=init_params,
            graph_runtime_state=runtime_state,
            dependencies=_build_dependencies(
                model_instance=model_instance,
                prompt_message_serializer=prompt_message_serializer,
                memory=memory,
            ),
        ),
    )

    assert node.model_instance is model_instance


def test_parameter_extractor_rejects_mixed_dependency_styles() -> None:
    with pytest.raises(TypeError, match="Pass either dependencies="):
        _call_parameter_extractor_constructor(
            node_id="extractor",
            data=ParameterExtractorNodeData(
                title="Parameter Extractor",
                model=ModelConfig(
                    provider="test",
                    name="test-model",
                    mode=LLMMode.CHAT,
                ),
                query=["start", "query"],
                parameters=[],
                reasoning_mode="function_call",
            ),
            graph_init_params=build_graph_init_params(
                graph_config={"nodes": [], "edges": []}
            ),
            graph_runtime_state=GraphRuntimeState(
                variable_pool=build_variable_pool(variables=[]),
                start_at=time.perf_counter(),
            ),
            dependencies=_build_dependencies(
                model_instance=Mock(),
                prompt_message_serializer=Mock(),
            ),
            model_instance=Mock(),
            prompt_message_serializer=Mock(),
        )


def test_parameter_extractor_legacy_dependency_keywords_still_work() -> None:
    variable_pool = build_variable_pool(variables=[])
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=time.perf_counter(),
    )
    init_params = build_graph_init_params(graph_config={"nodes": [], "edges": []})
    model_instance = Mock()
    prompt_message_serializer = Mock()
    memory = Mock()

    node = ParameterExtractorNode(
        node_id="extractor",
        data=ParameterExtractorNodeData(
            title="Parameter Extractor",
            model=ModelConfig(
                provider="test",
                name="test-model",
                mode=LLMMode.CHAT,
            ),
            query=["start", "query"],
            parameters=[],
            reasoning_mode="function_call",
        ),
        graph_init_params=init_params,
        graph_runtime_state=runtime_state,
        model_instance=model_instance,
        prompt_message_serializer=prompt_message_serializer,
        memory=memory,
    )

    assert node.model_instance is model_instance
