from typing import cast
from unittest.mock import MagicMock

import pytest

from graphon.model_runtime.entities.llm_entities import LLMMode, LLMResult, LLMUsage
from graphon.model_runtime.entities.message_entities import AssistantPromptMessage
from graphon.nodes.llm.runtime_protocols import LLMProtocol
from graphon.nodes.parameter_extractor.parameter_extractor_node import (
    ParameterExtractorNode,
)


def _build_parameter_extractor_node(
    *,
    first_token_timeout: int,
) -> ParameterExtractorNode:
    node = ParameterExtractorNode.__new__(ParameterExtractorNode)
    node.init_node_data({
        "model": {"provider": "test", "name": "model", "mode": LLMMode.CHAT},
        "query": ["start", "query"],
        "parameters": [
            {
                "name": "location",
                "type": "string",
                "description": "Target location",
                "required": True,
            },
        ],
        "reasoning_mode": "function_call",
        "first_token_timeout": first_token_timeout,
    })
    return node


def _mock_model_instance() -> MagicMock:
    model_instance = MagicMock(parameters={})
    model_instance.invoke_llm.return_value = LLMResult(
        model="model",
        prompt_messages=[],
        message=AssistantPromptMessage(content="ok"),
        usage=LLMUsage.empty_usage(),
    )
    return model_instance


def test_parameter_extractor_invoke_forwards_first_token_timeout() -> None:
    node = _build_parameter_extractor_node(first_token_timeout=5000)
    model_instance = _mock_model_instance()

    node._invoke(cast(LLMProtocol, model_instance), [], [], None)

    assert model_instance.invoke_llm.call_args.kwargs[
        "first_token_timeout"
    ] == pytest.approx(5.0)


def test_parameter_extractor_invoke_defaults_first_token_timeout_to_none() -> None:
    node = _build_parameter_extractor_node(first_token_timeout=0)
    model_instance = _mock_model_instance()

    node._invoke(cast(LLMProtocol, model_instance), [], [], None)

    assert model_instance.invoke_llm.call_args.kwargs["first_token_timeout"] is None
