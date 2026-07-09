import pytest

from graphon.entities.base_node_data import BaseNodeData, RetryConfig
from graphon.nodes.llm.entities import LLMInvocationConfig, LLMNodeData
from graphon.nodes.parameter_extractor.entities import ParameterExtractorNodeData
from graphon.nodes.question_classifier.entities import QuestionClassifierNodeData


def test_first_token_timeout_defaults_to_disabled() -> None:
    config = LLMInvocationConfig()

    assert config.first_token_timeout == 0
    assert config.first_token_timeout_seconds is None


def test_first_token_timeout_seconds_converts_milliseconds() -> None:
    assert LLMInvocationConfig(
        first_token_timeout=5000,
    ).first_token_timeout_seconds == pytest.approx(5.0)


def test_first_token_timeout_seconds_is_none_when_not_positive() -> None:
    assert (
        LLMInvocationConfig(first_token_timeout=0).first_token_timeout_seconds is None
    )
    assert (
        LLMInvocationConfig(first_token_timeout=-1).first_token_timeout_seconds is None
    )


def test_llm_family_nodes_carry_invocation_config() -> None:
    for node_data_cls in (
        LLMNodeData,
        QuestionClassifierNodeData,
        ParameterExtractorNodeData,
    ):
        field = node_data_cls.model_fields.get("invocation")
        assert field is not None
        assert field.annotation is LLMInvocationConfig


def test_first_token_timeout_does_not_pollute_shared_config() -> None:
    # The deadline must not live on the shared RetryConfig / base node data,
    # otherwise every (non-LLM) node would inherit this LLM-only field.
    assert "first_token_timeout" not in RetryConfig.model_fields
    assert "invocation" not in BaseNodeData.model_fields
