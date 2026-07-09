import pytest

from graphon.entities.base_node_data import BaseNodeData, RetryConfig
from graphon.nodes.llm.entities import LLMNodeData, first_token_timeout_seconds
from graphon.nodes.parameter_extractor.entities import ParameterExtractorNodeData
from graphon.nodes.question_classifier.entities import QuestionClassifierNodeData


def test_first_token_timeout_seconds_converts_milliseconds() -> None:
    assert first_token_timeout_seconds(5000) == pytest.approx(5.0)


def test_first_token_timeout_seconds_is_none_when_not_positive() -> None:
    assert first_token_timeout_seconds(0) is None
    assert first_token_timeout_seconds(-1) is None


def test_llm_family_nodes_carry_first_token_timeout() -> None:
    for node_data_cls in (
        LLMNodeData,
        QuestionClassifierNodeData,
        ParameterExtractorNodeData,
    ):
        field = node_data_cls.model_fields.get("first_token_timeout")
        assert field is not None
        # Opt-in: defaults to disabled, so pre-existing workflows are unaffected.
        assert field.default == 0


def test_first_token_timeout_does_not_pollute_shared_config() -> None:
    # The deadline must not live on the shared RetryConfig / base node data,
    # otherwise every (non-LLM) node would inherit this LLM-only field.
    assert "first_token_timeout" not in RetryConfig.model_fields
    assert "first_token_timeout" not in BaseNodeData.model_fields
