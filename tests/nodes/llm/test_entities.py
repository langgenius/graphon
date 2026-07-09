import pytest

from graphon.entities.base_node_data import BaseNodeData, RetryConfig
from graphon.nodes.llm.entities import FirstTokenTimeoutConfig, LLMNodeData
from graphon.nodes.parameter_extractor.entities import ParameterExtractorNodeData
from graphon.nodes.question_classifier.entities import QuestionClassifierNodeData


def test_first_token_timeout_defaults_to_zero() -> None:
    config = FirstTokenTimeoutConfig()

    assert config.first_token_timeout == 0
    assert config.first_token_timeout_seconds is None


def test_first_token_timeout_seconds_converts_milliseconds() -> None:
    config = FirstTokenTimeoutConfig(first_token_timeout=5000)

    assert config.first_token_timeout_seconds == pytest.approx(5.0)


def test_first_token_timeout_seconds_is_none_when_not_positive() -> None:
    assert (
        FirstTokenTimeoutConfig(first_token_timeout=0).first_token_timeout_seconds
        is None
    )
    assert (
        FirstTokenTimeoutConfig(first_token_timeout=-1).first_token_timeout_seconds
        is None
    )


def test_missing_first_token_timeout_is_backward_compatible() -> None:
    # Workflows serialized before this field existed omit it entirely.
    assert FirstTokenTimeoutConfig.model_validate({}).first_token_timeout == 0


def test_first_token_timeout_round_trips() -> None:
    dumped = FirstTokenTimeoutConfig(first_token_timeout=1500).model_dump(mode="json")

    assert dumped["first_token_timeout"] == 1500
    assert FirstTokenTimeoutConfig.model_validate(dumped).first_token_timeout == 1500


def test_llm_family_nodes_carry_first_token_timeout() -> None:
    for node_data_cls in (
        LLMNodeData,
        QuestionClassifierNodeData,
        ParameterExtractorNodeData,
    ):
        assert "first_token_timeout" in node_data_cls.model_fields


def test_first_token_timeout_does_not_pollute_shared_config() -> None:
    # D8: the deadline must not live on the shared RetryConfig / base node data,
    # otherwise every (non-LLM) node would inherit this LLM-only field.
    assert "first_token_timeout" not in RetryConfig.model_fields
    assert "first_token_timeout" not in BaseNodeData.model_fields
