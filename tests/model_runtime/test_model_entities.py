import pytest
from pydantic import ValidationError

from graphon.model_runtime.entities.common_entities import I18nObject
from graphon.model_runtime.entities.llm_entities import (
    LLMPollingConfig,
    LLMPollingResult,
    LLMPollingStatus,
    LLMResult,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import AssistantPromptMessage
from graphon.model_runtime.entities.model_entities import (
    AIModelEntity,
    FetchFrom,
    ModelFeature,
    ModelType,
)


def test_model_feature_accepts_polling() -> None:
    model = AIModelEntity.model_validate({
        "model": "polling-model",
        "label": I18nObject(en_US="Polling Model"),
        "model_type": ModelType.LLM,
        "features": ["polling"],
        "fetch_from": FetchFrom.PREDEFINED_MODEL,
        "model_properties": {},
    })

    assert ModelFeature.POLLING in (model.features or [])
    assert model.support_polling is True


def test_llm_polling_result_validates_status_payload() -> None:
    result = LLMResult(
        model="polling-model",
        message=AssistantPromptMessage(content="done"),
        usage=LLMUsage.empty_usage(),
    )

    assert (
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=result,
        ).result
        is result
    )
    assert (
        LLMPollingResult(status=LLMPollingStatus.FAILED, error="  failed  ").error
        == "failed"
    )

    for payload in (
        {"status": "running"},
        {"status": "succeeded"},
        {"status": "failed"},
        {"status": "failed", "error": "   "},
        {
            "status": "running",
            "plugin_state": {"job_id": "job-1"},
            "next_check_after_seconds": 0,
        },
    ):
        with pytest.raises(ValidationError):
            LLMPollingResult.model_validate(payload)


def test_llm_polling_result_accepts_fractional_intervals() -> None:
    result = LLMPollingResult(
        status=LLMPollingStatus.RUNNING,
        plugin_state={"job_id": "job-1"},
        next_check_after_seconds=0.25,
        expires_after_seconds=0.5,
    )

    assert result.next_check_after_seconds == pytest.approx(0.25)
    assert result.expires_after_seconds == pytest.approx(0.5)


def test_llm_polling_config_rejects_zero_min_interval() -> None:
    with pytest.raises(ValidationError):
        LLMPollingConfig(min_check_interval_seconds=0)
