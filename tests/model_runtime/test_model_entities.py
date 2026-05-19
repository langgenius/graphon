import pytest
from pydantic import ValidationError

from graphon.model_runtime.entities.common_entities import I18nObject
from graphon.model_runtime.entities.llm_entities import (
    LLMPollingResponse,
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


def test_llm_polling_response_validates_status_payload() -> None:
    result = LLMResult(
        model="polling-model",
        message=AssistantPromptMessage(content="done"),
        usage=LLMUsage.empty_usage(),
    )

    assert (
        LLMPollingResponse(
            status=LLMPollingStatus.SUCCEEDED,
            result=result,
        ).result
        is result
    )

    for payload in (
        {"status": "running"},
        {"status": "succeeded"},
        {"status": "failed"},
        {
            "status": "running",
            "plugin_state": {"job_id": "job-1"},
            "next_check_after_seconds": 0,
        },
    ):
        with pytest.raises(ValidationError):
            LLMPollingResponse.model_validate(payload)
