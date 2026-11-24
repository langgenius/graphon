from __future__ import annotations

import pytest
from pydantic import ValidationError

from graphon.model_runtime.entities.llm_entities import LLMUsage


def test_llm_usage_from_metadata_derives_total_tokens() -> None:
    usage = LLMUsage.from_metadata({
        "prompt_tokens": 2,
        "completion_tokens": 3,
        "latency": 0.5,
    })

    assert usage.total_tokens == 5


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        pytest.param(
            {"prompt_tokens": True},
            "prompt_tokens",
            id="reject-bool-token-count",
        ),
        pytest.param(
            {"latency": True},
            "latency",
            id="reject-bool-latency",
        ),
    ],
)
def test_llm_usage_from_metadata_rejects_bool_numeric_values(
    payload: dict[str, object],
    match: str,
) -> None:
    with pytest.raises(ValidationError, match=match):
        LLMUsage.from_metadata(payload)


def test_llm_usage_empty_usage_is_frozen_singleton() -> None:
    usage = LLMUsage.empty_usage()
    assert usage is LLMUsage.empty_usage()

    with pytest.raises(ValidationError):
        usage.prompt_tokens = 1  # type: ignore[misc]
