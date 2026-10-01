from copy import deepcopy
from typing import Any

import pytest

from graphon.model_runtime.v2 import JsonObject, JsonValue, ModelCallError, ModelError


def test_call_error_carries_safe_failure_and_owned_provider_usage() -> None:
    error = ModelError(
        code="vendor:future",
        message="Provider unavailable",
        provider_code="E42",
        retry_after_seconds=0,
    )
    units: list[JsonValue] = [2, None]
    usage: JsonObject = {"provider_units": units, "unknown": False}
    expected = deepcopy(usage)
    failure = ModelCallError(error=error, usage=usage)
    units.append(4)
    assert failure.usage == expected
    with pytest.raises(ModelCallError) as caught:
        raise failure
    assert caught.value.error == error
    assert error.code == "vendor:future"
    assert error.message == "Provider unavailable"
    assert str(caught.value) == error.message
    assert error.provider_code == "E42"
    assert error.retry_after_seconds == 0
    for field in ("code", "message", "provider_code", "retry_after_seconds"):
        with pytest.raises(AttributeError):
            setattr(error, field, "changed")


@pytest.mark.parametrize(
    "usage", [None, {}, [], False, "", 0, 0.0, {"units": [None, False, 2.5]}]
)
def test_call_error_preserves_usage_values_and_types(usage: Any) -> None:
    error = ModelError(code="timeout", message="Call timed out")
    assert error.provider_code is None
    assert error.retry_after_seconds is None
    failure = ModelCallError(error=error, usage=usage)
    expected = 0 if usage is None else usage
    assert failure.usage == expected
    assert type(failure.usage) is type(expected)
    assert type(ModelCallError(error=error).usage) is int
    assert ModelCallError(error=error).usage == 0


@pytest.mark.parametrize("delay", [-1, float("nan"), float("inf"), True, "1"])
def test_model_error_rejects_invalid_retry_delay(delay: Any) -> None:
    with pytest.raises((TypeError, ValueError)):
        ModelError(code="rate_limited", message="Try later", retry_after_seconds=delay)


def test_model_error_accepts_positive_retry_delay() -> None:
    error = ModelError(
        code="rate_limited", message="Try later", retry_after_seconds=1.25
    )
    assert error.retry_after_seconds == 1.25  # ruff: ignore[float-equality-comparison]
