from typing import Any

import pytest

from graphon.model_runtime.v2 import CallContext


def test_call_context_keeps_connection_and_call_identity_separate() -> None:
    first_call = CallContext(
        connection_id=" configured-a ", request_id=" first ", timeout_seconds=0.25
    )
    second_call = CallContext(connection_id=" configured-a ", request_id="second")
    assert first_call.connection_id == second_call.connection_id == " configured-a "
    assert first_call.request_id == " first "
    assert second_call.request_id == "second"
    assert first_call.timeout_seconds == pytest.approx(0.25)
    assert second_call.timeout_seconds is None
    for field_name in ("connection_id", "request_id", "timeout_seconds"):
        with pytest.raises(AttributeError):
            setattr(first_call, field_name, "changed")


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True, "1"])
def test_call_context_requires_a_positive_finite_timeout(timeout: Any) -> None:
    with pytest.raises((TypeError, ValueError)):
        CallContext(
            connection_id="configured", request_id="request", timeout_seconds=timeout
        )


@pytest.mark.parametrize("invalid_identifier", ["", None, 7])
def test_call_context_requires_connection_and_request_identifiers(
    invalid_identifier: Any,
) -> None:
    for field_name in ("connection_id", "request_id"):
        context_fields: dict[str, Any] = {
            "connection_id": "configured",
            "request_id": "request",
        }
        context_fields[field_name] = invalid_identifier
        with pytest.raises((TypeError, ValueError)):
            CallContext(**context_fields)
