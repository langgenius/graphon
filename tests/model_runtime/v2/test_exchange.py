from copy import deepcopy
from typing import Any

import pytest

from graphon.model_runtime.v2 import (
    ContractRef,
    JsonObject,
    JsonValue,
    ModelRef,
    ModelRequest,
    ModelResult,
    ProviderState,
)


def test_native_scalar_exchange_retains_identity_and_continuation() -> None:
    model = ModelRef(plugin_id="plugin", provider="provider", model="deployment")
    contract = ContractRef(id="text-score", revision="1")
    continuation = ProviderState(
        model=model, contract=contract, scope="configured", version="1", value="opaque"
    )
    request = ModelRequest(
        model=model, contract=contract, input="relevance", continuation=continuation
    )
    result = ModelResult(
        request_id="submission",
        model=model,
        contract=contract,
        output=0.82,
        continuation=continuation,
    )
    assert request.input == "relevance"
    assert request.parameters == {}
    assert request.output_schema is None
    assert result.output == pytest.approx(0.82)
    assert result.metadata == {}
    assert result.model == request.model
    assert result.contract == request.contract
    assert result.request_id == "submission"
    assert result.continuation == request.continuation == continuation
    for record, field_name in (
        (request, "input"),
        (result, "output"),
        (result, "request_id"),
    ):
        with pytest.raises(AttributeError):
            setattr(record, field_name, "changed")
    request.parameters["local"] = True
    result.metadata["local"] = True
    assert ModelRequest(model=model, contract=contract, input=None).parameters == {}
    assert (
        ModelResult(
            request_id="other", model=model, contract=contract, output=None
        ).metadata
        == {}
    )


def test_exchange_copies_supplied_json_containers() -> None:
    model = ModelRef(plugin_id="plugin", provider="provider", model="deployment")
    contract = ContractRef(id="native", revision="1")
    items: list[JsonValue] = [None, False, 3, 1.5]
    payload: JsonObject = {"items": items}
    expected = deepcopy(payload)
    request = ModelRequest(
        model=model,
        contract=contract,
        input=payload,
        parameters=payload,
        output_schema=payload,
    )
    result = ModelResult(
        request_id="request",
        model=model,
        contract=contract,
        output=payload,
        usage=payload,
        metadata=payload,
    )
    items.append("source mutation")
    for record, field_name in (
        (request, "input"),
        (request, "parameters"),
        (request, "output_schema"),
        (result, "output"),
        (result, "usage"),
        (result, "metadata"),
    ):
        field_value = getattr(record, field_name)
        assert field_value == expected


@pytest.mark.parametrize(
    "usage", [None, {}, [], False, "", 0, 0.0, {"units": [None, False]}]
)
def test_result_preserves_raw_usage_and_only_defaults_null(usage: Any) -> None:
    model = ModelRef(plugin_id="plugin", provider="provider", model="deployment")
    contract = ContractRef(id="native", revision="1")
    result = ModelResult(
        request_id="request", model=model, contract=contract, output=False, usage=usage
    )
    assert result.output is False
    expected = 0 if usage is None else usage
    assert result.usage == expected
    assert type(result.usage) is type(expected)
    result_without_usage = ModelResult(
        request_id="request", model=model, contract=contract, output=None
    )
    assert result_without_usage.output is None
    assert result_without_usage.usage == 0
    assert type(result_without_usage.usage) is int


def test_exchange_requires_object_parameters_schema_and_metadata() -> None:
    model = ModelRef(plugin_id="plugin", provider="provider", model="deployment")
    contract = ContractRef(id="native", revision="1")
    non_object: Any = []
    for field_name in ("parameters", "output_schema"):
        with pytest.raises((TypeError, ValueError)):
            ModelRequest(
                model=model, contract=contract, input=None, **{field_name: non_object}
            )
    with pytest.raises((TypeError, ValueError)):
        ModelResult(
            request_id="request",
            model=model,
            contract=contract,
            output=None,
            metadata=non_object,
        )
