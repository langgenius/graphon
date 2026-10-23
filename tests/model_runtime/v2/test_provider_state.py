from copy import deepcopy
from typing import Any

import pytest

from graphon.model_runtime.v2 import (
    ContractRef,
    JsonObject,
    JsonValue,
    ModelRef,
    ProviderState,
)


def test_provider_state_owns_opaque_json_and_retains_its_provenance() -> None:
    model = ModelRef(plugin_id="plugin", provider="provider", model="deployment")
    contract = ContractRef(id="dialogue", revision="1")
    cursor_values: list[JsonValue] = ["sensitive continuation", None, False, 1.25]
    payload: JsonObject = {"cursor": cursor_values}
    expected_payload = deepcopy(payload)
    state = ProviderState(
        model=model,
        contract=contract,
        scope=" connection-v1 ",
        version=" opaque-v2 ",
        value=payload,
    )
    cursor_values.append("source mutation")
    assert state.value == expected_payload
    assert (state.model, state.contract, state.scope, state.version) == (
        model,
        contract,
        " connection-v1 ",
        " opaque-v2 ",
    )
    assert "sensitive continuation" not in repr(state)
    for field_name in ("model", "contract", "scope", "version", "value"):
        with pytest.raises(AttributeError):
            setattr(state, field_name, "changed")


@pytest.mark.parametrize(
    "invalid_payload", [object(), {1: "coerced key"}, float("nan")]
)
def test_provider_state_rejects_non_json_payloads(invalid_payload: Any) -> None:
    with pytest.raises((TypeError, ValueError)):
        ProviderState(
            model=ModelRef(plugin_id="plugin", provider="provider", model="deployment"),
            contract=ContractRef(id="dialogue", revision="1"),
            scope="configured",
            version="1",
            value=invalid_payload,
        )


@pytest.mark.parametrize("invalid_identifier", ["", None, 7])
def test_provider_state_requires_scope_and_version(invalid_identifier: Any) -> None:
    for field_name in ("scope", "version"):
        identifiers = {"scope": "configured", "version": "1"}
        identifiers[field_name] = invalid_identifier
        with pytest.raises((TypeError, ValueError)):
            ProviderState(
                model=ModelRef(
                    plugin_id="plugin", provider="provider", model="deployment"
                ),
                contract=ContractRef(id="dialogue", revision="1"),
                value=None,
                **identifiers,
            )


@pytest.mark.parametrize("value", ["opaque", None, False, [], 0, 0.0])
def test_provider_state_preserves_native_json_roots(value: Any) -> None:
    state = ProviderState(
        model=ModelRef(plugin_id="plugin", provider="provider", model="deployment"),
        contract=ContractRef(id="dialogue", revision="1"),
        scope="configured",
        version="1",
        value=value,
    )
    assert state.value == value
    assert type(state.value) is type(value)
