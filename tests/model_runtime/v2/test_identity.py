from typing import Any

import pytest

from graphon.model_runtime.v2 import ContractRef, ModelRef


def test_identity_preserves_plugin_scope_and_contract_revision() -> None:
    model = ModelRef(plugin_id="plugin-a", provider="provider", model="deployment")
    assert model == ModelRef(
        plugin_id="plugin-a", provider="provider", model="deployment"
    )
    assert model != ModelRef(
        plugin_id="plugin-b", provider="provider", model="deployment"
    )
    assert ContractRef(id="score", revision="1") != ContractRef(
        id="score", revision="2"
    )
    contract = ContractRef(id=" score ", revision=" Revision A ")
    model_with_spaces = ModelRef(
        plugin_id="  ", provider=" Provider ", model=" Deployment "
    )
    assert (
        model_with_spaces.plugin_id,
        model_with_spaces.provider,
        model_with_spaces.model,
        contract.id,
        contract.revision,
    ) == ("  ", " Provider ", " Deployment ", " score ", " Revision A ")
    for value, field in ((model, "plugin_id"), (contract, "revision")):
        with pytest.raises(AttributeError):
            setattr(value, field, "changed")


@pytest.mark.parametrize("value", ["", None, 7])
def test_identity_rejects_invalid_components(value: Any) -> None:
    for field in ("plugin_id", "provider", "model"):
        fields = {"plugin_id": "plugin", "provider": "provider", "model": "deployment"}
        fields[field] = value
        with pytest.raises((TypeError, ValueError)):
            ModelRef(**fields)
    for field in ("id", "revision"):
        fields = {"id": "score", "revision": "1"}
        fields[field] = value
        with pytest.raises((TypeError, ValueError)):
            ContractRef(**fields)
