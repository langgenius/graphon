from typing import Any, cast

import pytest

from graphon.model_runtime.v2 import (
    ContractRef,
    DataFormat,
    ModelContract,
    ModelDescriptor,
    ModelRef,
)


def make_contract(
    contract_id: str, input_mime_type: str, output_mime_type: str
) -> ModelContract:
    return ModelContract(
        ref=ContractRef(id=contract_id, revision="1"),
        request=DataFormat(schema={}, mime_types=(input_mime_type,)),
        output=DataFormat(schema={}, mime_types=(output_mime_type,)),
        delivery=frozenset({"complete"}),
        stream=None,
        accepts_output_schema=False,
    )


def test_descriptor_keeps_pairings_independent_of_operation_tags() -> None:
    model_ref = ModelRef(plugin_id="plugin", provider="provider", model="deployment")
    contracts = (
        make_contract("text-score", "text/plain", "application/json"),
        make_contract("audio-text", "audio/L16;rate=16000", "text/plain"),
    )
    descriptor = ModelDescriptor(
        ref=model_ref,
        contracts=contracts,
        operation_tags=("vendor:rank", "transcribe"),
        label="Native scorer",
        description="Scores text and transcribes audio",
    )
    untagged_descriptor = ModelDescriptor(
        ref=model_ref, contracts=contracts, operation_tags=()
    )
    assert descriptor.ref == untagged_descriptor.ref
    assert descriptor.contracts == untagged_descriptor.contracts == contracts
    assert descriptor.operation_tags == ("vendor:rank", "transcribe")
    assert (descriptor.label, descriptor.description) == (
        "Native scorer",
        "Scores text and transcribes audio",
    )
    assert untagged_descriptor.label is None
    assert untagged_descriptor.description is None
    assert [
        (contract.request.mime_types, contract.output.mime_types)
        for contract in descriptor.contracts
    ] == [
        (("text/plain",), ("application/json",)),
        (("audio/L16;rate=16000",), ("text/plain",)),
    ]
    for record, field_name in (
        (descriptor, "contracts"),
        (descriptor, "operation_tags"),
        (contracts[0], "delivery"),
    ):
        with pytest.raises(AttributeError):
            setattr(record, field_name, ())


def test_descriptor_requires_contracts_with_distinct_ids() -> None:
    model_ref = ModelRef(plugin_id="plugin", provider="provider", model="deployment")
    original_contract = make_contract("score", "text/plain", "application/json")
    revised_contract = ModelContract(
        ref=ContractRef(id="score", revision="2"),
        request=original_contract.request,
        output=original_contract.output,
        delivery=original_contract.delivery,
        stream=None,
        accepts_output_schema=False,
    )
    for contracts in ((), (original_contract, revised_contract)):
        with pytest.raises((TypeError, ValueError)):
            ModelDescriptor(ref=model_ref, contracts=contracts, operation_tags=())


def test_contract_requires_delivery_and_a_format_for_streaming() -> None:
    text_format = DataFormat(schema={}, mime_types=("text/plain",))
    for delivery in (
        frozenset(),
        frozenset({"stream"}),
        frozenset({"complete", "stream"}),
        frozenset({"unknown"}),
    ):
        with pytest.raises((TypeError, ValueError)):
            ModelContract(
                ref=ContractRef(id="text", revision="1"),
                request=text_format,
                output=text_format,
                delivery=cast("Any", delivery),
                stream=None,
                accepts_output_schema=False,
            )
    for delivery in (
        frozenset({"complete", "stream", "job"}),
        frozenset({"stream"}),
        frozenset({"job"}),
    ):
        contract_with_stream_format = ModelContract(
            ref=ContractRef(id="text", revision="1"),
            request=text_format,
            output=text_format,
            delivery=cast("Any", delivery),
            stream=text_format,
            accepts_output_schema=True,
        )
        assert contract_with_stream_format.delivery == delivery
        assert contract_with_stream_format.stream == text_format
        assert contract_with_stream_format.accepts_output_schema is True
