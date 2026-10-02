from dataclasses import replace

from jsonschema import Draft202012Validator

from graphon.model_runtime.v2 import LLM_CONTRACT, ContractRef


def test_llm_contract_allows_model_local_identity_and_delivery() -> None:
    assert LLM_CONTRACT.request.profile == "graphon.llm.request/1"
    assert LLM_CONTRACT.output.profile == "graphon.llm.output/1"
    assert LLM_CONTRACT.delivery == frozenset({"complete"})
    assert not LLM_CONTRACT.accepts_output_schema
    assert LLM_CONTRACT.stream is not None
    assert LLM_CONTRACT.stream.profile == "graphon.llm.stream/1"
    for data_format in (
        LLM_CONTRACT.request,
        LLM_CONTRACT.output,
        LLM_CONTRACT.stream,
    ):
        assert data_format.kinds == ("text",)
        assert data_format.schema["$schema"] == Draft202012Validator.META_SCHEMA["$id"]
        Draft202012Validator.check_schema(data_format.schema)

    streaming_contract = replace(
        LLM_CONTRACT,
        ref=ContractRef(id="chat", revision="deployment-7"),
        delivery=frozenset({"stream"}),
    )
    assert streaming_contract.ref == ContractRef(id="chat", revision="deployment-7")
    assert streaming_contract.delivery == frozenset({"stream"})
    assert streaming_contract.request == LLM_CONTRACT.request
    assert streaming_contract.output == LLM_CONTRACT.output
    assert streaming_contract.stream == LLM_CONTRACT.stream


def test_llm_request_accepts_text_conversations_and_plugin_parameters() -> None:
    validator = Draft202012Validator(LLM_CONTRACT.request.schema)
    assert validator.is_valid({
        "input": {
            "messages": [
                {"role": "system", "content": " Preserve whitespace. "},
                {"role": "user", "content": "Hello\n世界", "name": "visitor"},
                {"role": "assistant", "content": ""},
                {"role": "vendor_role", "content": "Additional context"},
            ]
        },
        "parameters": {"vendor_setting": {"enabled": False}},
    })
    assert validator.is_valid({
        "input": {"messages": [{"role": "user", "content": "Hello"}]},
        "parameters": {},
    })
    for invalid_input in (
        "Hello",
        {},
        {"messages": "Hello"},
        {"messages": [{"role": "user"}]},
        {"messages": [{"content": "Hello"}]},
        {"messages": [{"role": 7, "content": "Hello"}]},
        {"messages": [{"role": "user", "content": None}]},
    ):
        assert not validator.is_valid({"input": invalid_input, "parameters": {}})
    valid_input = {"messages": [{"role": "user", "content": "Hello"}]}
    assert not validator.is_valid({"input": valid_input})
    assert not validator.is_valid({"parameters": {}})
    assert not validator.is_valid({"input": valid_input, "parameters": []})
    assert not validator.is_valid({
        "input": valid_input,
        "parameters": {},
        "unexpected_envelope_field": True,
    })


def test_llm_output_and_previews_require_text_and_allow_extra_fields() -> None:
    assert LLM_CONTRACT.stream is not None
    for data_format, text_field in (
        (LLM_CONTRACT.output, "text"),
        (LLM_CONTRACT.stream, "text_delta"),
    ):
        validator = Draft202012Validator(data_format.schema)
        assert validator.is_valid({text_field: ""})
        assert validator.is_valid({
            text_field: " Hello\n世界 ",
            "vendor_data": {"nested": [False, None, 0]},
        })
        for invalid_value in (
            None,
            False,
            0,
            "text",
            [],
            {},
            {text_field: None},
            {text_field: 7},
        ):
            assert not validator.is_valid(invalid_value)
    assert not Draft202012Validator(LLM_CONTRACT.output.schema).is_valid({
        "text_delta": "preview"
    })
    assert not Draft202012Validator(LLM_CONTRACT.stream.schema).is_valid({
        "text": "complete"
    })
