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
        assert data_format.mime_types == ()
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


def test_llm_output_and_previews_accept_text_shorthand_and_extra_fields() -> None:
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


def test_llm_messages_and_outputs_accept_ordered_multimodal_content() -> None:
    content = [
        {"type": "text", "text": "Compare these inputs", "vendor_note": "retained"},
        {
            "type": "media",
            "mime_type": "image/png",
            "detail": "high",
            "source": {"type": "uri", "uri": "https://example.com/image.png"},
        },
        {
            "type": "media",
            "mime_type": "audio/wav",
            "source": {"type": "inline", "encoding": "base64", "data": "YQ=="},
        },
        {
            "type": "media",
            "mime_type": "video/mp4",
            "source": {"type": "file", "id": "video-1"},
        },
        {
            "type": "media",
            "mime_type": "application/pdf",
            "source": {"type": "file", "id": "document-1"},
        },
        {
            "type": "media",
            "mime_type": "model/gltf+json",
            "source": {"type": "file", "id": "scene-1"},
        },
    ]
    assert Draft202012Validator(LLM_CONTRACT.request.schema).is_valid({
        "input": {"messages": [{"role": "user", "content": content}]},
        "parameters": {},
    })
    assert not Draft202012Validator(LLM_CONTRACT.request.schema).is_valid({
        "input": {"messages": [{"role": "user", "content": [{"type": "media"}]}]},
        "parameters": {},
    })
    output_validator = Draft202012Validator(LLM_CONTRACT.output.schema)
    assert output_validator.is_valid({"content": content, "provider_field": False})
    assert output_validator.is_valid({"content": [content[2]]})
    assert output_validator.is_valid({"content": []})
    assert output_validator.is_valid({
        "text": "Hello",
        "content": [{"type": "text", "text": "Hello"}],
    })
    assert not output_validator.is_valid({"text": "Hello", "content": "invalid"})


def test_llm_media_requires_one_declared_source() -> None:
    validator = Draft202012Validator(LLM_CONTRACT.output.schema)
    for source in (
        {},
        {"type": "uri", "uri": "https://example.com/image.png", "id": "ambiguous"},
        {"type": "file", "id": 7},
        {"type": "file", "uri": "https://example.com/image.png"},
        {"type": "inline", "encoding": "utf-8", "data": "raw bytes"},
    ):
        assert not validator.is_valid({
            "content": [{"type": "media", "mime_type": "image/png", "source": source}]
        })
    assert not validator.is_valid({
        "content": [{"type": "media", "source": {"type": "file", "id": "image-1"}}]
    })
    assert not validator.is_valid({"content": [{"type": "text", "text": 7}]})
    assert not validator.is_valid({"content": [{"type": "unknown", "text": "Hi"}]})
    for modality_tag in ("image", "audio", "video", "document"):
        assert not validator.is_valid({
            "content": [
                {
                    "type": modality_tag,
                    "mime_type": "image/png",
                    "source": {"type": "file", "id": "image-1"},
                }
            ]
        })


def test_llm_media_leaves_mime_interpretation_to_plugins() -> None:
    validator = Draft202012Validator(LLM_CONTRACT.output.schema)
    media_part = {
        "type": "media",
        "mime_type": "plugin-validated",
        "source": {"type": "file", "id": "asset-1"},
    }
    assert validator.is_valid({"content": [media_part]})
    for mime_type in ("", None, False, 7, []):
        assert not validator.is_valid({
            "content": [{**media_part, "mime_type": mime_type}]
        })


def test_llm_stream_previews_identify_content_parts() -> None:
    assert LLM_CONTRACT.stream is not None
    validator = Draft202012Validator(LLM_CONTRACT.stream.schema)
    assert validator.is_valid({"text_delta": "Legacy text shorthand"})
    assert validator.is_valid({"type": "text_delta", "index": 0, "text": "Hello"})
    assert validator.is_valid({
        "type": "content_part",
        "index": 1,
        "part": {
            "type": "media",
            "mime_type": "audio/L16;rate=16000;channels=1",
            "source": {"type": "file", "id": "audio-1"},
        },
    })
    for invalid_preview in (
        {"type": "text_delta", "text": "missing index"},
        {"type": "text_delta", "text_delta": "fallback", "text": "missing index"},
        {"type": "text_delta", "index": -1, "text": "negative"},
        {"type": "text_delta", "index": False, "text": "boolean"},
        {"type": "content_part", "index": 0, "part": {"type": "media"}},
        {"type": "unknown", "text_delta": "cannot bypass typed preview schema"},
    ):
        assert not validator.is_valid(invalid_preview)
