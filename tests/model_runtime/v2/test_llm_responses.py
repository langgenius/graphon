from jsonschema import Draft202012Validator

from graphon.model_runtime.v2 import LLM_CONTRACT


def test_llm_accepts_json_messages_outputs_and_tool_results() -> None:
    request_validator = Draft202012Validator(LLM_CONTRACT.request.schema)
    output_validator = Draft202012Validator(LLM_CONTRACT.output.schema)
    content = [
        {"type": "json", "value": value}
        for value in (False, None, 0, 1.5, "", [False, None, 3], {"ok": False})
    ]
    assert request_validator.is_valid({
        "input": {
            "messages": [
                {"role": "assistant", "content": content},
                {
                    "role": "tool",
                    "content": [
                        {"type": "tool_result", "call_id": "call-1", "content": content}
                    ],
                },
            ]
        },
        "parameters": {},
    })
    assert output_validator.is_valid({"content": content})
    assert not output_validator.is_valid({"content": [{"type": "json"}]})
    assert not request_validator.is_valid({
        "input": {
            "messages": [
                {
                    "role": "tool",
                    "content": [
                        {
                            "type": "tool_result",
                            "call_id": "call-1",
                            "content": [{"type": "json"}],
                        }
                    ],
                }
            ]
        },
        "parameters": {},
    })


def test_llm_accepts_reasoning_refusal_and_finish_reasons() -> None:
    request_validator = Draft202012Validator(LLM_CONTRACT.request.schema)
    output_validator = Draft202012Validator(LLM_CONTRACT.output.schema)
    for output in (
        {
            "content": [
                {"type": "reasoning", "text": "A provider-exposed summary."},
                {"type": "text", "text": "The answer."},
            ],
            "text": "The answer.",
            "finish_reason": "stop",
        },
        {"content": [{"type": "refusal", "text": "I cannot answer that request."}]},
        {"content": [], "finish_reason": "length"},
        {"text": "Partial answer", "finish_reason": "vendor_specific_stop"},
    ):
        assert output_validator.is_valid(output)
        if "content" in output:
            assert request_validator.is_valid({
                "input": {
                    "messages": [{"role": "assistant", "content": output["content"]}]
                },
                "parameters": {},
            })
    for content in (
        [{"type": "reasoning"}],
        [{"type": "reasoning", "text": None}],
        [{"type": "refusal", "text": False}],
    ):
        assert not output_validator.is_valid({
            "content": content,
            "text": "cannot bypass invalid content",
        })
        assert not request_validator.is_valid({
            "input": {"messages": [{"role": "assistant", "content": content}]},
            "parameters": {},
        })
    for finish_reason in (None, False, 1, []):
        assert not output_validator.is_valid({
            "content": [],
            "finish_reason": finish_reason,
        })


def test_llm_streams_reasoning_refusal_and_json() -> None:
    assert LLM_CONTRACT.stream is not None
    validator = Draft202012Validator(LLM_CONTRACT.stream.schema)
    for part_type in ("reasoning", "refusal"):
        assert validator.is_valid({
            "type": f"{part_type}_delta",
            "index": 0,
            "text": "",
        })
        assert validator.is_valid({
            "type": "content_part",
            "index": 1,
            "part": {"type": part_type, "text": "Visible summary"},
        })
        for invalid_preview in (
            {"type": f"{part_type}_delta", "text": "missing index"},
            {"type": f"{part_type}_delta", "index": -1, "text": "negative"},
            {"type": f"{part_type}_delta", "index": False, "text": "boolean"},
            {
                "type": f"{part_type}_delta",
                "index": 0,
                "text": None,
                "text_delta": "cannot bypass",
            },
        ):
            assert not validator.is_valid(invalid_preview)
    assert validator.is_valid({
        "type": "content_part",
        "index": 2,
        "part": {"type": "json", "value": [False, None]},
    })
    assert not validator.is_valid({
        "type": "content_part",
        "index": 2,
        "part": {"type": "json"},
    })
