from jsonschema import Draft202012Validator

from graphon.model_runtime.v2 import LLM_CONTRACT


def test_llm_requests_declare_tools_and_select_calls() -> None:
    validator = Draft202012Validator(LLM_CONTRACT.request.schema)
    tool = {
        "name": "lookup",
        "description": "Find a record",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    }
    input_value = {
        "messages": [{"role": "user", "content": "Find this record"}],
        "tools": [tool, {"name": "refresh", "input_schema": {}}],
    }
    for choice in ("auto", "none", "required", {"name": "lookup"}):
        assert validator.is_valid({
            "input": input_value,
            "parameters": {
                "tool_choice": choice,
                "parallel_tool_calls": False,
                "vendor_setting": {"enabled": False, "limit": None},
            },
        })
    assert validator.is_valid({"input": input_value, "parameters": {}})
    for invalid_tools in (
        "lookup",
        [{"input_schema": {}}],
        [{"name": "", "input_schema": {}}],
        [{"name": "lookup", "input_schema": "object"}],
        [{"name": "lookup", "input_schema": {}, "description": 7}],
    ):
        assert not validator.is_valid({
            "input": {**input_value, "tools": invalid_tools},
            "parameters": {},
        })
    for invalid_parameters in (
        {"tool_choice": "sometimes"},
        {"tool_choice": {"name": ""}},
        {"tool_choice": {}},
        {"parallel_tool_calls": "false"},
    ):
        assert not validator.is_valid({
            "input": input_value,
            "parameters": invalid_parameters,
        })


def test_llm_tool_turns_allow_argument_objects_and_multimodal_results() -> None:
    calls = [
        {
            "type": "tool_call",
            "call_id": "lookup-1",
            "name": "lookup",
            "arguments": {"query": "record", "cached": False, "cursor": None},
        },
        {
            "type": "tool_call",
            "call_id": "refresh-1",
            "name": "refresh",
            "arguments": {},
        },
    ]
    results = [
        {
            "type": "tool_result",
            "call_id": "lookup-1",
            "content": [
                {"type": "text", "text": "Record found"},
                {
                    "type": "image",
                    "mime_type": "image/png",
                    "source": {"type": "uri", "uri": "https://example.com/record.png"},
                },
                {
                    "type": "document",
                    "mime_type": "application/pdf",
                    "source": {"type": "file", "id": "record-1"},
                },
            ],
        },
        {
            "type": "tool_result",
            "call_id": "refresh-1",
            "content": "Refresh unavailable",
            "is_error": True,
        },
    ]
    content = [{"type": "text", "text": "Checking"}, *calls, *results]
    request_validator = Draft202012Validator(LLM_CONTRACT.request.schema)
    assert request_validator.is_valid({
        "input": {
            "messages": [
                {"role": "assistant", "content": calls},
                {"role": "tool", "content": results},
                {"role": "assistant", "content": content},
            ]
        },
        "parameters": {},
    })
    output_validator = Draft202012Validator(LLM_CONTRACT.output.schema)
    assert output_validator.is_valid({"content": calls})
    assert output_validator.is_valid({"content": content})
    for invalid_part in (
        {"type": "tool_call", "name": "lookup", "arguments": {}},
        {**calls[0], "call_id": ""},
        {**calls[0], "name": 7},
        {**calls[0], "arguments": '{"query":"record"}'},
        {"type": "tool_result", "content": "Missing correlation ID"},
        {**results[0], "call_id": 7},
        {**results[0], "content": [calls[0]]},
        {**results[1], "is_error": "true"},
    ):
        assert not request_validator.is_valid({
            "input": {"messages": [{"role": "assistant", "content": [invalid_part]}]},
            "parameters": {},
        })
        assert not output_validator.is_valid({"content": [invalid_part]})


def test_llm_tool_call_previews_allow_incomplete_argument_fragments() -> None:
    assert LLM_CONTRACT.stream is not None
    validator = Draft202012Validator(LLM_CONTRACT.stream.schema)
    preview = {
        "type": "tool_call_delta",
        "index": 0,
        "call_id": "lookup-1",
        "name": "lookup",
        "arguments_delta": '{"query":',
    }
    assert validator.is_valid(preview)
    assert validator.is_valid({**preview, "arguments_delta": '"record"}'})
    assert validator.is_valid({
        **preview,
        "index": 1,
        "call_id": "refresh-1",
        "name": "refresh",
        "arguments_delta": "",
    })
    for invalid_preview in (
        {key: value for key, value in preview.items() if key != "call_id"},
        {**preview, "call_id": ""},
        {**preview, "name": 7},
        {**preview, "index": -1},
        {**preview, "index": False},
        {**preview, "arguments_delta": {}},
    ):
        assert not validator.is_valid(invalid_preview)
