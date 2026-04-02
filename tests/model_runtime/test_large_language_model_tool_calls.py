from typing import Any
from unittest.mock import MagicMock

import pytest

from graphon.model_runtime.entities.message_entities import AssistantPromptMessage
from graphon.model_runtime.model_providers.base.large_language_model import (
    merge_tool_call_deltas,
)

ToolCall = AssistantPromptMessage.ToolCall

INPUTS_CASE_1 = [
    ToolCall(
        id="1",
        type="function",
        function=ToolCall.ToolCallFunction(name="func_foo", arguments=""),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='{"arg1": '),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='"value"}'),
    ),
]
EXPECTED_CASE_1 = [
    ToolCall(
        id="1",
        type="function",
        function=ToolCall.ToolCallFunction(
            name="func_foo",
            arguments='{"arg1": "value"}',
        ),
    ),
]

INPUTS_CASE_2 = [
    ToolCall(
        id="1",
        type="function",
        function=ToolCall.ToolCallFunction(name="func_foo", arguments=""),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='{"arg1": '),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='"value"}'),
    ),
    ToolCall(
        id="2",
        type="function",
        function=ToolCall.ToolCallFunction(name="func_bar", arguments=""),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='{"arg2": '),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='"value"}'),
    ),
]
EXPECTED_CASE_2 = [
    ToolCall(
        id="1",
        type="function",
        function=ToolCall.ToolCallFunction(
            name="func_foo",
            arguments='{"arg1": "value"}',
        ),
    ),
    ToolCall(
        id="2",
        type="function",
        function=ToolCall.ToolCallFunction(
            name="func_bar",
            arguments='{"arg2": "value"}',
        ),
    ),
]

INPUTS_CASE_3 = [
    ToolCall(
        id="1",
        type="function",
        function=ToolCall.ToolCallFunction(name="func_foo", arguments=""),
    ),
    ToolCall(
        id="1",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='{"arg1": '),
    ),
    ToolCall(
        id="1",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='"value"}'),
    ),
    ToolCall(
        id="2",
        type="function",
        function=ToolCall.ToolCallFunction(name="func_bar", arguments=""),
    ),
    ToolCall(
        id="2",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='{"arg2": '),
    ),
    ToolCall(
        id="2",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='"value"}'),
    ),
]
EXPECTED_CASE_3 = [
    ToolCall(
        id="1",
        type="function",
        function=ToolCall.ToolCallFunction(
            name="func_foo",
            arguments='{"arg1": "value"}',
        ),
    ),
    ToolCall(
        id="2",
        type="function",
        function=ToolCall.ToolCallFunction(
            name="func_bar",
            arguments='{"arg2": "value"}',
        ),
    ),
]

INPUTS_CASE_4 = [
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="func_foo", arguments=""),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='{"arg1": '),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='"value"}'),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="func_bar", arguments=""),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='{"arg2": '),
    ),
    ToolCall(
        id="",
        type="function",
        function=ToolCall.ToolCallFunction(name="", arguments='"value"}'),
    ),
]
EXPECTED_CASE_4 = [
    ToolCall(
        id="RANDOM_ID_1",
        type="function",
        function=ToolCall.ToolCallFunction(
            name="func_foo",
            arguments='{"arg1": "value"}',
        ),
    ),
    ToolCall(
        id="RANDOM_ID_2",
        type="function",
        function=ToolCall.ToolCallFunction(
            name="func_bar",
            arguments='{"arg2": "value"}',
        ),
    ),
]


def _run_case(
    inputs: list[ToolCall],
    expected: list[ToolCall],
    *,
    id_generator: Any = None,
) -> None:
    actual = []
    merge_tool_call_deltas(inputs, actual, id_generator=id_generator)
    assert actual == expected


def test__merge_tool_call_deltas():
    _run_case(INPUTS_CASE_1, EXPECTED_CASE_1)
    _run_case(INPUTS_CASE_2, EXPECTED_CASE_2)
    _run_case(INPUTS_CASE_3, EXPECTED_CASE_3)

    mock_id_generator = MagicMock()
    mock_id_generator.side_effect = [
        expected_case.id for expected_case in EXPECTED_CASE_4
    ]
    _run_case(INPUTS_CASE_4, EXPECTED_CASE_4, id_generator=mock_id_generator)


def test__merge_tool_call_deltas__no_id_no_name_first_delta_should_raise():
    inputs = [
        ToolCall(
            id="",
            type="function",
            function=ToolCall.ToolCallFunction(name="", arguments='{"arg1": '),
        ),
        ToolCall(
            id="",
            type="function",
            function=ToolCall.ToolCallFunction(name="func_foo", arguments='"value"}'),
        ),
    ]
    actual: list[ToolCall] = []
    with pytest.raises(
        ValueError,
        match=r"no existing tool call is available to apply the delta",
    ):
        merge_tool_call_deltas(inputs, actual, id_generator=MagicMock())
