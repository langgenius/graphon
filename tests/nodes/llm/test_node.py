import base64
import json
import re
from collections.abc import Generator, Iterable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, Never, cast
from unittest.mock import MagicMock

import pytest

from graphon.engine import Engine
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.frame import FrameStartedEvent
from graphon.engine_events.graph import (
    GraphRunPartialSucceededEvent,
    GraphRunPausedEvent,
    GraphRunSucceededEvent,
)
from graphon.engine_events.node import (
    NodeRunModelPollingProgressEvent,
    NodeRunReasoningChunkEvent,
    NodeRunRetryEvent,
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
)
from graphon.entities.base_node_data import BaseNodeData, RetryConfig
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.enums import WorkflowNodeExecutionStatus
from graphon.file import helpers as file_helpers
from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.graph.graph import Graph
from graphon.model_runtime.entities.common_entities import I18nObject
from graphon.model_runtime.entities.llm_entities import (
    LLMPollingConfig,
    LLMPollingResult,
    LLMPollingStatus,
    LLMResult,
    LLMResultChunk,
    LLMResultChunkDelta,
    LLMResultChunkWithStructuredOutput,
    LLMResultWithStructuredOutput,
    LLMStructuredOutput,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    PromptMessageContentUnionTypes,
    VideoPromptMessageContent,
)
from graphon.model_runtime.entities.model_entities import (
    AIModelEntity,
    FetchFrom,
    ModelFeature,
    ModelType,
)
from graphon.node_events.base import NodeEventPayload
from graphon.node_events.node import (
    ModelInvokeCompletedEvent,
    ModelPollingProgressEvent,
    StreamChunkEvent,
    StreamCompletedEvent,
    StreamReasoningEvent,
)
from graphon.nodes.answer.answer_node import AnswerNode
from graphon.nodes.answer.entities import AnswerNodeData
from graphon.nodes.human_input.entities import Completed, HITLCallback, PauseRequested
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.nodes.llm import LLMNode, LLMNodeData
from graphon.nodes.llm.exc import LLMNodeError
from graphon.nodes.llm.reasoning import split_reasoning
from graphon.nodes.llm.runtime_protocols import LLMPollingCapableProtocol, LLMProtocol
from graphon.nodes.start import StartNode
from graphon.nodes.start.entities import StartNodeData
from graphon.nodes.variable_assigner.v2.entities import (
    VariableAssignerNodeData,
    VariableOperationItem,
)
from graphon.nodes.variable_assigner.v2.enums import InputType, Operation
from graphon.nodes.variable_assigner.v2.node import VariableAssignerNode
from graphon.runtime.runtime_state import RuntimeState
from graphon.variables.segments import StringSegment

from ...helpers import build_init_params, build_variable_pool


class _PollingLLM(LLMPollingCapableProtocol):
    provider = "openai"
    model_name = "gpt-4o"
    stop = ()

    def __init__(self, responses: Sequence[object]) -> None:
        self.parameters = {}
        self.polling_config = LLMPollingConfig(
            min_check_interval_seconds=0.001,
            max_check_interval_seconds=0.01,
            max_wait_seconds=1,
            max_attempts=3,
            wake_interval_seconds=0.001,
        )
        self._responses = list(responses)
        self.start_calls: list[dict[str, Any]] = []
        self.check_calls: list[dict[str, Any]] = []

    def get_model_schema(self) -> AIModelEntity:
        return AIModelEntity(
            model=self.model_name,
            label=I18nObject(en_US=self.model_name),
            model_type=ModelType.LLM,
            features=[ModelFeature.POLLING],
            fetch_from=FetchFrom.PREDEFINED_MODEL,
            model_properties={},
        )

    def start_llm_polling(self, **kwargs: Any) -> Any:
        self.start_calls.append(kwargs)
        return self._responses.pop(0)

    def check_llm_polling(self, **kwargs: Any) -> Any:
        self.check_calls.append(kwargs)
        return self._responses.pop(0)

    def invoke_llm(self, **_: Any) -> Never:
        msg = "streaming invoke should not be used"
        raise AssertionError(msg)

    def invoke_llm_with_structured_output(self, **_: Any) -> Never:
        msg = "structured streaming invoke should not be used"
        raise AssertionError(msg)

    def is_structured_output_parse_error(self, _error: Exception) -> bool:
        return False


def _llm_result(text: str = "final answer") -> LLMResult:
    return LLMResult(
        model="gpt-4o",
        prompt_messages=[],
        message=AssistantPromptMessage(content=text),
        usage=LLMUsage.empty_usage(),
    )


def _stream_chunk(
    content: str | list[PromptMessageContentUnionTypes] | None,
    *,
    usage: LLMUsage | None = None,
    finish_reason: str | None = None,
) -> LLMResultChunk:
    return LLMResultChunk(
        model="gpt-4o",
        delta=LLMResultChunkDelta(
            index=0,
            message=AssistantPromptMessage(content=content),
            usage=usage,
            finish_reason=finish_reason,
        ),
    )


def _stream_results(
    *results: LLMResultChunk | LLMStructuredOutput,
) -> Generator[LLMResultChunk | LLMStructuredOutput, None, None]:
    yield from results


def _build_llm_node(
    *,
    model_instance: object | None = None,
    prompt_text: str = "Hello",
    variables: Sequence[tuple[Sequence[str], Any]] = (),
    workflow_run_id: str | None = "wr-test",
    run_context: dict[str, Any] | None = None,
    llm_file_saver: Any | None = None,
    runtime_state: RuntimeState | None = None,
) -> LLMNode:
    prepared_model = model_instance
    if prepared_model is None:
        prepared_model = MagicMock(
            provider="openai",
            model_name="gpt-4o",
            parameters={},
            stop=(),
        )
    prepared_variables = []
    if workflow_run_id is not None:
        prepared_variables.append((("sys", "workflow_run_id"), workflow_run_id))
    prepared_variables.extend(variables)

    node = LLMNode(
        node_id="llm",
        data=LLMNodeData.model_validate({
            "title": "LLM",
            "model": {
                "provider": "openai",
                "name": "gpt-4o",
                "mode": "chat",
                "completion_params": {},
            },
            "prompt_template": [
                {
                    "role": "user",
                    "text": prompt_text,
                }
            ],
            "context": {"enabled": False},
        }),
        init_params=build_init_params(
            graph_config={"nodes": [], "edges": []},
            run_context=run_context,
        ),
        runtime_state=runtime_state
        or RuntimeState(
            workflow_id="workflow",
            variable_pool=build_variable_pool(variables=prepared_variables),
            start_at=0.0,
        ),
        model_instance=cast(LLMProtocol, prepared_model),
        llm_file_saver=MagicMock() if llm_file_saver is None else llm_file_saver,
        prompt_message_serializer=MagicMock(
            serialize=MagicMock(return_value=[]),
        ),
    )
    node.bind_execution_id("llm-run")
    return node


def _stub_simple_prompt(monkeypatch: pytest.MonkeyPatch, node: LLMNode) -> None:
    monkeypatch.setattr(node, "_fetch_inputs", lambda **_: {})
    monkeypatch.setattr(node, "_fetch_jinja_inputs", lambda **_: {})
    monkeypatch.setattr(node, "_fetch_context", lambda **_: iter(()))
    monkeypatch.setattr(
        "graphon.nodes.llm.node.llm_utils.fetch_prompt_messages",
        lambda **_: ([], None),
    )


@pytest.mark.parametrize(
    "switch_values",
    [
        {"structured_output_enabled": True},
        {"structured_output_switch_on": True},
        {
            "structured_output_enabled": False,
            "structured_output_switch_on": True,
        },
    ],
    ids=["legacy", "current", "current-wins-conflict"],
)
def test_structured_output_switch_survives_node_data_round_trip(
    switch_values: dict[str, bool],
) -> None:
    payload = {
        "type": "llm",
        "model": {
            "provider": "openai",
            "name": "gpt-4o",
            "mode": "chat",
        },
        "prompt_template": [{"role": "user", "text": "Hello"}],
        "context": {"enabled": False},
        "structured_output": {"schema": {"type": "object"}},
        **switch_values,
    }

    node_data = LLMNode.validate_node_data(BaseNodeData.model_validate(payload))
    restored = LLMNode.validate_node_data(node_data)
    dumped = restored.model_dump(mode="python", by_alias=True)
    restored_from_dump = LLMNode.validate_node_data(dumped)

    assert restored.structured_output_switch_on is True
    assert restored.structured_output_enabled is True
    assert dumped["structured_output_switch_on"] is True
    assert "structured_output_enabled" not in dumped
    assert restored_from_dump.structured_output_switch_on is True


def test_fetch_structured_output_schema_checks_draft7_schema() -> None:
    with pytest.raises(LLMNodeError) as exc_info:
        LLMNode.fetch_structured_output_schema(
            structured_output={"schema": {"type": "not-a-json-schema-type"}},
        )

    assert "stage=schema" in str(exc_info.value)
    assert "path=$.type" in str(exc_info.value)


def test_final_structured_output_validation_disables_remote_ref_retrieval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_urlopen(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("remote schema retrieval was attempted")

    monkeypatch.setattr("urllib.request.urlopen", fail_urlopen)

    with pytest.raises(LLMNodeError) as exc_info:
        LLMNode._validate_structured_output_result(
            structured_output={},
            json_schema={"$ref": "http://127.0.0.1/schema"},
        )

    assert "stage=result" in str(exc_info.value)
    assert "path=$" in str(exc_info.value)
    assert "schema reference could not be resolved" in str(exc_info.value)


def test_final_structured_output_validation_supports_local_refs() -> None:
    schema = {
        "definitions": {
            "result": {
                "type": "object",
                "properties": {"ok": {"type": "boolean"}},
                "required": ["ok"],
            },
        },
        "$ref": "#/definitions/result",
    }

    LLMNode._validate_structured_output_result(
        structured_output={"ok": True},
        json_schema=schema,
    )


_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "profile": {
            "type": "object",
            "properties": {
                "role": {"type": "string", "enum": ["admin"]},
                "scores": {"type": "array", "items": {"type": "integer"}},
            },
            "required": ["role", "scores"],
        },
    },
    "required": ["profile"],
}


@pytest.mark.parametrize(
    ("structured_output", "path"),
    [
        (None, "$"),
        ({}, "$"),
        ({"profile": {}}, "$.profile"),
        ({"profile": {"role": "viewer", "scores": [1]}}, "$.profile.role"),
        (
            {"profile": {"role": "admin", "scores": [1, "invalid"]}},
            "$.profile.scores[1]",
        ),
    ],
)
def test_final_structured_output_is_validated_against_full_schema(
    structured_output: dict[str, Any] | None,
    path: str,
) -> None:
    chunk = LLMResultChunkWithStructuredOutput(
        model="gpt-4o",
        delta=LLMResultChunkDelta(
            index=0,
            message=AssistantPromptMessage(content=""),
            usage=LLMUsage.empty_usage(),
        ),
        structured_output=structured_output,
    )
    model = MagicMock(is_structured_output_parse_error=lambda _error: False)

    with pytest.raises(LLMNodeError) as exc_info:
        list(
            LLMNode.handle_invoke_result(
                invoke_result=_stream_results(chunk),
                file_saver=MagicMock(),
                file_outputs=[],
                node_id="llm",
                model_instance=cast(LLMProtocol, model),
                json_schema=_RESULT_SCHEMA,
            ),
        )

    assert "stage=result" in str(exc_info.value)
    assert f"path={path}" in str(exc_info.value)
    if structured_output is None:
        assert "structured output is missing" in str(exc_info.value)


def test_blocking_structured_output_is_validated_against_schema() -> None:
    result = LLMResultWithStructuredOutput(
        model="gpt-4o",
        message=AssistantPromptMessage(content=""),
        usage=LLMUsage.empty_usage(),
        structured_output={"profile": {"role": "admin", "scores": ["invalid"]}},
    )

    with pytest.raises(LLMNodeError, match=r"stage=result, path=\$\.profile\.scores"):
        list(
            LLMNode.handle_invoke_result(
                invoke_result=result,
                file_saver=MagicMock(),
                file_outputs=[],
                node_id="llm",
                model_instance=cast(LLMProtocol, MagicMock()),
                json_schema=_RESULT_SCHEMA,
            ),
        )


@pytest.mark.parametrize(
    ("features", "should_invoke"),
    [
        ([ModelFeature.STRUCTURED_OUTPUT], True),
        ([], False),
        (None, True),
    ],
    ids=["supported", "unsupported", "unknown"],
)
def test_structured_output_capability_is_tri_state(
    features: list[ModelFeature] | None,
    should_invoke: bool,
) -> None:
    model = MagicMock(
        provider="openai",
        model_name="gpt-4o",
        parameters={},
        stop=(),
        is_structured_output_parse_error=lambda _error: False,
    )
    model.get_model_schema.return_value = SimpleNamespace(
        features=features,
        supports_prompt_content_type=lambda _content_type: True,
    )
    model.invoke_llm_with_structured_output.return_value = _stream_results(
        LLMResultChunkWithStructuredOutput(
            model="gpt-4o",
            delta=LLMResultChunkDelta(
                index=0,
                message=AssistantPromptMessage(content=""),
                usage=LLMUsage.empty_usage(),
            ),
            structured_output={},
        ),
    )
    node = _build_llm_node(model_instance=model)
    node.node_data.structured_output_switch_on = True
    node.node_data.structured_output = {"schema": {"type": "object"}}

    completed = next(
        event for event in node._run() if isinstance(event, StreamCompletedEvent)
    )

    if should_invoke:
        model.invoke_llm_with_structured_output.assert_called_once()
        assert completed.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
        assert completed.node_run_result.outputs["structured_output"] == {}
    else:
        model.invoke_llm_with_structured_output.assert_not_called()
        assert completed.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
        assert "stage=capability" in completed.node_run_result.error


def test_run_emits_model_identity_in_node_result_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _build_llm_node()

    _stub_simple_prompt(monkeypatch, node)
    monkeypatch.setattr(
        "graphon.nodes.llm.node.LLMNode.invoke_llm",
        lambda **_: iter([
            ModelInvokeCompletedEvent(
                text="Hello back",
                usage=LLMUsage.empty_usage(),
                finish_reason="stop",
            ),
        ]),
    )

    events = list(node._run())
    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )

    assert completed_event.node_run_result.inputs["model_provider"] == "openai"
    assert completed_event.node_run_result.inputs["model_name"] == "gpt-4o"


def test_run_records_resolved_prompt_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _build_llm_node(
        prompt_text="{{#source.value#}}",
        variables=[(("source", "value"), "recorded")],
    )
    monkeypatch.setattr(
        "graphon.nodes.llm.node.llm_utils.fetch_prompt_messages",
        lambda **_: ([], None),
    )
    monkeypatch.setattr(
        "graphon.nodes.llm.node.LLMNode.invoke_llm",
        lambda **_: iter([
            ModelInvokeCompletedEvent(
                text="ok",
                usage=LLMUsage.empty_usage(),
                finish_reason="stop",
            ),
        ]),
    )

    completed_event = next(
        event for event in node._run() if isinstance(event, StreamCompletedEvent)
    )

    assert completed_event.node_run_result.inputs["#source.value#"] == "recorded"


def test_fetch_inputs_normalizes_none_to_empty_string() -> None:
    node = _build_llm_node(
        prompt_text="{{#source.value#}}",
        variables=[(("source", "value"), None)],
    )

    assert node._fetch_inputs(node.node_data) == {"#source.value#": ""}


@pytest.mark.parametrize("word", ["red", "blue"])
def test_run_sends_hyphenated_node_output_to_model(word: str) -> None:
    model = MagicMock(
        provider="openai",
        model_name="gpt-4o",
        parameters={},
        stop=(),
    )
    model.get_model_schema.return_value.supports_prompt_content_type.return_value = True
    model.invoke_llm.return_value = _stream_results(
        _stream_chunk("ok", usage=LLMUsage.empty_usage(), finish_reason="stop"),
    )
    node = _build_llm_node(
        model_instance=model,
        prompt_text="Please process: {{#input-check.validated_input#}}",
        variables=[(("input-check", "validated_input"), word)],
    )

    completed = next(
        event for event in node._run() if isinstance(event, StreamCompletedEvent)
    )

    assert completed.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    model.invoke_llm.assert_called_once()
    messages = model.invoke_llm.call_args.kwargs["prompt_messages"]
    assert messages[0].content == f"Please process: {word}"
    assert completed.node_run_result.inputs["#input-check.validated_input#"] == word


def test_run_does_not_reuse_file_outputs_after_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    saved_file = File(
        file_id="saved",
        file_type=FileType.VIDEO,
        transfer_method=FileTransferMethod.TOOL_FILE,
        reference="saved",
        filename="video.mp4",
        extension=".mp4",
        mime_type="video/mp4",
        size=1,
    )
    attempts = 0

    def invoke(
        **kwargs: Any,
    ) -> Generator[
        NodeEventPayload | LLMStructuredOutput,
        None,
        None,
    ]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            kwargs["file_outputs"].append(saved_file)
            msg = "retry"
            raise LLMNodeError(msg)
        yield ModelInvokeCompletedEvent(
            text="ok",
            usage=LLMUsage.empty_usage(),
            finish_reason="stop",
        )

    node = _build_llm_node()
    _stub_simple_prompt(monkeypatch, node)
    monkeypatch.setattr("graphon.nodes.llm.node.LLMNode.invoke_llm", invoke)

    first_completed = next(
        event for event in node._run() if isinstance(event, StreamCompletedEvent)
    )
    second_completed = next(
        event for event in node._run() if isinstance(event, StreamCompletedEvent)
    )

    assert first_completed.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
    assert (
        second_completed.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    )
    assert "files" not in second_completed.node_run_result.outputs


@pytest.mark.parametrize(
    ("invoke_events", "expected_error"),
    [
        ([], "without a completion event"),
        ([NodeEventPayload()], "Unexpected LLM invocation event: NodeEventPayload"),
    ],
)
def test_run_rejects_invalid_invocation_event_sequence(
    monkeypatch: pytest.MonkeyPatch,
    invoke_events: list[NodeEventPayload],
    expected_error: str,
) -> None:
    node = _build_llm_node()
    _stub_simple_prompt(monkeypatch, node)
    monkeypatch.setattr(
        "graphon.nodes.llm.node.LLMNode.invoke_llm",
        lambda **_: iter(invoke_events),
    )

    completed_event = next(
        event for event in node._run() if isinstance(event, StreamCompletedEvent)
    )

    assert completed_event.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
    assert completed_event.node_run_result.error_type == "LLMNodeError"
    assert expected_error in completed_event.node_run_result.error


def test_polling_llm_start_can_succeed_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _PollingLLM([
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=_llm_result("done"),
        ),
    ])
    node = _build_llm_node(
        model_instance=model,
        variables=[(("sys", "workflow_run_id"), "wr-1")],
    )
    _stub_simple_prompt(monkeypatch, node)

    events = list(node._run())

    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert (
        completed_event.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    )
    assert completed_event.node_run_result.outputs["text"] == "done"
    assert model.check_calls == []


def test_polling_llm_checks_until_success(monkeypatch: pytest.MonkeyPatch) -> None:
    model = _PollingLLM([
        LLMPollingResult(
            status=LLMPollingStatus.RUNNING,
            plugin_state={"job_id": "job-1"},
            next_check_after_seconds=1,
        ),
        LLMPollingResult(
            status=LLMPollingStatus.RUNNING,
            plugin_state={"job_id": "job-1", "cursor": "2"},
            next_check_after_seconds=1,
        ),
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=_llm_result("checked"),
        ),
    ])
    node = _build_llm_node(model_instance=model)
    _stub_simple_prompt(monkeypatch, node)

    events = list(node._run())

    progress_events = [
        event for event in events if isinstance(event, ModelPollingProgressEvent)
    ]
    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert [event.attempt for event in progress_events] == [0, 1]
    assert model.check_calls[0]["plugin_state"] == {"job_id": "job-1"}
    assert model.check_calls[1]["plugin_state"] == {
        "job_id": "job-1",
        "cursor": "2",
    }
    assert completed_event.node_run_result.outputs["text"] == "checked"


def test_polling_llm_saves_video_output_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    saved_file = File(
        file_id="tool-file-id",
        file_type=FileType.VIDEO,
        transfer_method=FileTransferMethod.TOOL_FILE,
        reference="tool-file-id",
        filename="video.mp4",
        extension=".mp4",
        mime_type="video/mp4",
        size=1024,
    )
    file_saver = MagicMock()
    file_saver.save_remote_url.return_value = saved_file
    model = _PollingLLM([
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=LLMResult(
                model="seedance",
                prompt_messages=[],
                message=AssistantPromptMessage(
                    content=[
                        VideoPromptMessageContent(
                            format="mp4",
                            mime_type="video/mp4",
                            url="https://example.com/video.mp4",
                            filename="video.mp4",
                        ),
                    ],
                ),
                usage=LLMUsage.empty_usage(),
            ),
        ),
    ])
    node = _build_llm_node(model_instance=model, llm_file_saver=file_saver)
    _stub_simple_prompt(monkeypatch, node)
    monkeypatch.setattr(
        file_helpers,
        "resolve_file_url",
        lambda _file, **_kwargs: "https://files.example.com/video.mp4",
    )

    events = list(node._run())

    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert completed_event.node_run_result.outputs["text"] == (
        "[video.mp4](https://files.example.com/video.mp4)"
    )
    assert [
        (event.chunk, event.is_final)
        for event in events
        if isinstance(event, StreamChunkEvent) and event.selector == ["llm", "text"]
    ] == [(completed_event.node_run_result.outputs["text"], True)]
    assert completed_event.node_run_result.outputs["files"].value == [saved_file]
    file_saver.save_remote_url.assert_called_once_with(
        "https://example.com/video.mp4",
        FileType.VIDEO,
    )


def test_streaming_invoke_result_emits_chunks_and_completion() -> None:
    usage = LLMUsage.empty_usage().model_copy(
        update={"prompt_tokens": 1, "total_tokens": 1},
    )
    model = MagicMock(is_structured_output_parse_error=lambda _error: False)

    events = list(
        LLMNode.handle_invoke_result(
            invoke_result=_stream_results(
                _stream_chunk("hello", usage=usage, finish_reason="stop"),
            ),
            file_saver=MagicMock(),
            file_outputs=[],
            node_id="llm",
            model_instance=cast(LLMProtocol, model),
        ),
    )

    stream_event = next(
        event for event in events if isinstance(event, StreamChunkEvent)
    )
    completed_event = next(
        event for event in events if isinstance(event, ModelInvokeCompletedEvent)
    )
    assert stream_event.chunk == "hello"
    assert completed_event.text == "hello"
    assert completed_event.finish_reason == "stop"
    assert completed_event.usage.time_to_first_token is not None


def test_structured_streaming_chunk_is_forwarded_and_processed() -> None:
    model = MagicMock(is_structured_output_parse_error=lambda _error: False)
    structured_chunk = LLMResultChunkWithStructuredOutput(
        model="gpt-4o",
        delta=LLMResultChunkDelta(
            index=0,
            message=AssistantPromptMessage(content="hello"),
            usage=LLMUsage.empty_usage(),
        ),
        structured_output={"ok": True},
    )

    events = list(
        LLMNode.handle_invoke_result(
            invoke_result=_stream_results(structured_chunk),
            file_saver=MagicMock(),
            file_outputs=[],
            node_id="llm",
            model_instance=cast(LLMProtocol, model),
        ),
    )

    assert events[0] == structured_chunk
    assert any(
        isinstance(event, StreamChunkEvent) and event.chunk == "hello"
        for event in events
    )
    completed_event = next(
        event for event in events if isinstance(event, ModelInvokeCompletedEvent)
    )
    assert completed_event.structured_output == {"ok": True}


def test_streaming_structured_parse_error_is_converted() -> None:
    class StructuredParseError(Exception):
        pass

    def _failing_stream() -> Generator[
        LLMResultChunk | LLMStructuredOutput,
        None,
        None,
    ]:
        msg = "invalid payload"
        raise StructuredParseError(msg)
        yield

    model = MagicMock(
        is_structured_output_parse_error=lambda error: isinstance(
            error,
            StructuredParseError,
        ),
    )

    with pytest.raises(LLMNodeError, match="Failed to parse structured output"):
        list(
            LLMNode.handle_invoke_result(
                invoke_result=_failing_stream(),
                file_saver=MagicMock(),
                file_outputs=[],
                node_id="llm",
                model_instance=cast(LLMProtocol, model),
            ),
        )


def test_empty_streaming_content_does_not_set_first_token_time() -> None:
    usage = LLMUsage.empty_usage().model_copy(
        update={"prompt_tokens": 1, "total_tokens": 1},
    )
    model = MagicMock(is_structured_output_parse_error=lambda _error: False)

    events = list(
        LLMNode.handle_invoke_result(
            invoke_result=_stream_results(_stream_chunk("", usage=usage)),
            file_saver=MagicMock(),
            file_outputs=[],
            node_id="llm",
            model_instance=cast(LLMProtocol, model),
        ),
    )

    completed_event = next(
        event for event in events if isinstance(event, ModelInvokeCompletedEvent)
    )
    assert completed_event.usage.time_to_first_token is None
    assert completed_event.usage.time_to_generate is None


def test_save_multimodal_output_persists_inline_video() -> None:
    file_saver = MagicMock()
    saved_file = MagicMock()
    file_saver.save_binary_string.return_value = saved_file
    content = VideoPromptMessageContent(
        format="mp4",
        mime_type="video/mp4",
        base64_data=base64.b64encode(b"video-bytes").decode(),
        filename="clip.mp4",
    )

    result = LLMNode.save_multimodal_output(
        content=content,
        file_saver=file_saver,
    )

    assert result is saved_file
    file_saver.save_binary_string.assert_called_once_with(
        data=b"video-bytes",
        mime_type="video/mp4",
        file_type=FileType.VIDEO,
        extension_override=".mp4",
    )


def test_save_multimodal_output_requires_data_source() -> None:
    file_saver = MagicMock()
    content = VideoPromptMessageContent(
        format="mp4",
        mime_type="video/mp4",
        filename="clip.mp4",
    )

    with pytest.raises(ValueError, match="url or base64_data"):
        LLMNode.save_multimodal_output(
            content=content,
            file_saver=file_saver,
        )

    file_saver.save_binary_string.assert_not_called()
    file_saver.save_remote_url.assert_not_called()


def test_save_multimodal_output_rejects_invalid_inline_data() -> None:
    file_saver = MagicMock()
    content = VideoPromptMessageContent(
        format="mp4",
        mime_type="video/mp4",
        base64_data="not valid base64",
        filename="clip.mp4",
    )

    with pytest.raises(ValueError, match="base64_data is invalid"):
        LLMNode.save_multimodal_output(
            content=content,
            file_saver=file_saver,
        )

    file_saver.save_binary_string.assert_not_called()
    file_saver.save_remote_url.assert_not_called()


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            {"status": "running"},
            "plugin_state",
        ),
        (
            {"status": "succeeded"},
            "result is required",
        ),
        (
            {"status": "failed"},
            "error is required",
        ),
    ],
)
def test_polling_llm_rejects_invalid_terminal_or_running_payloads(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
    message: str,
) -> None:
    node = _build_llm_node(model_instance=_PollingLLM([payload]))
    _stub_simple_prompt(monkeypatch, node)

    events = list(node._run())

    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert completed_event.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
    assert message in completed_event.node_run_result.error


def test_polling_llm_fails_when_max_attempts_are_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _PollingLLM([
        LLMPollingResult(
            status=LLMPollingStatus.RUNNING,
            plugin_state={"job_id": "job-1"},
            next_check_after_seconds=1,
            max_attempts=1,
        ),
        LLMPollingResult(
            status=LLMPollingStatus.RUNNING,
            plugin_state={"job_id": "job-1"},
            next_check_after_seconds=1,
        ),
    ])
    node = _build_llm_node(model_instance=model)
    _stub_simple_prompt(monkeypatch, node)

    events = list(node._run())

    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert model.check_calls == [
        {
            "plugin_state": {"job_id": "job-1"},
        },
    ]
    assert completed_event.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
    assert "exceeded max attempts" in completed_event.node_run_result.error


def test_polling_llm_respects_existing_abort_before_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _PollingLLM([
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=_llm_result(),
        ),
    ])
    node = _build_llm_node(model_instance=model)
    node.runtime_state.graph_execution.abort("stop")
    _stub_simple_prompt(monkeypatch, node)

    events = list(node._run())

    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert model.start_calls == []
    assert completed_event.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
    assert "aborted" in completed_event.node_run_result.error


def test_polling_llm_not_use_run_context_workflow_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _PollingLLM([
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=_llm_result("done"),
        ),
    ])
    node = _build_llm_node(
        model_instance=model,
        workflow_run_id=None,
        run_context={"workflow_run_id": "wr-context"},
    )
    _stub_simple_prompt(monkeypatch, node)

    events = list(node._run())

    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert (
        completed_event.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    )
    assert completed_event.node_run_result.outputs["text"] == "done"


def test_polling_llm_ignores_schema_feature_without_capability_base() -> None:
    model = MagicMock(
        provider="openai",
        model_name="gpt-4o",
        parameters={},
        stop=(),
    )
    model.get_model_schema.return_value = SimpleNamespace(
        features=[ModelFeature.POLLING]
    )
    node = _build_llm_node(model_instance=model)

    assert node._polling_model_instance() is None


def test_polling_llm_requires_capability_base_for_polling_methods() -> None:
    model = MagicMock(
        provider="openai",
        model_name="gpt-4o",
        parameters={},
        stop=(),
    )
    model.start_llm_polling = MagicMock()
    model.check_llm_polling = MagicMock()
    node = _build_llm_node(model_instance=model)

    assert node._polling_model_instance() is None


def test_polling_llm_fails_when_response_arrives_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _PollingLLM([
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=_llm_result("late"),
        ),
    ])
    node = _build_llm_node(model_instance=model)
    _stub_simple_prompt(monkeypatch, node)

    ticks = iter([0.0, 1.1])

    def perf_counter() -> float:
        return next(ticks, 1.1)

    monkeypatch.setattr(
        "graphon.nodes.llm.node.time", SimpleNamespace(perf_counter=perf_counter)
    )

    events = list(node._run())

    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert completed_event.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
    assert "timed out" in completed_event.node_run_result.error


def test_polling_progress_event_omits_next_check_when_deadline_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "graphon.nodes.llm.node.time", SimpleNamespace(perf_counter=lambda: 10.0)
    )

    event = LLMNode._build_polling_progress_event(
        attempt=0,
        delay_seconds=5,
        deadline=12,
    )

    assert event.next_check_at is None


def test_polling_progress_event_keeps_next_check_when_delay_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "graphon.nodes.llm.node.time", SimpleNamespace(perf_counter=lambda: 10.0)
    )

    event = LLMNode._build_polling_progress_event(
        attempt=0,
        delay_seconds=5,
        deadline=20,
    )

    assert event.next_check_at == event.last_checked_at + timedelta(seconds=5)


def test_polling_progress_event_dispatches_to_engine_event() -> None:
    node = _build_llm_node()
    progress_event = ModelPollingProgressEvent(
        attempt=2,
        last_checked_at=datetime(2026, 5, 19, tzinfo=UTC).replace(tzinfo=None),
        next_check_at=None,
    )

    engine_event = node._dispatch(progress_event)

    assert isinstance(engine_event, NodeRunModelPollingProgressEvent)
    assert engine_event.attempt == 2


def _collect_stream_events(
    parts: Sequence[str],
    *,
    reasoning_format: Literal["separated", "tagged"],
) -> list[NodeEventPayload]:
    """Stream ``parts`` through the LLM node and return every emitted event."""
    model = MagicMock(is_structured_output_parse_error=lambda _error: False)
    return [
        event
        for event in LLMNode.handle_invoke_result(
            invoke_result=_stream_results(*[_stream_chunk(part) for part in parts]),
            file_saver=MagicMock(),
            file_outputs=[],
            node_id="llm",
            model_instance=cast(LLMProtocol, model),
            reasoning_format=reasoning_format,
        )
        if isinstance(event, NodeEventPayload)
    ]


def _run_stream(
    parts: Sequence[str],
    *,
    reasoning_format: Literal["separated", "tagged"],
) -> tuple[list[str], ModelInvokeCompletedEvent]:
    """Stream ``parts`` and return (clean text chunks, completion)."""
    events = _collect_stream_events(parts, reasoning_format=reasoning_format)
    chunks = [e.chunk for e in events if isinstance(e, StreamChunkEvent)]
    completed = next(e for e in events if isinstance(e, ModelInvokeCompletedEvent))
    return chunks, completed


def test_separated_stream_strips_think_in_single_chunk() -> None:
    chunks, completed = _run_stream(
        ["<think>plan</think>answer"],
        reasoning_format="separated",
    )

    assert "".join(chunks) == "answer"
    assert all("<think>" not in chunk for chunk in chunks)
    assert completed.text == "answer"
    assert completed.reasoning_content == "plan"


def test_separated_stream_strips_think_split_across_chunks() -> None:
    chunks, completed = _run_stream(
        ["<thi", "nk>plan</thi", "nk>ans", "wer"],
        reasoning_format="separated",
    )

    assert "".join(chunks) == "answer"
    assert all("think" not in chunk for chunk in chunks)
    assert completed.text == "answer"
    assert completed.reasoning_content == "plan"


def test_separated_stream_handles_think_tag_attributes() -> None:
    chunks, completed = _run_stream(
        ['<think foo="x">p</think>hi'],
        reasoning_format="separated",
    )

    assert "".join(chunks) == "hi"
    assert completed.text == "hi"
    assert completed.reasoning_content == "p"


def test_separated_stream_handles_multiple_think_blocks() -> None:
    chunks, completed = _run_stream(
        ["<think>a</think>X<think>b</think>Y"],
        reasoning_format="separated",
    )

    assert "".join(chunks) == "XY"
    assert completed.text == "XY"
    assert completed.reasoning_content == "a\nb"


def test_separated_stream_drops_unclosed_trailing_think() -> None:
    chunks, completed = _run_stream(
        ["hi<think>tail"],
        reasoning_format="separated",
    )

    assert "".join(chunks) == "hi"
    assert all("<think>" not in chunk for chunk in chunks)
    # Truncated reasoning is kept out of the text but preserved as reasoning.
    assert completed.text == "hi"
    assert completed.reasoning_content == "tail"


def test_separated_stream_does_not_swallow_non_think_angle_brackets() -> None:
    chunks, _ = _run_stream(["<div>ok"], reasoning_format="separated")

    assert "".join(chunks) == "<div>ok"


def test_separated_stream_keeps_tags_with_think_prefix() -> None:
    chunks, completed = _run_stream(
        ["before<think", "ing>idea</thinking>after"],
        reasoning_format="separated",
    )

    assert "".join(chunks) == "before<thinking>idea</thinking>after"
    assert completed.text == "before<thinking>idea</thinking>after"
    assert completed.reasoning_content == ""


def test_separated_stream_keeps_malformed_open_tag_with_nested_bracket() -> None:
    chunks, completed = _run_stream(
        ["x<think <y", ">secret</think>z"],
        reasoning_format="separated",
    )

    assert "".join(chunks) == "x<think <y>secret</think>z"
    assert completed.text == "x<think <y>secret</think>z"
    assert completed.reasoning_content == ""


def test_separated_stream_flushes_held_partial_open_on_finalize() -> None:
    # Stream ends on a held partial open: finalize() must flush "<thi", not drop it.
    chunks, completed = _run_stream(["answer<thi"], reasoning_format="separated")

    assert "".join(chunks) == "answer<thi"
    assert chunks[-1] == "<thi"  # from the finalize() flush, not feed()
    assert completed.text == "answer<thi"


def test_separated_stream_strips_leading_whitespace_after_reasoning() -> None:
    chunks, completed = _run_stream(
        ["<think>r</think>", "\n", "answer"],
        reasoning_format="separated",
    )

    assert "".join(chunks) == "answer"
    assert completed.text == "answer"


def test_tagged_stream_keeps_think_tags_unchanged() -> None:
    chunks, completed = _run_stream(
        ["<think>plan</think>answer"],
        reasoning_format="tagged",
    )

    assert "".join(chunks) == "<think>plan</think>answer"
    assert completed.text == "<think>plan</think>answer"
    assert completed.reasoning_content == ""


def _reasoning_chunks(
    parts: Sequence[str],
    *,
    reasoning_format: Literal["separated", "tagged"] = "separated",
) -> list[StreamReasoningEvent]:
    events = _collect_stream_events(parts, reasoning_format=reasoning_format)
    return [e for e in events if isinstance(e, StreamReasoningEvent)]


def test_separated_stream_emits_reasoning_chunks() -> None:
    reasoning = _reasoning_chunks(["<think>plan</think>answer"])

    assert "".join(e.chunk for e in reasoning) == "plan"
    assert [e.is_final for e in reasoning] == [False, True]
    assert {tuple(e.selector) for e in reasoning} == {("llm", "reasoning_content")}


def test_separated_stream_preserves_reasoning_text_order_within_delta() -> None:
    events = [
        event
        for event in _collect_stream_events(
            ["<think>plan</think>answer"],
            reasoning_format="separated",
        )
        if isinstance(event, StreamChunkEvent | StreamReasoningEvent)
    ]

    assert [(type(event), event.chunk, event.is_final) for event in events] == [
        (StreamReasoningEvent, "plan", False),
        (StreamChunkEvent, "answer", False),
        (StreamReasoningEvent, "", True),
    ]


def test_separated_stream_reasoning_split_across_chunks() -> None:
    reasoning = _reasoning_chunks(["<thi", "nk>plan</thi", "nk>ans", "wer"])

    assert "".join(e.chunk for e in reasoning) == "plan"
    assert reasoning[-1].is_final
    assert sum(e.is_final for e in reasoning) == 1


def test_separated_stream_emits_single_terminal_reasoning_marker() -> None:
    reasoning = _reasoning_chunks(["<think>a</think>X<think>b</think>Y"])

    # Live stream concatenates the blocks without split_reasoning()'s "\n" join.
    assert "".join(e.chunk for e in reasoning) == "ab"
    finals = [i for i, e in enumerate(reasoning) if e.is_final]
    assert finals == [len(reasoning) - 1]


def test_separated_stream_truncated_reasoning_marks_final() -> None:
    reasoning = _reasoning_chunks(["hi<think>tail"])

    assert "".join(e.chunk for e in reasoning) == "tail"
    assert reasoning[-1].is_final
    assert sum(e.is_final for e in reasoning) == 1


def test_separated_stream_flushes_held_partial_close_as_final_reasoning() -> None:
    # Stream ends mid "</thi": the residual is flushed as the final reasoning event.
    reasoning = _reasoning_chunks(["<think>ab</thi"])

    assert "".join(e.chunk for e in reasoning) == "ab</thi"
    assert reasoning[-1].is_final
    assert reasoning[-1].chunk == "</thi"
    assert sum(e.is_final for e in reasoning) == 1


def test_tagged_stream_emits_no_reasoning_events() -> None:
    reasoning = _reasoning_chunks(
        ["<think>plan</think>answer"],
        reasoning_format="tagged",
    )

    assert reasoning == []


def test_separated_stream_without_think_emits_no_reasoning_events() -> None:
    reasoning = _reasoning_chunks(["plain ", "answer"])

    assert reasoning == []


def test_reasoning_event_dispatches_to_engine_event() -> None:
    node = _build_llm_node()
    reasoning_event = StreamReasoningEvent(
        selector=["other", "reasoning_content"],
        chunk="thinking",
        is_final=True,
    )

    engine_event = node._dispatch(reasoning_event)

    assert isinstance(engine_event, NodeRunReasoningChunkEvent)
    assert engine_event.node_id == "llm"
    assert engine_event.selector == ["llm", "reasoning_content"]
    assert engine_event.chunk == "thinking"
    assert engine_event.is_final is True


def test_run_forwards_streaming_reasoning_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Regression: _yield_run_completion must forward StreamReasoningEvent through
    # the full _run() path. Without it the separated-mode reasoning the streaming
    # invoke emits is silently dropped before reaching the graph layer, leaving the
    # chatflow "thinking" panel dark even though the producer/dispatch sides work.
    node = _build_llm_node()
    _stub_simple_prompt(monkeypatch, node)
    monkeypatch.setattr(
        "graphon.nodes.llm.node.LLMNode.invoke_llm",
        lambda **_: LLMNode.handle_invoke_result(
            invoke_result=_stream_results(_stream_chunk("<think>plan</think>answer")),
            file_saver=MagicMock(),
            file_outputs=[],
            node_id="llm",
            model_instance=cast(
                LLMProtocol,
                MagicMock(is_structured_output_parse_error=lambda _error: False),
            ),
            reasoning_format="separated",
        ),
    )

    events = list(node._run())

    reasoning = [e for e in events if isinstance(e, StreamReasoningEvent)]
    assert "".join(e.chunk for e in reasoning) == "plan"
    assert {tuple(e.selector) for e in reasoning} == {("llm", "reasoning_content")}
    assert sum(e.is_final for e in reasoning) == 1
    assert reasoning[-1].is_final

    completed = next(e for e in events if isinstance(e, StreamCompletedEvent))
    assert completed.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    assert completed.node_run_result.outputs["text"] == "answer"
    assert completed.node_run_result.outputs["reasoning_content"] == "plan"


def _normalize_like_split_reasoning(text: str) -> str:
    # Mirror split_reasoning()'s whitespace handling: collapse blank-line runs + strip.
    return re.sub(r"\n\s*\n", "\n\n", text).strip()


@pytest.mark.parametrize(
    "full_text",
    [
        "<think>plan</think>answer",
        "before<think>a</think>middle<think>b</think>after",
        "<think>only reasoning</think>\n\nanswer body",
        "before<think>a</think>\n\n\nmiddle",  # internal blank-line run
        "<think>r</think>answer\n\n",  # trailing whitespace
    ],
)
def test_separated_stream_matches_split_reasoning(full_text: str) -> None:
    # Stream and batch splitter agree only up to split_reasoning()'s whitespace pass.
    parts = [full_text[i : i + 3] for i in range(0, len(full_text), 3)]
    chunks, _ = _run_stream(parts, reasoning_format="separated")
    expected, _ = split_reasoning(full_text, "separated")

    assert _normalize_like_split_reasoning("".join(chunks)) == expected


@pytest.fixture
def text_model() -> MagicMock:
    model = MagicMock(
        spec=LLMProtocol, provider="openai", model_name="gpt-4o", parameters={}, stop=()
    )
    model.get_model_schema.return_value = _PollingLLM([]).get_model_schema()
    return model


def _build_llm_answer_engine(
    llm: LLMNode,
    template: str,
    *,
    replacement: str | None = None,
    approval: HITLCallback | None = None,
    connect_llm_to_answer: bool = False,
) -> Engine:
    args = {"init_params": build_init_params(), "runtime_state": llm.runtime_state}
    start = StartNode(node_id="start", data=StartNodeData(title="Start"), **args)
    graph = Graph.new().add_root(start).add_node(llm)
    if approval is not None:
        graph.add_node(
            HumanInputNode(
                node_id="human",
                data=HumanInputNode.validate_node_data({
                    "type": "human-input",
                    "title": "Approve",
                }),
                hitl_callback=approval,
                **args,
            )
        )
    if replacement is not None:
        edit = VariableOperationItem(
            variable_selector=["llm", "text"],
            input_type=InputType.CONSTANT,
            operation=Operation.OVER_WRITE,
            value=replacement,
        )
        graph.add_node(
            VariableAssignerNode(
                node_id="edit",
                data=VariableAssignerNodeData(title="Edit", items=[edit]),
                **args,
            )
        )
    graph.add_node(
        AnswerNode(node_id="answer", data=AnswerNodeData(answer=template), **args)
    )
    if connect_llm_to_answer:
        graph.connect(tail="llm", head="answer")
    return Engine(graph=graph.build(), runtime_state=llm.runtime_state, workers=1)


def _select_stream_chunks(
    events: Iterable[EngineEvent],
) -> list[NodeRunStreamChunkEvent]:
    return [event for event in events if isinstance(event, NodeRunStreamChunkEvent)]


def _restore_filter_after_first_llm_chunk(
    engine: Engine,
) -> tuple[list[NodeRunStreamChunkEvent], list[NodeRunStreamChunkEvent]]:
    context = EngineEventFilterContext.from_engine(engine)
    events = list(engine.run())
    baseline = next(event for event in events if isinstance(event, FrameStartedEvent))
    first_chunk_index = next(
        i
        for i, event in enumerate(events)
        if isinstance(event, NodeRunStreamChunkEvent)
        and event.selector == ["llm", "text"]
    )
    event_filter = ResponseStreamFilter()
    event_filter.initialize(context)
    before = _select_stream_chunks(
        filtered_event
        for event in events[: first_chunk_index + 1]
        for filtered_event in event_filter.on_event(event)
    )
    restored = ResponseStreamFilter()
    restored.loads(event_filter.dumps())
    resumed = baseline.model_copy(update={"reason": WorkflowStartReason.RESUMPTION})
    after = filter_engine_events(
        [resumed, *events[first_chunk_index + 1 :]], context=context, filters=[restored]
    )
    return before, _select_stream_chunks(after)


@pytest.mark.parametrize("text", ["done", "", " \n "])
def test_complete_polling_response_keeps_retry_index_after_restore(text: str) -> None:
    model = _PollingLLM([
        LLMPollingResult(status=LLMPollingStatus.FAILED, error="try again"),
        LLMPollingResult(status=LLMPollingStatus.SUCCEEDED, result=_llm_result(text)),
    ])
    llm = _build_llm_node(model_instance=model)
    llm.node_data.retry_config = RetryConfig(
        retry_enabled=True, max_retries=1, retry_interval=0
    )
    engine = _build_llm_answer_engine(llm, "Reply: {{#llm.text#}}/{{#llm.text#}}")

    before, after = _restore_filter_after_first_llm_chunk(engine)

    assert [chunk.chunk for chunk in before] == ["Reply: "]
    assert "".join(chunk.chunk for chunk in after) == f"{text}/{text}"
    assert [
        (chunk.chunk, chunk.retry_index, chunk.is_final)
        for chunk in after
        if chunk.selector == ["llm", "text"]
    ] == [(text, 1, False), (text, 1, True)]


@pytest.mark.parametrize("text", ["Hello", ""])
def test_answer_streams_llm_text_across_restore(
    text_model: MagicMock, text: str
) -> None:
    text_model.invoke_llm.return_value = _stream_results(_stream_chunk(text))
    engine = _build_llm_answer_engine(
        _build_llm_node(model_instance=text_model), "Reply: {{#llm.text#}}!"
    )

    before, after = _restore_filter_after_first_llm_chunk(engine)

    assert [chunk.chunk for chunk in before] == ["Reply: ", text]
    assert [
        (chunk.chunk, chunk.is_final)
        for chunk in after
        if chunk.selector == ["llm", "text"]
    ] == [("", True)]
    assert "".join(chunk.chunk for chunk in after) == "!"
    assert after[-1].is_final


def test_answer_reads_reassigned_llm_text(text_model: MagicMock) -> None:
    # Removing the inner reasoning block exposes the outer tag.
    text_model.invoke_llm.return_value = _llm_result(
        "<th<think>note</think>ink>private</think>draft"
    )
    llm = _build_llm_node(model_instance=text_model)
    llm.node_data.reasoning_format = "separated"
    engine = _build_llm_answer_engine(
        llm, "{{#llm.text#}}/{{#llm.text#}}", replacement="edited"
    )
    context = EngineEventFilterContext.from_engine(engine)
    raw = list(engine.run())

    assert [(chunk.chunk, chunk.is_final) for chunk in _select_stream_chunks(raw)] == [
        ("draft", True)
    ]
    filtered = filter_engine_events(
        raw, context=context, filters=[ResponseStreamFilter()]
    )
    assert (
        "".join(chunk.chunk for chunk in _select_stream_chunks(filtered))
        == "edited/edited"
    )


@pytest.mark.parametrize(
    ("failed_text", "retry", "can_stream", "expected"),
    [
        ("", _stream_chunk(None), False, "edited/edited"),
        (" \n ", _stream_chunk(None), True, " \n /"),
        ("", _stream_chunk(""), False, "/"),
        ("hel", _llm_result("hello"), True, "hel/"),
        ("hel", _llm_result("hello"), False, "edited/edited"),
        ("first", _stream_chunk("next"), False, "firstnext/"),
    ],
    ids=[
        "empty-failed-attempt",
        "visible-whitespace",
        "empty-successful-attempt",
        "visible-before-whole-retry",
        "gated-before-whole-retry",
        "gated-before-token-retry",
    ],
)
def test_answer_stream_after_a_failed_attempt(
    text_model: MagicMock,
    failed_text: str,
    retry: LLMResultChunk | LLMResult,
    can_stream: bool,
    expected: str,
) -> None:
    def failed_stream() -> Generator[LLMResultChunk, None, None]:
        yield _stream_chunk(failed_text)
        yield _stream_chunk("")
        msg = "retry after text delta"
        raise ValueError(msg)

    text_model.invoke_llm.side_effect = [
        failed_stream(),
        _stream_results(retry) if isinstance(retry, LLMResultChunk) else retry,
    ]
    llm = _build_llm_node(model_instance=text_model)
    llm.node_data.retry_config = RetryConfig(
        retry_enabled=True, max_retries=1, retry_interval=0
    )
    engine = _build_llm_answer_engine(
        llm,
        "{{#llm.text#}}/{{#llm.text#}}",
        replacement="edited",
        connect_llm_to_answer=can_stream,
    )
    context = EngineEventFilterContext.from_engine(engine)
    filtered = list(
        filter_engine_events(
            engine.run(), context=context, filters=[ResponseStreamFilter()]
        )
    )
    final = next(
        event
        for event in filtered
        if isinstance(event, (GraphRunSucceededEvent, GraphRunPartialSucceededEvent))
    )
    assert final.outputs["answer"] == "edited/edited"
    retry_event_index = next(
        i for i, e in enumerate(filtered) if isinstance(e, NodeRunRetryEvent)
    )
    before = _select_stream_chunks(filtered[:retry_event_index])
    assert "".join(chunk.chunk for chunk in before) == (
        failed_text if can_stream else ""
    )
    chunks = _select_stream_chunks(filtered)
    assert "".join(chunk.chunk for chunk in chunks) == expected
    if can_stream:
        assert [
            (chunk.chunk, chunk.retry_index)
            for chunk in chunks
            if chunk.is_final and chunk.selector == ["llm", "text"]
        ] == [("", 1)]


def test_complete_result_waits_for_approval_across_pause(text_model: MagicMock) -> None:
    text_model.invoke_llm.return_value = _llm_result("approved text")
    template = "{{#llm.text#}}/{{#llm.text#}}"
    engine = _build_llm_answer_engine(
        _build_llm_node(model_instance=text_model),
        template,
        connect_llm_to_answer=True,
        approval=lambda _: PauseRequested(session_id="approve"),
    )
    stream = ResponseStreamFilter()
    events = list(
        filter_engine_events(
            engine.run(),
            filters=[stream],
            context=EngineEventFilterContext.from_engine(engine),
        )
    )
    assert isinstance(events[-1], GraphRunPausedEvent)
    assert not _select_stream_chunks(events)

    restored = ResponseStreamFilter()
    restored.loads(stream.dumps())
    llm = _build_llm_node(
        model_instance=text_model,
        runtime_state=RuntimeState.from_snapshot(engine.runtime_state.dumps()),
    )
    resumed = _build_llm_answer_engine(
        llm,
        template,
        connect_llm_to_answer=True,
        approval=lambda _: Completed(selected_handle="source", inputs={}, outputs={}),
    )
    restored.initialize(EngineEventFilterContext.from_engine(resumed))
    chunks = []
    for event in resumed.run():
        if isinstance(event, NodeRunStartedEvent) and event.node_id == "answer":
            assert not chunks
        chunks.extend(_select_stream_chunks(restored.on_event(event)))
    assert "".join(c.chunk for c in chunks) == "approved text/approved text"
    text_model.invoke_llm.assert_called_once()


@pytest.mark.parametrize(
    "checkpoint_id",
    ["803dcac", "eb75e38", "eb75e38-consumed"],
    ids=["v2", "v3", "v3-after-prefix"],
)
def test_resume_old_llm_final_marker(checkpoint_id: str, text_model: MagicMock) -> None:
    # Both revisions paused after the third polling attempt, before the edit.
    path = Path(__file__).parents[2] / "engine" / "fixtures" / "llm_answer_pauses.json"
    checkpoint = json.loads(path.read_text())[checkpoint_id]
    restored = ResponseStreamFilter()
    restored.loads(checkpoint["filter"])
    llm = _build_llm_node(
        model_instance=text_model,
        runtime_state=RuntimeState.from_snapshot(checkpoint["runtime"]),
    )
    marker_consumed = checkpoint_id == "eb75e38-consumed"
    template = (
        "prefix{{#llm.text#}}|{{#human.answer#}}[{{#llm.text#}}]/{{#llm.reasoning_content#}}!"
        if marker_consumed
        else "{{#llm.text#}}/{{#llm.text#}}"
    )
    engine = _build_llm_answer_engine(
        llm,
        template,
        replacement="edited",
        connect_llm_to_answer=marker_consumed,
        approval=lambda _: Completed(
            selected_handle="source",
            inputs={},
            outputs={"answer": StringSegment(value="approved")},
        ),
    )
    events = list(
        filter_engine_events(
            engine.run(),
            filters=[restored],
            context=EngineEventFilterContext.from_engine(engine),
        )
    )
    chunks = _select_stream_chunks(events)
    assert "".join(c.chunk for c in chunks) == (
        "approved[]/why!" if marker_consumed else "edited/edited"
    )
    assert [c.retry_index for c in chunks if c.node_id == "llm"] == (
        [2] if marker_consumed else [2, 2]
    )
    text_model.invoke_llm.assert_not_called()


def test_retry_index_for_reasoning_without_a_text_reference() -> None:
    model = _PollingLLM([
        LLMPollingResult(status=LLMPollingStatus.FAILED, error="retry"),
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=_llm_result("<think>why</think>draft"),
        ),
    ])
    llm = _build_llm_node(model_instance=model)
    llm.node_data.reasoning_format = "separated"
    llm.node_data.retry_config = RetryConfig(
        retry_enabled=True, max_retries=1, retry_interval=0
    )
    engine = _build_llm_answer_engine(llm, "{{#llm.reasoning_content#}}")
    chunks = _select_stream_chunks(
        filter_engine_events(
            engine.run(),
            filters=[ResponseStreamFilter()],
            context=EngineEventFilterContext.from_engine(engine),
        )
    )
    assert [(c.chunk, c.retry_index) for c in chunks] == [("why", 1)]


def test_complete_result_waits_for_an_opaque_host_node(
    monkeypatch: pytest.MonkeyPatch, text_model: MagicMock
) -> None:
    text_model.invoke_llm.return_value = _llm_result("draft")
    engine = _build_llm_answer_engine(
        _build_llm_node(model_instance=text_model),
        "{{#llm.text#}}",
        replacement="edited",
        connect_llm_to_answer=True,
    )
    # The host writes through the engine but cannot describe that write in advance.
    writer = engine.graph.nodes["edit"]
    monkeypatch.setattr(writer, "node_type", "host-edit")
    monkeypatch.setattr(writer, "blocks_variable_output", lambda _selectors: False)
    events = filter_engine_events(
        engine.run(),
        filters=[ResponseStreamFilter()],
        context=EngineEventFilterContext.from_engine(engine),
    )
    assert "".join(c.chunk for c in _select_stream_chunks(events)) == "edited"


def test_answer_uses_current_llm_text_when_the_baseline_has_a_value(
    text_model: MagicMock,
) -> None:
    text_model.invoke_llm.return_value = _llm_result("current")
    llm = _build_llm_node(
        model_instance=text_model, variables=[(("llm", "text"), "previous")]
    )
    engine = _build_llm_answer_engine(llm, "{{#llm.text#}}/{{#llm.text#}}")
    events = filter_engine_events(
        engine.run(),
        filters=[ResponseStreamFilter()],
        context=EngineEventFilterContext.from_engine(engine),
    )
    assert "".join(c.chunk for c in _select_stream_chunks(events)) == "current/current"
