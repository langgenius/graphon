import base64
import re
from collections.abc import Generator, Sequence
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Literal, Never, cast
from unittest.mock import MagicMock

import pytest

from graphon.enums import WorkflowNodeExecutionStatus
from graphon.file import helpers as file_helpers
from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.graph_events.node import (
    NodeRunModelPollingProgressEvent,
    NodeRunReasoningChunkEvent,
)
from graphon.model_runtime.entities.llm_entities import (
    LLMPollingConfig,
    LLMPollingResult,
    LLMPollingStatus,
    LLMResult,
    LLMResultChunk,
    LLMResultChunkDelta,
    LLMResultChunkWithStructuredOutput,
    LLMStructuredOutput,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    PromptMessageContentUnionTypes,
    VideoPromptMessageContent,
)
from graphon.model_runtime.entities.model_entities import ModelFeature
from graphon.node_events.base import NodeEventBase
from graphon.node_events.node import (
    ModelInvokeCompletedEvent,
    ModelPollingProgressEvent,
    StreamChunkEvent,
    StreamCompletedEvent,
    StreamReasoningEvent,
)
from graphon.nodes.llm import LLMNode, LLMNodeData
from graphon.nodes.llm.exc import LLMNodeError
from graphon.nodes.llm.reasoning import split_reasoning
from graphon.nodes.llm.runtime_protocols import LLMPollingCapableProtocol, LLMProtocol
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from ...helpers import build_graph_init_params, build_variable_pool


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
    variables: Sequence[tuple[Sequence[str], Any]] = (),
    workflow_run_id: str | None = "wr-test",
    run_context: dict[str, Any] | None = None,
    llm_file_saver: Any | None = None,
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

    return LLMNode(
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
                    "text": "Hello",
                }
            ],
            "context": {"enabled": False},
        }),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
            run_context=run_context,
        ),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=build_variable_pool(variables=prepared_variables),
            start_at=0.0,
        ),
        model_instance=cast(LLMProtocol, prepared_model),
        llm_file_saver=MagicMock() if llm_file_saver is None else llm_file_saver,
        prompt_message_serializer=MagicMock(
            serialize=MagicMock(return_value=[]),
        ),
    )


def _stub_simple_prompt(monkeypatch: pytest.MonkeyPatch, node: LLMNode) -> None:
    monkeypatch.setattr(node, "_fetch_inputs", lambda **_: {})
    monkeypatch.setattr(node, "_fetch_jinja_inputs", lambda **_: {})
    monkeypatch.setattr(node, "_collect_run_context", lambda **_: iter(()))
    monkeypatch.setattr(
        LLMNode,
        "fetch_prompt_messages",
        staticmethod(lambda **_: ([], None)),
    )


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
    assert completed_event.node_run_result.outputs["files"].value == [saved_file]
    file_saver.save_remote_url.assert_called_once_with(
        "https://example.com/video.mp4",
        FileType.VIDEO,
    )


def test_invoke_llm_forwards_first_token_timeout() -> None:
    model = MagicMock(
        provider="openai",
        model_name="gpt-4o",
        parameters={},
        stop=(),
        is_structured_output_parse_error=lambda _error: False,
    )
    model.invoke_llm.return_value = _stream_results(_stream_chunk("hi"))

    list(
        LLMNode.invoke_llm(
            model_instance=cast(LLMProtocol, model),
            prompt_messages=[],
            structured_output_enabled=False,
            file_saver=MagicMock(),
            file_outputs=[],
            node_id="llm",
            first_token_timeout=1.5,
        ),
    )

    assert model.invoke_llm.call_args.kwargs["first_token_timeout"] == pytest.approx(
        1.5,
    )


def test_invoke_llm_defaults_first_token_timeout_to_none() -> None:
    model = MagicMock(
        provider="openai",
        model_name="gpt-4o",
        parameters={},
        stop=(),
        is_structured_output_parse_error=lambda _error: False,
    )
    model.invoke_llm.return_value = _stream_results(_stream_chunk("hi"))

    list(
        LLMNode.invoke_llm(
            model_instance=cast(LLMProtocol, model),
            prompt_messages=[],
            structured_output_enabled=False,
            file_saver=MagicMock(),
            file_outputs=[],
            node_id="llm",
        ),
    )

    assert model.invoke_llm.call_args.kwargs["first_token_timeout"] is None


def test_invoke_llm_structured_forwards_first_token_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = MagicMock(
        provider="openai",
        model_name="gpt-4o",
        parameters={},
        stop=(),
        is_structured_output_parse_error=lambda _error: False,
    )
    model.invoke_llm_with_structured_output.return_value = _stream_results(
        _stream_chunk("hi"),
    )
    monkeypatch.setattr(
        LLMNode,
        "fetch_structured_output_schema",
        staticmethod(lambda **_: {}),
    )

    list(
        LLMNode.invoke_llm(
            model_instance=cast(LLMProtocol, model),
            prompt_messages=[],
            structured_output_enabled=True,
            structured_output={"schema": {}},
            file_saver=MagicMock(),
            file_outputs=[],
            node_id="llm",
            first_token_timeout=2.0,
        ),
    )

    kwargs = model.invoke_llm_with_structured_output.call_args.kwargs
    assert kwargs["first_token_timeout"] == pytest.approx(2.0)


def test_run_forwards_first_token_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _build_llm_node()
    node.node_data.first_token_timeout = 5000
    _stub_simple_prompt(monkeypatch, node)

    captured: dict[str, Any] = {}

    def _capture(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return iter([
            ModelInvokeCompletedEvent(
                text="ok",
                usage=LLMUsage.empty_usage(),
                finish_reason="stop",
            ),
        ])

    monkeypatch.setattr("graphon.nodes.llm.node.LLMNode.invoke_llm", _capture)

    list(node._run())

    assert captured["first_token_timeout"] == pytest.approx(5.0)


def test_polling_llm_does_not_receive_first_token_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The polling path intentionally does not thread first_token_timeout:
    # start_llm_polling has no such parameter. Pin this so a future change that
    # forwards it there is flagged as out-of-scope for v1 rather than silent.
    model = _PollingLLM([
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=_llm_result("done"),
        ),
    ])
    node = _build_llm_node(model_instance=model)
    node.node_data.first_token_timeout = 5000
    _stub_simple_prompt(monkeypatch, node)

    list(node._run())

    assert model.start_calls
    assert "first_token_timeout" not in model.start_calls[0]


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
    node.graph_runtime_state.graph_execution.abort("stop")
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

    monkeypatch.setattr("graphon.nodes.llm.node.time.perf_counter", perf_counter)

    events = list(node._run())

    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert completed_event.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
    assert "timed out" in completed_event.node_run_result.error


def test_polling_progress_event_omits_next_check_when_deadline_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("graphon.nodes.llm.node.time.perf_counter", lambda: 10.0)

    event = LLMNode._build_polling_progress_event(
        attempt=0,
        delay_seconds=5,
        deadline=12,
    )

    assert event.next_check_at is None


def test_polling_progress_event_keeps_next_check_when_delay_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("graphon.nodes.llm.node.time.perf_counter", lambda: 10.0)

    event = LLMNode._build_polling_progress_event(
        attempt=0,
        delay_seconds=5,
        deadline=20,
    )

    assert event.next_check_at == event.last_checked_at + timedelta(seconds=5)


def test_polling_progress_event_dispatches_to_graph_event() -> None:
    node = _build_llm_node()
    progress_event = ModelPollingProgressEvent(
        attempt=2,
        last_checked_at=datetime(2026, 5, 19, tzinfo=UTC).replace(tzinfo=None),
        next_check_at=None,
    )

    graph_event = node._dispatch(progress_event)

    assert isinstance(graph_event, NodeRunModelPollingProgressEvent)
    assert graph_event.attempt == 2


def _collect_stream_events(
    parts: Sequence[str],
    *,
    reasoning_format: Literal["separated", "tagged"],
) -> list[NodeEventBase]:
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
        if isinstance(event, NodeEventBase)
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


def test_reasoning_event_dispatches_to_graph_event() -> None:
    node = _build_llm_node()
    reasoning_event = StreamReasoningEvent(
        selector=["other", "reasoning_content"],
        chunk="thinking",
        is_final=True,
    )

    graph_event = node._dispatch(reasoning_event)

    assert isinstance(graph_event, NodeRunReasoningChunkEvent)
    assert graph_event.node_id == "llm"
    assert graph_event.selector == ["llm", "reasoning_content"]
    assert graph_event.chunk == "thinking"
    assert graph_event.is_final is True


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
