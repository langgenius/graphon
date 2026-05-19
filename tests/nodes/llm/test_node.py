from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Never, cast
from unittest.mock import MagicMock

import pytest

from graphon.enums import WorkflowNodeExecutionStatus
from graphon.graph_events.node import NodeRunModelPollingProgressEvent
from graphon.model_runtime.entities.llm_entities import (
    LLMPollingConfig,
    LLMPollingResult,
    LLMPollingStatus,
    LLMResult,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import AssistantPromptMessage
from graphon.model_runtime.entities.model_entities import ModelFeature
from graphon.node_events.node import (
    ModelInvokeCompletedEvent,
    ModelPollingProgressEvent,
    StreamCompletedEvent,
)
from graphon.nodes.llm import LLMNode, LLMNodeData
from graphon.nodes.llm.runtime_protocols import LLMProtocol
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from ...helpers import build_graph_init_params, build_variable_pool


class _PollingLLM:
    provider = "openai"
    model_name = "gpt-4o"
    stop = ()
    supports_polling = True

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

    def start_llm_polling(self, **kwargs: Any) -> object:
        self.start_calls.append(kwargs)
        return self._responses.pop(0)

    def check_llm_polling(self, **kwargs: Any) -> object:
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


class _SchemaPollingLLM(_PollingLLM):
    supports_polling = None

    def get_model_schema(self) -> SimpleNamespace:
        return SimpleNamespace(features=[ModelFeature.POLLING])


def _dynamic_supports_polling(*_: object) -> bool:
    return True


def _llm_result(text: str = "final answer") -> LLMResult:
    return LLMResult(
        model="gpt-4o",
        prompt_messages=[],
        message=AssistantPromptMessage(content=text),
        usage=LLMUsage.empty_usage(),
    )


def _build_llm_node(
    *,
    model_instance: object | None = None,
    variables: Sequence[tuple[Sequence[str], Any]] = (),
    workflow_run_id: str | None = "wr-test",
    run_context: dict[str, Any] | None = None,
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
        llm_file_saver=MagicMock(),
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
    assert model.start_calls[0]["workflow_run_id"] == "wr-1"
    assert model.start_calls[0]["node_id"] == "llm"
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
            "workflow_run_id": "wr-test",
            "node_id": "llm",
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


def test_polling_llm_requires_workflow_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _PollingLLM([
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=_llm_result(),
        ),
    ])
    node = _build_llm_node(model_instance=model, workflow_run_id=None)
    _stub_simple_prompt(monkeypatch, node)

    events = list(node._run())

    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert model.start_calls == []
    assert completed_event.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
    assert "workflow_run_id" in completed_event.node_run_result.error


def test_polling_llm_uses_run_context_workflow_run_id(
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
    assert model.start_calls[0]["workflow_run_id"] == "wr-context"


def test_polling_llm_can_use_model_schema_feature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _SchemaPollingLLM([
        LLMPollingResult(
            status=LLMPollingStatus.SUCCEEDED,
            result=_llm_result("feature-enabled"),
        ),
    ])
    node = _build_llm_node(model_instance=model)
    _stub_simple_prompt(monkeypatch, node)

    events = list(node._run())

    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert (
        completed_event.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    )
    assert completed_event.node_run_result.outputs["text"] == "feature-enabled"
    assert model.start_calls[0]["workflow_run_id"] == "wr-test"


@pytest.mark.parametrize(
    "supports_polling",
    [False, "true", _dynamic_supports_polling],
)
def test_polling_llm_requires_graph_bool_support_flag(
    supports_polling: object,
) -> None:
    model = _PollingLLM([])
    cast(Any, model).supports_polling = supports_polling
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
