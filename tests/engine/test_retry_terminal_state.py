"""Graph outcomes when a node retries."""

from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any, override
from unittest.mock import Mock

import pytest

from graphon.dsl import SandboxCodeExecutionError, SandboxCodeExecutor, loads
from graphon.engine import Engine
from graphon.engine.event.node_failure import NodeFailureHandler
from graphon.engine.frame import ExecutionFrame
from graphon.engine.layer import Layer
from graphon.engine_events import (
    EngineEvent,
    GraphRunFailedEvent,
    GraphRunPartialSucceededEvent,
    GraphRunPausedEvent,
    GraphRunSucceededEvent,
    NodeEvent,
    NodeRunExceptionEvent,
    NodeRunFailedEvent,
    NodeRunRetryEvent,
)
from graphon.entities.pause_reason import SchedulingPause
from graphon.enums import WorkflowNodeExecutionStatus
from graphon.node_events import NodeRunResult
from graphon.runtime.ready_queue import ReadyTask
from graphon.runtime.runtime_state import RuntimeState
from tests.engine.test_frame_failures import _CustomLoopHandler
from tests.engine.test_frame_observations import _dsl, _node
from tests.engine.test_runtime_state_serialization import _hitl_engine


@pytest.fixture
def execute_code(monkeypatch: pytest.MonkeyPatch) -> Mock:
    execute = Mock(side_effect=SandboxCodeExecutionError("code failed"))
    monkeypatch.setattr(SandboxCodeExecutor, "execute", execute)
    return execute


def _retrying_code_node(**data: Any) -> dict[str, Any]:
    return _node(
        "code",
        "code",
        variables=[],
        code_language="python3",
        code="def main():\n    return {}",
        outputs={},
        retry_config={"retry_enabled": True, "max_retries": 2, "retry_interval": 0},
        **data,
    )


def _load_engine_with_retries(*, error_strategy: str | None = None) -> Engine:
    code_node = _retrying_code_node(error_strategy=error_strategy)
    return loads(_dsl([code_node], [("start", "code")]), workers=1)


def _yield_event(event: NodeEvent) -> Generator[NodeEvent]:
    yield event


def test_graph_succeeds_on_the_last_node_retry(execute_code: Mock) -> None:
    execute_code.side_effect = [SandboxCodeExecutionError("code failed")] * 2 + [{}]
    engine = _load_engine_with_retries()

    events = list(engine.run())

    assert execute_code.call_count == 3
    assert isinstance(events[-1], GraphRunSucceededEvent)


def test_using_default_value_after_last_retry_counts_one_failure(
    execute_code: Mock,
) -> None:
    engine = _load_engine_with_retries(error_strategy="default-value")

    events = list(engine.run())

    assert execute_code.call_count == 3
    terminal_event = events[-1]
    assert isinstance(terminal_event, GraphRunPartialSucceededEvent)
    assert terminal_event.exceptions_count == 1


def test_node_that_emits_its_own_fallback_on_retry_counts_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _load_engine_with_retries(error_strategy="default-value")
    node = engine.graph.nodes["code"]
    fallback = NodeRunExceptionEvent(
        id="",
        node_id=node.id,
        node_type=node.node_type,
        start_at=datetime.now(UTC),
        error="provider offline",
        node_run_result=NodeRunResult(
            status=WorkflowNodeExecutionStatus.EXCEPTION,
            outputs={"value": "cached"},
        ),
    )
    run_node = Mock(side_effect=[OSError("provider offline"), _yield_event(fallback)])
    monkeypatch.setattr(node, "_run", run_node)

    terminal = list(engine.run())[-1]

    assert run_node.call_count == 2
    assert isinstance(terminal, GraphRunPartialSucceededEvent)
    assert terminal.exceptions_count == 1
    value = engine.runtime_state.variable_pool.get(("code", "value"))
    assert value is not None
    assert value.to_object() == "cached"


@pytest.mark.parametrize("node_emits_retry", [False, True], ids=["policy", "node"])
@pytest.mark.parametrize(
    "queue_rejects_retry", [False, True], ids=["accepted", "rejected"]
)
def test_retry_counts_a_failure_only_when_rejected(
    monkeypatch: pytest.MonkeyPatch, node_emits_retry: bool, queue_rejects_retry: bool
) -> None:
    engine = _load_engine_with_retries()
    node = engine.graph.nodes["code"]
    retry = NodeRunRetryEvent(
        id="",
        node_id=node.id,
        node_type=node.node_type,
        node_title=node.title,
        start_at=datetime.now(UTC),
        retry_index=1,
        error="provider offline",
        node_run_result=NodeRunResult(status=WorkflowNodeExecutionStatus.FAILED),
    )
    run_node = Mock(
        side_effect=[
            _yield_event(retry) if node_emits_retry else OSError("provider offline"),
            NodeRunResult(status=WorkflowNodeExecutionStatus.SUCCEEDED),
        ]
    )
    monkeypatch.setattr(node, "_run", run_node)
    failure_counts: list[tuple[str, int]] = []
    retry_counts: list[int] = []

    class RetryObserver(Layer):
        @override
        def on_event(self, event: EngineEvent) -> None:
            if isinstance(event, NodeRunRetryEvent):
                failure_counts.append(("retry", self.runtime_state.exceptions_count))
                execution = (
                    engine.runtime_state.graph_execution.get_or_create_node_execution(
                        frame_id=event.frame_id, node_id=event.node_id
                    )
                )
                retry_counts.append(execution.retry_count)

    engine.add_layer(RetryObserver())
    state = engine.runtime_state
    put = state.ready_queue.put
    error = OSError("ready queue unavailable")

    def put_task(task: ReadyTask) -> None:
        if run_node.called:
            failure_counts.append(("enqueue", state.graph_execution.exceptions_count))
            if queue_rejects_retry:
                raise error
        put(task)

    monkeypatch.setattr(state.ready_queue, "put", put_task)
    if queue_rejects_retry:
        assert _assert_graph_fails_with_one_node_failure(engine, OSError) is error
        assert run_node.call_count == 1
    else:
        assert isinstance(list(engine.run())[-1], GraphRunSucceededEvent)
        assert run_node.call_count == 2
    assert failure_counts == [("retry", 0), ("enqueue", 0)]
    assert retry_counts == [1]


def _assert_graph_fails_with_one_node_failure(
    engine: Engine, error_type: type[Exception]
) -> Exception:
    events: list[EngineEvent] = []
    with pytest.raises(error_type) as error:
        events.extend(engine.run())

    terminal_event = events[-1]
    assert isinstance(terminal_event, GraphRunFailedEvent)
    assert terminal_event.error == str(error.value)
    assert terminal_event.exceptions_count == 1
    assert engine.runtime_state.graph_execution.exceptions_count == 1
    assert engine.runtime_state.graph_execution.error is error.value
    return error.value


def test_exhausted_retries_without_a_fallback_count_one_failure(
    execute_code: Mock,
) -> None:
    _assert_graph_fails_with_one_node_failure(_load_engine_with_retries(), RuntimeError)
    assert execute_code.call_count == 3


def test_policy_error_counts_the_failed_attempt(
    monkeypatch: pytest.MonkeyPatch, execute_code: Mock
) -> None:
    engine = _load_engine_with_retries()
    error = OSError("retry unavailable")
    monkeypatch.setattr(NodeFailureHandler, "handle", Mock(side_effect=error))

    assert _assert_graph_fails_with_one_node_failure(engine, OSError) is error
    assert execute_code.call_count == 1


@pytest.mark.parametrize(
    "reject_at", ["deferral", "resume", None], ids=["deferral", "resume", "accepted"]
)
def test_paused_retry_counts_only_if_deferral_fails(
    monkeypatch: pytest.MonkeyPatch, execute_code: Mock, reject_at: str | None
) -> None:
    document = _dsl([_retrying_code_node()], [("start", "code")])
    engine = loads(document, workers=1)
    state = engine.runtime_state

    def pause_then_fail(**_kwargs: object) -> None:
        state.graph_execution.pause(SchedulingPause(message="pause before retry"))
        message = "code failed"
        raise SandboxCodeExecutionError(message)

    execute_code.side_effect = pause_then_fail
    if reject_at == "deferral":
        error = OSError("deferred queue unavailable")
        reject = Mock(side_effect=error)
        monkeypatch.setattr(state.deferred_ready_queue, "put", reject)
        assert _assert_graph_fails_with_one_node_failure(engine, OSError) is error
        reject.assert_called_once()
        return

    # A saved count can include a fallback or a retry counted by an older engine.
    state.graph_execution.record_node_failure()
    events = list(engine.run())
    assert isinstance(events[-1], GraphRunPausedEvent)
    assert any(isinstance(event, NodeRunRetryEvent) for event in events)
    restored = RuntimeState.from_snapshot(state.dumps())
    assert restored.graph_execution.exceptions_count == 1
    resumed = _hitl_engine(document, runtime_state=restored, callback=Mock())

    if reject_at == "resume":
        error = OSError("ready queue unavailable on resume")
        monkeypatch.setattr(restored.ready_queue, "put", Mock(side_effect=error))
        assert _assert_graph_fails_with_one_node_failure(resumed, OSError) is error
        assert execute_code.call_count == 1
    else:
        execute_code.side_effect = None
        execute_code.return_value = {}
        terminal = list(resumed.run())[-1]
        assert isinstance(terminal, GraphRunPartialSucceededEvent)
        assert terminal.exceptions_count == 1
        assert execute_code.call_count == 2


def test_container_rejecting_a_failed_attempt_counts_it_once(
    execute_code: Mock,
) -> None:
    error = OSError("container could not prepare the failure")

    class LoopHandlerRejectingFailedRetry(_CustomLoopHandler):
        @override
        def prepare_frame_event(
            self, *, frame: ExecutionFrame, event: NodeEvent
        ) -> None:
            if isinstance(event, NodeRunFailedEvent) and event.retry_index == 1:
                raise error

    loop = _node(
        "loop",
        "loop",
        start_node_id="loop-start",
        loop_count=1,
        break_conditions=[],
        logical_operator="and",
    )
    nodes = [
        loop,
        _node("loop-start", "loop-start", container_id="loop"),
        _retrying_code_node(container_id="loop"),
    ]
    engine = loads(
        _dsl(nodes, [("start", "loop"), ("loop-start", "code")]),
        workers=1,
        container_handler_factories=[LoopHandlerRejectingFailedRetry],
    )

    assert _assert_graph_fails_with_one_node_failure(engine, OSError) is error
    assert execute_code.call_count == 2
