"""Errors while the engine schedules work or drains completed nodes."""

from asyncio import CancelledError
from dataclasses import dataclass
from threading import Event
from typing import override
from unittest.mock import Mock

import pytest

from graphon.dsl import loads
from graphon.engine import Engine
from graphon.engine.layer import Layer
from graphon.engine_events import (
    EngineEvent,
    FrameFinishedEvent,
    FrameStartedEvent,
    GraphRunAbortedEvent,
    GraphRunFailedEvent,
    GraphRunPausedEvent,
    GraphRunStartedEvent,
    NodeEvent,
    NodeRunFailedEvent,
    NodeRunSucceededEvent,
)
from graphon.entities.pause_reason import SchedulingPause
from graphon.enums import WorkflowNodeExecutionStatus
from graphon.node_events import NodeRunResult
from graphon.nodes.base.node import Node
from graphon.runtime.ready_queue import ReadyTask, StartTask
from tests.engine.test_frame_observations import _dsl, _node


@dataclass(frozen=True)
class FrozenQueueError(Exception):
    reason: str


class QueueErrorGroup(BaseExceptionGroup[BaseException]):
    pass


@pytest.fixture
def engine() -> Engine:
    return loads(
        _dsl([_node("answer", "answer", answer="ok")], [("start", "answer")]), workers=1
    )


def _reject_node_start(
    monkeypatch: pytest.MonkeyPatch, engine: Engine, node_id: str, error: BaseException
) -> None:
    queue = engine.runtime_state.ready_queue
    put = queue.put

    def reject(task: ReadyTask) -> None:
        if isinstance(task, StartTask) and task.node_id == node_id:
            raise error
        put(task)

    monkeypatch.setattr(queue, "put", reject)


@pytest.mark.parametrize(
    ("phase", "cause"),
    [
        ("startup", CancelledError("startup cancelled")),
        ("dispatch", CancelledError("queue cancelled")),
        ("resume", CancelledError("resume cancelled")),
        ("dispatch", FrozenQueueError("queue closed")),
        ("startup", ExceptionGroup("writes", [OSError("queue closed")])),
        ("dispatch", QueueErrorGroup("writes", [OSError("queue closed")])),
        (
            "dispatch",
            BaseExceptionGroup(
                "writes",
                [
                    OSError("queue closed"),
                    BaseExceptionGroup("stopped", [CancelledError()]),
                ],
            ),
        ),
    ],
    ids=[
        "startup",
        "dispatch",
        "resume",
        "frozen",
        "exception-group",
        "custom-group",
        "mixed-group",
    ],
)
def test_queue_failure_reaches_caller(
    monkeypatch: pytest.MonkeyPatch, engine: Engine, phase: str, cause: BaseException
) -> None:
    if phase == "resume":
        stream = engine.run()
        assert isinstance(next(stream), GraphRunStartedEvent)
        engine.runtime_state.graph_execution.pause(
            SchedulingPause(message="pause before start")
        )
        assert isinstance(list(stream)[-1], GraphRunPausedEvent)
    _reject_node_start(
        monkeypatch, engine, "answer" if phase == "dispatch" else "start", cause
    )
    graph_end_errors: list[Exception | None] = []

    class GraphEndErrorRecorder(Layer):
        @override
        def on_graph_end(self, error: Exception | None) -> None:
            graph_end_errors.append(error)

    engine.add_layer(GraphEndErrorRecorder())
    events: list[EngineEvent] = []
    with pytest.raises(
        type(cause) if isinstance(cause, Exception) else RuntimeError
    ) as raised:
        events.extend(engine.run())

    terminal = events[-1]
    assert isinstance(terminal, GraphRunFailedEvent)
    assert terminal.error == str(raised.value)
    assert terminal.exceptions_count == 0
    assert engine.runtime_state.graph_execution.error is raised.value
    assert graph_end_errors == [raised.value]
    if isinstance(cause, Exception):
        assert raised.value is cause
    else:
        assert raised.value.__cause__ is cause


@pytest.mark.parametrize("node_id", ["start", "answer"])
def test_queue_interrupt_reaches_caller(
    monkeypatch: pytest.MonkeyPatch, engine: Engine, node_id: str
) -> None:
    interrupt = BaseExceptionGroup(
        "stopped",
        [CancelledError(), BaseExceptionGroup("signal", [KeyboardInterrupt()])],
    )
    _reject_node_start(monkeypatch, engine, node_id, interrupt)
    events: list[EngineEvent] = []
    with pytest.raises(BaseExceptionGroup) as raised:
        events.extend(engine.run())

    assert raised.value is interrupt
    assert not engine.runtime_state.graph_execution.completed
    assert not any(isinstance(event, GraphRunFailedEvent) for event in events)


def test_consumer_cancellation_propagates_from_the_yield(engine: Engine) -> None:
    stream = engine.run()
    next(event for event in stream if isinstance(event, FrameStartedEvent))
    cancelled = CancelledError("consumer disconnected")
    with pytest.raises(CancelledError) as raised:
        stream.throw(cancelled)
    assert raised.value is cancelled


@pytest.mark.parametrize(
    "abort", [False, True], ids=["fatal-node-then-queue-error", "abort-then-node-error"]
)
def test_draining_results_preserves_the_initial_failure_or_abort_reason(
    monkeypatch: pytest.MonkeyPatch, abort: bool
) -> None:
    nodes = [
        _node(node_id, "answer", answer="ok") for node_id in ("first", "later", "end")
    ]
    engine = loads(
        _dsl(nodes, [("start", "first"), ("start", "later"), ("later", "end")]),
        workers=2,
    )
    release_later = Event()
    later_result_queued = Event()

    class WaitForLaterResult(Layer):
        @override
        def on_event(self, event: EngineEvent) -> None:
            if (
                isinstance(event, (NodeRunFailedEvent, NodeRunSucceededEvent))
                and event.node_id == "first"
            ):
                if abort:
                    engine.request_abort("operator stopped")
                release_later.set()
                later_result_queued.wait(5)

        @override
        def on_node_run_end(
            self,
            node: Node,
            error: Exception | None,
            result_event: NodeEvent | None = None,
        ) -> None:
            if node.id == "later":
                later_result_queued.set()

    engine.add_layer(WaitForLaterResult())
    if not abort:
        monkeypatch.setattr(
            engine.graph.nodes["first"],
            "_run",
            Mock(side_effect=ValueError("first node failed")),
        )

    def run_later() -> NodeRunResult:
        assert release_later.wait(5)
        if abort:
            message = "later node failed"
            raise ValueError(message)
        return NodeRunResult(status=WorkflowNodeExecutionStatus.SUCCEEDED)

    monkeypatch.setattr(engine.graph.nodes["later"], "_run", run_later)
    queue_error = OSError("queue closed while draining")
    _reject_node_start(monkeypatch, engine, "end", queue_error)
    events: list[EngineEvent] = []
    if abort:
        events.extend(engine.run())
        terminal = events[-1]
        assert isinstance(terminal, GraphRunAbortedEvent)
        assert terminal.reason == "Aborted: operator stopped"
        assert any(
            isinstance(event, NodeRunFailedEvent) and event.node_id == "later"
            for event in events
        )
        reason = terminal.reason
    else:
        with pytest.raises(RuntimeError, match="first node failed") as raised:
            events.extend(engine.run())
        terminal = events[-1]
        assert isinstance(terminal, GraphRunFailedEvent)
        assert engine.runtime_state.graph_execution.error is raised.value
        assert queue_error.__traceback__ is not None
        reason = terminal.error

    finish = next(
        event
        for event in events
        if isinstance(event, FrameFinishedEvent) and event.frame_id == "root"
    )
    assert finish.outcome == ("aborted" if abort else "failed")
    assert finish.error == reason
    assert str(engine.runtime_state.graph_execution.error) == reason
