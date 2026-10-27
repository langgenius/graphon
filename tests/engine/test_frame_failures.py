"""Frame failures survive restoration and consumer exceptions."""

from itertools import chain
from threading import Event

import pytest

from graphon.engine import Engine
from graphon.engine.frame import ExecutionFrame, FrameRegistry
from graphon.engine.layer import Layer
from graphon.engine_events import (
    EngineEvent,
    FrameFinishedEvent,
    FrameStartedEvent,
    GraphRunFailedEvent,
    GraphRunPausedEvent,
    NodeEvent,
    NodeRunFailedEvent,
    NodeRunStartedEvent,
)
from graphon.enums import BuiltinNodeTypes, WorkflowNodeExecutionStatus
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
    ContainerExecutionResult,
    ContainerNodeRunResult,
    LoopFrameRequest,
)
from graphon.nodes.human_input.entities import Completed, HITLContext, PauseRequested
from graphon.runtime.container_state import (
    ContainerFrameState,
    CustomContainerFrameState,
)
from graphon.runtime.ready_queue import ResumeTask
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool
from tests.engine.test_frame_observations import _dsl, _node
from tests.engine.test_runtime_state_serialization import (
    _complete_iteration_hitl,
    _complete_loop_hitl,
    _hitl_engine,
    _iteration_dsl,
    _new_runtime_state,
)


class _CustomLoopHandler:
    node_type = BuiltinNodeTypes.LOOP

    def __init__(self, registry: FrameRegistry) -> None:
        self.registry = registry

    def restore_frame(self, frame_state: ContainerFrameState) -> None:
        root = self.registry["root"].state
        run = root.get_container_run(frame_state.parent_invocation_id)
        pool = frame_state.runtime_data.variable_pool
        assert isinstance(pool, VariablePool)
        self.registry.restore_child(
            frame_id=frame_state.frame_id,
            parent_frame_id=run.frame_id,
            container_id=run.node_id,
            root_node_id="loop-start",
            runtime_data=frame_state.runtime_data,
            variable_pool=pool.model_copy(deep=True),
        )

    def handle_request(
        self, *, invocation_id: str, request: ContainerAwaitRequest
    ) -> None:
        assert isinstance(request, LoopFrameRequest)
        root = self.registry["root"].state
        run = root.get_container_run(invocation_id)
        frame = self.registry.create_child(
            frame_id="custom-child",
            parent_frame_id=run.frame_id,
            container_id=run.node_id,
            root_node_id=request.root_node_id,
            variable_pool=self.registry[run.frame_id].state.variable_pool.model_copy(
                deep=True
            ),
        )
        root.put_container_frame(
            CustomContainerFrameState(
                frame_id=frame.frame_id,
                parent_invocation_id=invocation_id,
                runtime_data=frame.state.snapshot_frame(),
            )
        )
        frame.scheduler.enqueue_node(request.root_node_id)

    def prepare_frame_event(self, *, frame: ExecutionFrame, event: NodeEvent) -> None:
        pass

    def should_emit(self, *, event: NodeEvent) -> bool:
        _ = event
        return True

    def record_frame_failure(
        self, *, frame: ExecutionFrame, event: NodeRunFailedEvent
    ) -> None:
        frame.state.variable_pool.add(("handler", "failure"), event.error)

    def complete_frame_if_ready(self, frame: ExecutionFrame) -> None:
        if not frame.scheduler.is_execution_complete():
            return
        root = self.registry["root"].state
        frame_state = root.get_container_frame(frame.frame_id)
        failure = frame.state.variable_pool.get(("handler", "failure"))
        assert failure is not None
        root.enqueue_ready_task(
            ResumeTask(
                invocation_id=frame_state.parent_invocation_id,
                result=ContainerExecutionResult(
                    metadata={},
                    steps=1,
                    node_run_result=ContainerNodeRunResult(
                        status=WorkflowNodeExecutionStatus.FAILED,
                        error=failure.text,
                    ),
                ),
            )
        )
        root.pop_container_frame(frame.frame_id)
        self.registry.remove(frame.frame_id)


@pytest.mark.parametrize("restore_snapshot", [False, True])
def test_custom_frame_failure_survives_pause(restore_snapshot: bool) -> None:
    document = _dsl(
        [
            _node(
                "loop",
                "loop",
                start_node_id="loop-start",
                loop_count=1,
                break_conditions=[],
                logical_operator="and",
            ),
            _node("loop-start", "loop-start", container_id="loop"),
            _node(
                "fail",
                "assigner",
                version="2",
                container_id="loop",
                items=[
                    {
                        "variable_selector": ["missing", "value"],
                        "input_type": "constant",
                        "operation": "+=",
                        "value": 1,
                    }
                ],
            ),
            _node("human-input", "human-input", container_id="loop"),
        ],
        [("start", "loop"), ("loop-start", "fail"), ("loop-start", "human-input")],
    )
    human_input_ready = Event()

    def complete_or_pause_human_input(
        context: HITLContext,
    ) -> Completed | PauseRequested:
        return (
            _complete_loop_hitl(context)
            if human_input_ready.is_set()
            else PauseRequested(session_id="pending")
        )

    human_input_engine = _hitl_engine(
        document,
        runtime_state=_new_runtime_state({}),
        callback=complete_or_pause_human_input,
    )
    engine = Engine(
        human_input_engine.graph,
        human_input_engine.runtime_state,
        workers=2,
        container_handler_factories=[_CustomLoopHandler],
    )
    pause_events = list(engine.run())
    assert isinstance(pause_events[-1], GraphRunPausedEvent)
    failure = next(
        event
        for event in pause_events
        if isinstance(event, NodeRunFailedEvent) and event.node_id == "fail"
    )
    assert not any(isinstance(event, FrameFinishedEvent) for event in pause_events)
    if restore_snapshot:
        human_input_engine = _hitl_engine(
            document,
            runtime_state=RuntimeState.from_snapshot(engine.runtime_state.dumps()),
            callback=complete_or_pause_human_input,
        )
        engine = Engine(
            human_input_engine.graph,
            human_input_engine.runtime_state,
            workers=2,
            container_handler_factories=[_CustomLoopHandler],
        )
    human_input_ready.set()
    events: list[EngineEvent] = []
    with pytest.raises(RuntimeError, match="missing"):
        events.extend(engine.run())
    frame_finished_events = [
        event for event in events if isinstance(event, FrameFinishedEvent)
    ]
    assert {event.frame_id for event in frame_finished_events} == {
        "root",
        failure.frame_id,
    }
    assert all(
        event.outcome == "failed" and event.error == failure.error
        for event in frame_finished_events
    )


@pytest.mark.parametrize("after_pause", [False, True])
def test_consumer_exception_finishes_open_frames(after_pause: bool) -> None:
    callback_started = Event()
    release_callback = Event()
    layer_events: list[EngineEvent] = []

    class EventCaptureLayer(Layer):
        def on_event(self, event: EngineEvent) -> None:
            layer_events.append(event)
            if isinstance(event, FrameFinishedEvent | GraphRunFailedEvent):
                release_callback.set()

    def complete_or_pause_human_input(
        context: HITLContext,
    ) -> Completed | PauseRequested:
        callback_started.set()
        if after_pause:
            return PauseRequested(session_id="pending")
        assert release_callback.wait(5)
        return _complete_iteration_hitl(context)

    engine = _hitl_engine(
        _iteration_dsl(),
        runtime_state=_new_runtime_state({"items": ["only"]}),
        callback=complete_or_pause_human_input,
    )
    engine.add_layer(EventCaptureLayer())
    stream = engine.run()
    events: list[EngineEvent] = []
    error = ValueError("consumer rejected the response")
    try:
        for event in stream:
            events.append(event)
            if (after_pause and isinstance(event, GraphRunPausedEvent)) or (
                not after_pause
                and isinstance(event, NodeRunStartedEvent)
                and event.node_id == "human-input"
            ):
                break
        assert callback_started.wait(3)
        with pytest.raises(ValueError, match="consumer rejected") as raised:
            events.extend(chain((stream.throw(error),), stream))
        assert raised.value is error
    finally:
        release_callback.set()
        stream.close()

    started_frame_ids = {
        event.frame_id for event in events if isinstance(event, FrameStartedEvent)
    }
    frame_finished_events = [
        event for event in events if isinstance(event, FrameFinishedEvent)
    ]
    assert len(started_frame_ids) == 2
    assert len(frame_finished_events) == len(started_frame_ids)
    assert {event.frame_id for event in frame_finished_events} == started_frame_ids
    assert all(
        event.outcome == "failed" and event.error == str(error)
        for event in frame_finished_events
    )
    graph_failed = next(
        event for event in events if isinstance(event, GraphRunFailedEvent)
    )
    assert all(
        events.index(event) < events.index(graph_failed)
        for event in frame_finished_events
    )
    assert [
        event
        for event in layer_events
        if isinstance(event, FrameFinishedEvent | GraphRunFailedEvent)
    ] == [
        event
        for event in events
        if isinstance(event, FrameFinishedEvent | GraphRunFailedEvent)
    ]
