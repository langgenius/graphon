from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC
from threading import Event, Thread
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
import yaml

from graphon.dsl import loads
from graphon.engine import Engine
from graphon.engine.container_handler import LoopContainerHandler
from graphon.engine.frame import FrameRegistry
from graphon.engine.layer import Layer
from graphon.engine.ready_queue.entities import StartTask
from graphon.engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.graph import (
    GraphRunFailedEvent,
    GraphRunPartialSucceededEvent,
)
from graphon.engine_events.iteration import (
    NodeRunIterationFailedEvent,
    NodeRunIterationNextEvent,
    NodeRunIterationSucceededEvent,
)
from graphon.engine_events.loop import (
    NodeRunLoopFailedEvent,
    NodeRunLoopSucceededEvent,
)
from graphon.engine_events.node import NodeRunSucceededEvent
from graphon.variables.segments import Segment
from tests.helpers.workflow_events import (
    event_path,
    fake_slim_llm,
    final_outputs,
    run_workflow,
)

_OPENAI_PLUGIN_ID = "langgenius/openai:0.3.8@test"
_ENGINE_TIMEOUT_SECONDS = 2


def _graph_dsl(
    *,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    dependencies: Sequence[dict[str, Any]] = (),
) -> str:
    return yaml.safe_dump({
        "kind": "graph",
        "dependencies": list(dependencies),
        "graph": {
            "nodes": nodes,
            "edges": edges,
        },
    })


def _start_node() -> dict[str, Any]:
    return {"id": "start", "data": {"type": "start", "variables": []}}


def _end_node(outputs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"id": "end", "data": {"type": "end", "outputs": outputs}}


def _edge(source: str, target: str) -> dict[str, str]:
    return {"source": source, "target": target}


def _iteration_dsl(*, is_parallel: bool, parallel_nums: int) -> str:
    return _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "iteration",
                "data": {
                    "type": "iteration",
                    "title": "For each item",
                    "iterator_selector": ["start", "items"],
                    "output_selector": ["render", "output"],
                    "start_node_id": "iteration-start",
                    "is_parallel": is_parallel,
                    "parallel_nums": parallel_nums,
                    "error_handle_mode": "terminated",
                    "flatten_output": True,
                },
            },
            {
                "id": "iteration-start",
                "data": {"type": "iteration-start", "iteration_id": "iteration"},
            },
            {
                "id": "render",
                "data": {
                    "type": "template-transform",
                    "iteration_id": "iteration",
                    "variables": [
                        {
                            "variable": "item",
                            "value_selector": ["iteration", "item"],
                        }
                    ],
                    "template": "{{ item }}!",
                },
            },
            _end_node([
                {
                    "variable": "items",
                    "value_selector": ["iteration", "output"],
                },
            ]),
        ],
        edges=[
            _edge("start", "iteration"),
            _edge("iteration-start", "render"),
            _edge("iteration", "end"),
        ],
    )


def _event(
    event_type: str,
    subject: str = "",
    container_id: str = "",
) -> tuple[str, str, str]:
    return event_type, subject, container_id


def _run_failed_workflow(
    dsl: str,
    *,
    start_inputs: Mapping[str, object],
) -> list[EngineEvent]:
    events: list[EngineEvent] = []
    engine = loads(dsl, start_inputs=start_inputs)
    with pytest.raises(RuntimeError, match="Variable"):
        events.extend(engine.run())
    return events


def _use_bounded_ready_queue(engine: Engine) -> InMemoryReadyQueue:
    ready_queue = InMemoryReadyQueue(maxsize=1)
    engine.graph_runtime_state._ready_queue = ready_queue
    engine._worker_pool._ready_queue = ready_queue
    return ready_queue


def _run_with_timeout(engine: Engine) -> list[EngineEvent]:
    events: list[EngineEvent] = []
    errors: list[Exception] = []
    finished = Event()

    def run() -> None:
        try:
            events.extend(engine.run())
        except Exception as error:  # ruff: ignore[blind-except] - re-raised in the test thread.
            errors.append(error)
        finally:
            finished.set()

    thread = Thread(target=run, daemon=True)
    thread.start()
    completed_in_time = finished.wait(timeout=_ENGINE_TIMEOUT_SECONDS)
    if not completed_in_time:
        engine.graph_runtime_state.ready_queue.drain()
        engine.request_abort("bounded ready queue test timed out")
        finished.wait(timeout=_ENGINE_TIMEOUT_SECONDS)
    thread.join(timeout=_ENGINE_TIMEOUT_SECONDS)

    assert completed_in_time, "graph execution blocked on the bounded ready queue"
    assert not thread.is_alive()
    if errors:
        raise errors[0]
    return events


def _failing_assigner(*, container_field: str, container_id: str) -> dict[str, Any]:
    return {
        "id": "fail",
        "data": {
            "type": "assigner",
            "version": "2",
            container_field: container_id,
            "items": [
                {
                    "variable_selector": ["missing", "value"],
                    "input_type": "constant",
                    "operation": "over-write",
                    "value": "unused",
                }
            ],
        },
    }


def _openai_dependency() -> dict[str, Any]:
    return {
        "type": "marketplace",
        "value": {"marketplace_plugin_unique_identifier": _OPENAI_PLUGIN_ID},
    }


def _openai_credentials() -> Mapping[str, Any]:
    return {
        "model_credentials": [
            {
                "vendor": "openai",
                "values": {"api_key": "secret-key"},
            },
        ],
    }


def test_full_answer_graph_is_verified_from_events() -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "answer",
                "data": {"type": "answer", "answer": "Hello {{#start.name#}}"},
            },
        ],
        edges=[_edge("start", "answer")],
    )

    engine = loads(
        dsl,
        workflow_id="workflow-envelope",
        start_inputs={"name": "Graphon"},
    )
    layer = MagicMock(spec=Layer)
    engine.add_layer(cast(Layer, layer))
    events = list(engine.run())

    assert event_path(events) == [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->answer"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "answer"),
        _event("NodeRunSucceededEvent", "answer"),
        _event("GraphRunSucceededEvent"),
    ]
    assert {event.graph_id for event in events} == {"workflow-envelope"}
    assert len({event.execution_id for event in events}) == 1
    assert events[0].execution_id
    assert len({event.id for event in events}) == len(events)
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert [event.event_type for event in events] == [
        type(event).__name__ for event in events
    ]
    assert {event.schema_version for event in events} == {"1.0"}
    assert all(event.emitted_at.tzinfo is UTC for event in events)
    layer_events = [call.args[0] for call in layer.on_event.call_args_list]
    assert len(layer_events) == len(events)
    assert all(
        layer_event is emitted_event
        for layer_event, emitted_event in zip(layer_events, events, strict=True)
    )
    outputs = final_outputs(events)
    assert set(outputs) == {"answer", "files"}
    assert outputs["answer"] == "Hello Graphon"


def test_resume_replays_tasks_through_a_bounded_ready_queue() -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {"id": "answer", "data": {"type": "answer", "answer": "done"}},
        ],
        edges=[_edge("start", "answer")],
    )
    engine = loads(
        dsl,
        workers=1,
    )
    ready_queue = _use_bounded_ready_queue(engine)
    engine.graph_runtime_state.graph_execution.start()
    ready_queue.put(StartTask(frame_id="root", node_id="start"))
    engine.graph_runtime_state.defer_ready_task(
        StartTask(frame_id="root", node_id="answer"),
    )
    errors: list[Exception] = []
    finished = Event()

    def start_execution() -> None:
        try:
            engine._start_execution(resume=True)
        except Exception as error:  # ruff: ignore[blind-except] - re-raised in the test thread.
            errors.append(error)
        finally:
            finished.set()

    thread = Thread(target=start_execution, daemon=True)
    thread.start()
    completed_in_time = finished.wait(timeout=_ENGINE_TIMEOUT_SECONDS)
    if not completed_in_time:
        ready_queue.drain()
        finished.wait(timeout=_ENGINE_TIMEOUT_SECONDS)
    thread.join(timeout=_ENGINE_TIMEOUT_SECONDS)
    engine._stop_execution()

    assert completed_in_time, "resume blocked while replaying bounded queue tasks"
    assert not thread.is_alive()
    if errors:
        raise errors[0]


def test_full_iteration_graph_records_process_and_final_outputs() -> None:
    dsl = _iteration_dsl(is_parallel=False, parallel_nums=1)

    events = run_workflow(dsl, start_inputs={"items": ["alpha", "beta"]})

    assert event_path(events) == [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->iteration"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "iteration"),
        _event("NodeRunIterationStartedEvent", "iteration"),
        _event("NodeRunIterationNextEvent", "iteration"),
        _event(
            "GraphEdgeTakenEvent",
            "iteration-start->render",
            container_id="iteration",
        ),
        _event("NodeRunStartedEvent", "render", container_id="iteration"),
        _event("NodeRunSucceededEvent", "render", container_id="iteration"),
        _event("NodeRunIterationNextEvent", "iteration"),
        _event(
            "GraphEdgeTakenEvent",
            "iteration-start->render",
            container_id="iteration",
        ),
        _event("NodeRunStartedEvent", "render", container_id="iteration"),
        _event("NodeRunSucceededEvent", "render", container_id="iteration"),
        _event("NodeRunIterationSucceededEvent", "iteration"),
        _event("GraphEdgeTakenEvent", "iteration->end"),
        _event("NodeRunSucceededEvent", "iteration"),
        _event("NodeRunStartedEvent", "end"),
        _event("NodeRunSucceededEvent", "end"),
        _event("GraphRunSucceededEvent"),
    ]
    progress = [
        event.index for event in events if isinstance(event, NodeRunIterationNextEvent)
    ]
    succeeded = next(
        event for event in events if isinstance(event, NodeRunIterationSucceededEvent)
    )
    assert progress == [0, 1]
    assert succeeded.steps == 2
    assert succeeded.outputs == {"output": ["alpha!", "beta!"]}
    assert final_outputs(events) == {"items": ["alpha!", "beta!"]}


def test_parallel_iteration_with_one_worker_and_a_bounded_ready_queue() -> None:
    dsl = _iteration_dsl(is_parallel=True, parallel_nums=2)
    engine = loads(
        dsl,
        start_inputs={"items": ["alpha", "beta"]},
        workers=1,
    )
    _use_bounded_ready_queue(engine)

    events = _run_with_timeout(engine)

    progress = [
        event.index for event in events if isinstance(event, NodeRunIterationNextEvent)
    ]
    assert progress == [0, 1]
    assert final_outputs(events) == {"items": ["alpha!", "beta!"]}


@pytest.mark.parametrize(
    ("break_value", "completed_rounds"),
    [("0", 0), ("2", 2)],
)
def test_full_loop_graph_breaks_at_the_configured_condition(
    break_value: str,
    completed_rounds: int,
) -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "loop",
                "data": {
                    "type": "loop",
                    "title": "Break loop",
                    "loop_count": 5,
                    "start_node_id": "loop-start",
                    "break_conditions": [
                        {
                            "variable_selector": ["loop", "counter"],
                            "comparison_operator": "≥",
                            "value": break_value,
                        }
                    ],
                    "logical_operator": "and",
                    "loop_variables": [
                        {
                            "label": "counter",
                            "var_type": "number",
                            "value_type": "constant",
                            "value": 0,
                        },
                    ],
                },
            },
            {
                "id": "loop-start",
                "data": {
                    "type": "loop-start",
                    "loop_id": "loop",
                    "container_id": "loop",
                },
            },
            {
                "id": "increment",
                "data": {
                    "type": "assigner",
                    "version": "2",
                    "loop_id": "loop",
                    "items": [
                        {
                            "variable_selector": ["loop", "counter"],
                            "input_type": "constant",
                            "operation": "+=",
                            "value": 1,
                        }
                    ],
                },
            },
            _end_node([
                {
                    "variable": "counter",
                    "value_selector": ["loop", "counter"],
                },
            ]),
        ],
        edges=[
            _edge("start", "loop"),
            _edge("loop-start", "increment"),
            _edge("loop", "end"),
        ],
    )

    events = run_workflow(dsl)

    expected_path = [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->loop"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "loop"),
        _event("NodeRunLoopStartedEvent", "loop"),
    ]
    for index in range(completed_rounds):
        expected_path.extend([
            _event(
                "GraphEdgeTakenEvent",
                "loop-start->increment",
                container_id="loop",
            ),
            _event("NodeRunStartedEvent", "increment", container_id="loop"),
            _event("NodeRunVariableUpdatedEvent", "increment", container_id="loop"),
            _event("NodeRunSucceededEvent", "increment", container_id="loop"),
        ])
        if index + 1 < completed_rounds:
            expected_path.append(_event("NodeRunLoopNextEvent", "loop"))
    expected_path.extend([
        _event("NodeRunLoopSucceededEvent", "loop"),
        _event("GraphEdgeTakenEvent", "loop->end"),
        _event("NodeRunSucceededEvent", "loop"),
        _event("NodeRunStartedEvent", "end"),
        _event("NodeRunSucceededEvent", "end"),
        _event("GraphRunSucceededEvent"),
    ])

    succeeded = next(
        event for event in events if isinstance(event, NodeRunLoopSucceededEvent)
    )
    assert event_path(events) == expected_path
    assert succeeded.steps == (0 if completed_rounds == 0 else 5)
    assert succeeded.outputs == (
        {} if completed_rounds == 0 else {"counter": 2, "loop_round": 2}
    )
    assert succeeded.metadata["completed_reason"] == "loop_break"
    assert final_outputs(events) == {"counter": completed_rounds}


def test_full_loop_graph_stops_at_loop_end_node() -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "loop",
                "data": {
                    "type": "loop",
                    "loop_count": 3,
                    "start_node_id": "loop-start",
                    "break_conditions": [],
                    "logical_operator": "and",
                },
            },
            {
                "id": "loop-start",
                "data": {"type": "loop-start", "loop_id": "loop"},
            },
            {
                "id": "stop",
                "data": {"type": "loop-end", "loop_id": "loop"},
            },
            _end_node([]),
        ],
        edges=[
            _edge("start", "loop"),
            _edge("loop-start", "stop"),
            _edge("loop", "end"),
        ],
    )

    events = run_workflow(dsl)

    assert event_path(events) == [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->loop"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "loop"),
        _event("NodeRunLoopStartedEvent", "loop"),
        _event(
            "GraphEdgeTakenEvent",
            "loop-start->stop",
            container_id="loop",
        ),
        _event("NodeRunStartedEvent", "stop", container_id="loop"),
        _event("NodeRunSucceededEvent", "stop", container_id="loop"),
        _event("NodeRunLoopSucceededEvent", "loop"),
        _event("GraphEdgeTakenEvent", "loop->end"),
        _event("NodeRunSucceededEvent", "loop"),
        _event("NodeRunStartedEvent", "end"),
        _event("NodeRunSucceededEvent", "end"),
        _event("GraphRunSucceededEvent"),
    ]
    succeeded = next(
        event for event in events if isinstance(event, NodeRunLoopSucceededEvent)
    )
    assert succeeded.outputs == {"loop_round": 1}
    assert succeeded.metadata["completed_reason"] == "loop_break"


def test_full_loop_graph_uses_custom_container_handler_factory() -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "loop",
                "data": {
                    "type": "loop",
                    "loop_count": 1,
                    "start_node_id": "loop-start",
                    "break_conditions": [],
                    "logical_operator": "and",
                },
            },
            {
                "id": "loop-start",
                "data": {"type": "loop-start", "loop_id": "loop"},
            },
            _end_node([]),
        ],
        edges=[
            _edge("start", "loop"),
            _edge("loop", "end"),
        ],
    )
    loaded_handlers: list[LoopContainerHandler] = []

    def load_loop_handler(frame_registry: FrameRegistry) -> LoopContainerHandler:
        handler = LoopContainerHandler(frame_registry)
        loaded_handlers.append(handler)
        return handler

    engine = loads(
        dsl,
        container_handler_factories=(load_loop_handler,),
    )
    events = list(engine.run())

    assert len(loaded_handlers) == 1
    assert (
        engine._container_handlers[LoopContainerHandler.node_type] is loaded_handlers[0]
    )
    assert any(isinstance(event, NodeRunLoopSucceededEvent) for event in events)


def test_nested_iteration_loop_end_stops_ancestor_loop() -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "loop",
                "data": {
                    "type": "loop",
                    "loop_count": 3,
                    "start_node_id": "loop-start",
                    "break_conditions": [],
                    "logical_operator": "and",
                },
            },
            {
                "id": "loop-start",
                "data": {"type": "loop-start", "loop_id": "loop"},
            },
            {
                "id": "iteration",
                "data": {
                    "type": "iteration",
                    "loop_id": "loop",
                    "container_id": "loop",
                    "iterator_selector": ["start", "items"],
                    "output_selector": ["iteration", "item"],
                    "start_node_id": "iteration-start",
                    "is_parallel": False,
                    "parallel_nums": 1,
                    "error_handle_mode": "terminated",
                    "flatten_output": False,
                },
            },
            {
                "id": "iteration-start",
                "data": {
                    "type": "iteration-start",
                    "loop_id": "loop",
                    "iteration_id": "iteration",
                    "container_id": "iteration",
                },
            },
            {
                "id": "stop",
                "data": {
                    "type": "loop-end",
                    "loop_id": "loop",
                    "iteration_id": "iteration",
                    "container_id": "iteration",
                },
            },
            _end_node([]),
        ],
        edges=[
            _edge("start", "loop"),
            _edge("loop-start", "iteration"),
            _edge("iteration-start", "stop"),
            _edge("loop", "end"),
        ],
    )

    events = run_workflow(dsl, start_inputs={"items": ["only"]})

    stop_succeeded = next(
        event
        for event in events
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "stop"
    )
    loop_succeeded = next(
        event for event in events if isinstance(event, NodeRunLoopSucceededEvent)
    )
    iteration_succeeded = [
        event for event in events if isinstance(event, NodeRunIterationSucceededEvent)
    ]
    assert stop_succeeded.container_id == "iteration"
    assert loop_succeeded.outputs == {"loop_round": 1}
    assert loop_succeeded.metadata["completed_reason"] == "loop_break"
    assert len(iteration_succeeded) == 1


def test_loop_condition_error_uses_node_failure_lifecycle() -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "loop",
                "data": {
                    "type": "loop",
                    "loop_count": 2,
                    "start_node_id": "loop-start",
                    "break_conditions": [
                        {
                            "variable_selector": ["missing", "value"],
                            "comparison_operator": "≥",
                            "value": "1",
                        },
                    ],
                    "logical_operator": "and",
                },
            },
            {
                "id": "loop-start",
                "data": {"type": "loop-start", "loop_id": "loop"},
            },
        ],
        edges=[_edge("start", "loop")],
    )

    events = _run_failed_workflow(dsl, start_inputs={})

    assert event_path(events)[-3:] == [
        _event("NodeRunLoopFailedEvent", "loop"),
        _event("NodeRunFailedEvent", "loop"),
        _event("GraphRunFailedEvent"),
    ]
    terminal = events[-1]
    assert isinstance(terminal, GraphRunFailedEvent)
    assert terminal.exceptions_count == 1


def test_full_loop_graph_propagates_child_failure() -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "loop",
                "data": {
                    "type": "loop",
                    "title": "Failing loop",
                    "loop_count": 2,
                    "start_node_id": "loop-start",
                    "break_conditions": [],
                    "logical_operator": "and",
                },
            },
            {
                "id": "loop-start",
                "data": {"type": "loop-start", "loop_id": "loop"},
            },
            {
                "id": "child-response",
                "data": {
                    "type": "end",
                    "loop_id": "loop",
                    "outputs": [
                        {
                            "variable": "partial",
                            "value_selector": ["start", "value"],
                        },
                    ],
                },
            },
            _failing_assigner(container_field="loop_id", container_id="loop"),
        ],
        edges=[
            _edge("start", "loop"),
            _edge("loop-start", "child-response"),
            _edge("child-response", "fail"),
        ],
    )

    engine = loads(dsl, start_inputs={"value": "partial"})
    events: list[EngineEvent] = []
    with pytest.raises(RuntimeError, match="Variable"):
        events.extend(engine.run())

    assert event_path(events) == [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->loop"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "loop"),
        _event("NodeRunLoopStartedEvent", "loop"),
        _event(
            "GraphEdgeTakenEvent",
            "loop-start->child-response",
            container_id="loop",
        ),
        _event("NodeRunStartedEvent", "child-response", container_id="loop"),
        _event(
            "GraphEdgeTakenEvent",
            "child-response->fail",
            container_id="loop",
        ),
        _event("NodeRunSucceededEvent", "child-response", container_id="loop"),
        _event("NodeRunStartedEvent", "fail", container_id="loop"),
        _event("NodeRunFailedEvent", "fail", container_id="loop"),
        _event("NodeRunLoopFailedEvent", "loop"),
        _event("NodeRunFailedEvent", "loop"),
        _event("GraphRunFailedEvent"),
    ]
    failed = next(
        event for event in events if isinstance(event, NodeRunLoopFailedEvent)
    )
    terminal = events[-1]
    assert isinstance(terminal, GraphRunFailedEvent)
    assert failed.steps == 2
    assert failed.metadata["completed_reason"] == "error"
    assert terminal.exceptions_count == 2
    assert engine.graph_runtime_state.outputs == {}


@pytest.mark.parametrize(
    ("items", "flatten_output", "expected_output"),
    [
        ([], True, []),
        ([["a"], ["b", "c"]], True, ["a", "b", "c"]),
        ([["a"], ["b", "c"]], False, [["a"], ["b", "c"]]),
    ],
)
def test_full_iteration_graph_handles_empty_and_nested_outputs(
    items: list[object],
    flatten_output: bool,
    expected_output: list[object],
) -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "iteration",
                "data": {
                    "type": "iteration",
                    "title": "Identity iteration",
                    "iterator_selector": ["start", "items"],
                    "output_selector": ["iteration", "item"],
                    "start_node_id": "iteration-start",
                    "is_parallel": False,
                    "parallel_nums": 1,
                    "error_handle_mode": "terminated",
                    "flatten_output": flatten_output,
                },
            },
            {
                "id": "iteration-start",
                "data": {
                    "type": "iteration-start",
                    "iteration_id": "iteration",
                },
            },
            _end_node([
                {
                    "variable": "items",
                    "value_selector": ["iteration", "output"],
                }
            ]),
        ],
        edges=[
            _edge("start", "iteration"),
            _edge("iteration", "end"),
        ],
    )

    events = run_workflow(dsl, start_inputs={"items": items})
    expected_path = [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->iteration"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "iteration"),
        _event("NodeRunIterationStartedEvent", "iteration"),
    ]
    expected_path.extend(
        _event("NodeRunIterationNextEvent", "iteration") for _ in items
    )
    expected_path.extend([
        _event("NodeRunIterationSucceededEvent", "iteration"),
        _event("GraphEdgeTakenEvent", "iteration->end"),
        _event("NodeRunSucceededEvent", "iteration"),
        _event("NodeRunStartedEvent", "end"),
        _event("NodeRunSucceededEvent", "end"),
        _event("GraphRunSucceededEvent"),
    ])

    succeeded = next(
        event for event in events if isinstance(event, NodeRunIterationSucceededEvent)
    )
    iteration_output = succeeded.outputs["output"]
    if isinstance(iteration_output, Segment):
        iteration_output = iteration_output.to_object()
    assert event_path(events) == expected_path
    assert succeeded.steps == len(items)
    assert iteration_output == expected_output
    assert final_outputs(events) == {"items": expected_output}


@pytest.mark.parametrize(
    ("error_handle_mode", "expected_output"),
    [
        ("terminated", []),
        ("continue-on-error", [None, None]),
        ("remove-abnormal-output", []),
    ],
)
def test_full_iteration_graph_applies_error_handling_mode(
    error_handle_mode: str,
    expected_output: list[object],
) -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "iteration",
                "data": {
                    "type": "iteration",
                    "title": "Failing iteration",
                    "iterator_selector": ["start", "items"],
                    "output_selector": ["fail", "output"],
                    "start_node_id": "iteration-start",
                    "is_parallel": False,
                    "parallel_nums": 1,
                    "error_handle_mode": error_handle_mode,
                    "flatten_output": True,
                },
            },
            {
                "id": "iteration-start",
                "data": {
                    "type": "iteration-start",
                    "iteration_id": "iteration",
                },
            },
            {
                "id": "child-response",
                "data": {
                    "type": "end",
                    "iteration_id": "iteration",
                    "outputs": [
                        {
                            "variable": "partial",
                            "value_selector": ["iteration", "item"],
                        },
                    ],
                },
            },
            _failing_assigner(
                container_field="iteration_id",
                container_id="iteration",
            ),
            _end_node([
                {
                    "variable": "items",
                    "value_selector": ["iteration", "output"],
                }
            ]),
        ],
        edges=[
            _edge("start", "iteration"),
            _edge("iteration-start", "child-response"),
            _edge("child-response", "fail"),
            _edge("iteration", "end"),
        ],
    )
    start_inputs = {"items": ["a", "b"]}
    events = (
        _run_failed_workflow(dsl, start_inputs=start_inputs)
        if error_handle_mode == "terminated"
        else run_workflow(dsl, start_inputs=start_inputs)
    )
    executed_items = 1 if error_handle_mode == "terminated" else 2
    expected_path = [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->iteration"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "iteration"),
        _event("NodeRunIterationStartedEvent", "iteration"),
    ]
    for _ in range(executed_items):
        expected_path.append(_event("NodeRunIterationNextEvent", "iteration"))
        expected_path.extend([
            _event(
                "GraphEdgeTakenEvent",
                "iteration-start->child-response",
                container_id="iteration",
            ),
            _event(
                "NodeRunStartedEvent",
                "child-response",
                container_id="iteration",
            ),
            _event(
                "GraphEdgeTakenEvent",
                "child-response->fail",
                container_id="iteration",
            ),
            _event(
                "NodeRunSucceededEvent",
                "child-response",
                container_id="iteration",
            ),
            _event("NodeRunStartedEvent", "fail", container_id="iteration"),
            _event("NodeRunFailedEvent", "fail", container_id="iteration"),
        ])
    if error_handle_mode == "terminated":
        expected_path.extend([
            _event("NodeRunIterationFailedEvent", "iteration"),
            _event("NodeRunFailedEvent", "iteration"),
            _event("GraphRunFailedEvent"),
        ])
        failed = next(
            event for event in events if isinstance(event, NodeRunIterationFailedEvent)
        )
        terminal = events[-1]
        assert isinstance(terminal, GraphRunFailedEvent)
        assert failed.outputs == {"output": expected_output}
        assert terminal.exceptions_count == 2
    else:
        expected_path.extend([
            _event("NodeRunIterationSucceededEvent", "iteration"),
            _event("GraphEdgeTakenEvent", "iteration->end"),
            _event("NodeRunSucceededEvent", "iteration"),
            _event("NodeRunStartedEvent", "end"),
            _event("NodeRunSucceededEvent", "end"),
            _event("GraphRunPartialSucceededEvent"),
        ])
        succeeded = next(
            event
            for event in events
            if isinstance(event, NodeRunIterationSucceededEvent)
        )
        terminal = events[-1]
        assert isinstance(terminal, GraphRunPartialSucceededEvent)
        assert succeeded.outputs == {"output": expected_output}
        assert terminal.outputs == {"items": expected_output}
        assert terminal.exceptions_count == 2
    assert event_path(events) == expected_path


def test_full_llm_graph_uses_mocked_slim_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_llm = fake_slim_llm(
        monkeypatch,
        responses={"gpt-test": ["mocked answer"]},
    )
    dsl = _graph_dsl(
        dependencies=[_openai_dependency()],
        nodes=[
            _start_node(),
            {
                "id": "llm",
                "data": {
                    "type": "llm",
                    "title": "LLM",
                    "model": {
                        "provider": "langgenius/openai/openai",
                        "name": "gpt-test",
                        "mode": "chat",
                        "completion_params": {"temperature": 0},
                    },
                    "prompt_template": [
                        {"role": "user", "text": "Reply to {{#sys.query#}}"},
                    ],
                    "context": {"enabled": False},
                },
            },
            _end_node([
                {
                    "variable": "text",
                    "value_selector": ["llm", "text"],
                },
            ]),
        ],
        edges=[_edge("start", "llm"), _edge("llm", "end")],
    )

    events = run_workflow(
        dsl,
        credentials=_openai_credentials(),
        start_inputs={"query": "Graphon"},
    )

    assert event_path(events) == [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->llm"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "llm"),
        _event("NodeRunStreamChunkEvent", "llm"),
        _event("GraphEdgeTakenEvent", "llm->end"),
        _event("NodeRunSucceededEvent", "llm"),
        _event("NodeRunStartedEvent", "end"),
        _event("NodeRunSucceededEvent", "end"),
        _event("GraphRunSucceededEvent"),
    ]
    assert final_outputs(events) == {"text": "mocked answer"}
    assert fake_llm.instances[-1].invoke_calls[-1]["stream"] is True
