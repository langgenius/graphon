from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest
import yaml

from graphon.dsl import loads
from graphon.graph_events.base import GraphEngineEvent
from graphon.graph_events.graph import (
    GraphRunFailedEvent,
    GraphRunPartialSucceededEvent,
)
from graphon.graph_events.iteration import (
    NodeRunIterationFailedEvent,
    NodeRunIterationNextEvent,
    NodeRunIterationSucceededEvent,
)
from graphon.graph_events.loop import (
    NodeRunLoopFailedEvent,
    NodeRunLoopSucceededEvent,
)
from graphon.variables.segments import Segment
from tests.helpers.workflow_events import (
    event_path,
    fake_slim_llm,
    final_outputs,
    run_workflow,
)

_OPENAI_PLUGIN_ID = "langgenius/openai:0.3.8@test"


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


def _event(
    event_type: str,
    subject: str = "",
    in_loop: str = "",
    in_iteration: str = "",
) -> tuple[str, str, str, str]:
    return event_type, subject, in_loop, in_iteration


def _run_failed_workflow(
    dsl: str,
    *,
    start_inputs: Mapping[str, object],
) -> list[GraphEngineEvent]:
    events: list[GraphEngineEvent] = []
    engine = loads(dsl, start_inputs=start_inputs)
    with pytest.raises(RuntimeError, match="Variable"):
        events.extend(engine.run())
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

    events = run_workflow(dsl, start_inputs={"name": "Graphon"})

    assert event_path(events) == [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->answer"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "answer"),
        _event("NodeRunSucceededEvent", "answer"),
        _event("GraphRunSucceededEvent"),
    ]
    outputs = final_outputs(events)
    assert set(outputs) == {"answer", "files"}
    assert outputs["answer"] == "Hello Graphon"


def test_full_iteration_graph_records_process_and_final_outputs() -> None:
    dsl = _graph_dsl(
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
                    "is_parallel": False,
                    "parallel_nums": 1,
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

    events = run_workflow(dsl, start_inputs={"items": ["alpha", "beta"]})

    assert event_path(events) == [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->iteration"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "iteration"),
        _event("NodeRunIterationStartedEvent", "iteration"),
        _event("NodeRunIterationNextEvent", "iteration"),
        _event("GraphEdgeTakenEvent", "iteration-start->render"),
        _event("NodeRunStartedEvent", "render", in_iteration="iteration"),
        _event("NodeRunSucceededEvent", "render", in_iteration="iteration"),
        _event("NodeRunIterationNextEvent", "iteration"),
        _event("GraphEdgeTakenEvent", "iteration-start->render"),
        _event("NodeRunStartedEvent", "render", in_iteration="iteration"),
        _event("NodeRunSucceededEvent", "render", in_iteration="iteration"),
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
                "data": {"type": "loop-start", "loop_id": "loop"},
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
            _event("GraphEdgeTakenEvent", "loop-start->increment"),
            _event("NodeRunStartedEvent", "increment", in_loop="loop"),
            _event("NodeRunVariableUpdatedEvent", "increment", in_loop="loop"),
            _event("NodeRunSucceededEvent", "increment", in_loop="loop"),
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
        _event("GraphEdgeTakenEvent", "loop-start->stop"),
        _event("NodeRunStartedEvent", "stop", in_loop="loop"),
        _event("NodeRunSucceededEvent", "stop", in_loop="loop"),
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
            _failing_assigner(container_field="loop_id", container_id="loop"),
        ],
        edges=[
            _edge("start", "loop"),
            _edge("loop-start", "fail"),
        ],
    )

    events = _run_failed_workflow(dsl, start_inputs={})

    assert event_path(events) == [
        _event("GraphRunStartedEvent"),
        _event("NodeRunStartedEvent", "start"),
        _event("GraphEdgeTakenEvent", "start->loop"),
        _event("NodeRunSucceededEvent", "start"),
        _event("NodeRunStartedEvent", "loop"),
        _event("NodeRunLoopStartedEvent", "loop"),
        _event("GraphEdgeTakenEvent", "loop-start->fail"),
        _event("NodeRunStartedEvent", "fail", in_loop="loop"),
        _event("NodeRunFailedEvent", "fail", in_loop="loop"),
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
            _edge("iteration-start", "fail"),
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
            _event("GraphEdgeTakenEvent", "iteration-start->fail"),
            _event("NodeRunStartedEvent", "fail", in_iteration="iteration"),
            _event("NodeRunFailedEvent", "fail", in_iteration="iteration"),
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
