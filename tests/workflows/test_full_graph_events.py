from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest
import yaml

from tests.helpers.workflow_events import (
    expect_event_path,
    fake_slim_llm,
    graph_started,
    graph_succeeded,
    iteration_next,
    iteration_started,
    iteration_succeeded,
    loop_next,
    loop_started,
    loop_succeeded,
    node_started,
    node_succeeded,
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

    expect_event_path(
        events,
        [
            graph_started(),
            node_started("start"),
            node_succeeded("start", outputs={"name": "Graphon"}),
            node_started("answer"),
            node_succeeded("answer", outputs={"answer": "Hello Graphon"}),
            graph_succeeded(outputs={"answer": "Hello Graphon"}),
        ],
    )


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
                    "output_selector": ["item-answer", "answer"],
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
                "id": "item-answer",
                "data": {
                    "type": "answer",
                    "iteration_id": "iteration",
                    "answer": "{{#iteration.item#}}!",
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
            _edge("iteration-start", "item-answer"),
            _edge("iteration", "end"),
        ],
    )

    events = run_workflow(dsl, start_inputs={"items": ["alpha", "beta"]})

    expect_event_path(
        events,
        [
            graph_started(),
            node_started("iteration"),
            iteration_started("iteration"),
            iteration_next("iteration", index=0),
            node_succeeded(
                "item-answer",
                outputs={"answer": "alpha!"},
                in_iteration="iteration",
            ),
            iteration_next("iteration", index=1),
            node_succeeded(
                "item-answer",
                outputs={"answer": "beta!"},
                in_iteration="iteration",
            ),
            iteration_succeeded(
                "iteration",
                steps=2,
                outputs={"output": ["alpha!", "beta!"]},
            ),
            node_succeeded("iteration", outputs={"output": ["alpha!", "beta!"]}),
            graph_succeeded(outputs={"items": ["alpha!", "beta!"]}),
        ],
    )


def test_full_loop_graph_records_rounds_and_final_outputs() -> None:
    dsl = _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "loop",
                "data": {
                    "type": "loop",
                    "title": "Two rounds",
                    "loop_count": 2,
                    "start_node_id": "loop-start",
                    "break_conditions": [],
                    "logical_operator": "and",
                    "loop_variables": [
                        {
                            "label": "seed",
                            "var_type": "string",
                            "value_type": "constant",
                            "value": "fixed",
                        },
                    ],
                    "outputs": {},
                },
            },
            {
                "id": "loop-start",
                "data": {"type": "loop-start", "loop_id": "loop"},
            },
            {
                "id": "loop-answer",
                "data": {
                    "type": "answer",
                    "loop_id": "loop",
                    "answer": "{{#loop.seed#}}",
                },
            },
            _end_node([
                {
                    "variable": "rounds",
                    "value_selector": ["loop", "loop_round"],
                },
                {
                    "variable": "seed",
                    "value_selector": ["loop", "seed"],
                },
            ]),
        ],
        edges=[
            _edge("start", "loop"),
            _edge("loop-start", "loop-answer"),
            _edge("loop", "end"),
        ],
    )

    events = run_workflow(dsl)

    expect_event_path(
        events,
        [
            graph_started(),
            node_started("loop"),
            loop_started("loop"),
            node_succeeded("loop-answer", outputs={"answer": "fixed"}, in_loop="loop"),
            loop_next("loop", index=1),
            node_succeeded("loop-answer", outputs={"answer": "fixed"}, in_loop="loop"),
            loop_succeeded("loop", steps=2, outputs={"seed": "fixed", "loop_round": 2}),
            node_succeeded("loop", outputs={"seed": "fixed", "loop_round": 2}),
            graph_succeeded(outputs={"rounds": 2, "seed": "fixed"}),
        ],
    )


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

    expect_event_path(
        events,
        [
            graph_started(),
            node_started("llm"),
            node_succeeded("llm", outputs={"text": "mocked answer"}),
            graph_succeeded(outputs={"text": "mocked answer"}),
        ],
    )
    assert fake_llm.instances[-1].invoke_calls[-1]["stream"] is True
