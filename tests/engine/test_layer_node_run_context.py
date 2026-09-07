import json
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

import pytest

from graphon.dsl import loads
from graphon.engine.layer import Layer
from graphon.engine_events.graph import GraphRunSucceededEvent
from graphon.nodes.base.node import Node


def test_nested_parallel_containers_pass_the_direct_parent_execution() -> None:
    active: ContextVar[str | None] = ContextVar("active", default=None)
    parents: dict[str, tuple[str, str | None]] = {}
    activations: dict[str, int] = {}

    class ContextLayer(Layer):
        @contextmanager
        def node_run_context(
            self,
            node: Node,
            *,
            parent_execution_id: str | None = None,
        ) -> Iterator[None]:
            parents[node.execution_id] = (node.id, parent_execution_id)
            activations[node.execution_id] = activations.get(node.execution_id, 0) + 1
            token = active.set(node.execution_id)
            try:
                yield
            finally:
                active.reset(token)

    engine = loads(
        json.dumps({
            "kind": "graph",
            "graph": {
                "nodes": [
                    {"id": "start", "data": {"type": "start", "variables": []}},
                    {
                        "id": "iteration",
                        "data": {
                            "type": "iteration",
                            "start_node_id": "iteration-start",
                            "iterator_selector": ["start", "items"],
                            "output_selector": ["loop", "loop_round"],
                            "is_parallel": True,
                            "parallel_nums": 2,
                        },
                    },
                    {
                        "id": "iteration-start",
                        "data": {
                            "type": "iteration-start",
                            "container_id": "iteration",
                        },
                    },
                    {
                        "id": "loop",
                        "data": {
                            "type": "loop",
                            "container_id": "iteration",
                            "loop_count": 2,
                            "start_node_id": "loop-start",
                            "break_conditions": [],
                            "logical_operator": "and",
                        },
                    },
                    {
                        "id": "loop-start",
                        "data": {
                            "type": "loop-start",
                            "container_id": "loop",
                        },
                    },
                    {"id": "end", "data": {"type": "end", "outputs": []}},
                ],
                "edges": [
                    {"source": "start", "target": "iteration"},
                    {"source": "iteration-start", "target": "loop"},
                    {"source": "iteration", "target": "end"},
                ],
            },
        }),
        start_inputs={"items": ["a", "b"]},
        workers=2,
    )
    engine.add_layer(ContextLayer())

    events = list(engine.run())

    assert isinstance(events[-1], GraphRunSucceededEvent)
    iteration_ids = {
        execution_id
        for execution_id, (node_id, _) in parents.items()
        if node_id == "iteration"
    }
    loop_ids = {
        execution_id
        for execution_id, (node_id, _) in parents.items()
        if node_id == "loop"
    }
    assert len(iteration_ids) == 1
    assert len(loop_ids) == 2
    assert sum(node_id == "loop-start" for node_id, _ in parents.values()) == 4
    for execution_id, (node_id, parent_id) in parents.items():
        if node_id in {"start", "iteration", "end"}:
            assert parent_id is None
        elif node_id in {"iteration-start", "loop"}:
            assert parent_id in iteration_ids
        else:
            assert node_id == "loop-start"
            assert parent_id in loop_ids
        assert activations[execution_id] == {"iteration": 2, "loop": 3}.get(node_id, 1)
    assert {
        parent_id for node_id, parent_id in parents.values() if node_id == "loop-start"
    } == loop_ids
    assert active.get() is None


@pytest.mark.parametrize("failure", ["enter", "exit"])
def test_layer_context_failure_does_not_fail_node_or_skip_other_layers(
    failure: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    steps: list[str] = []

    class FailingLayer(Layer):
        @contextmanager
        def node_run_context(
            self,
            node: Node,
            *,
            parent_execution_id: str | None = None,
        ) -> Iterator[None]:
            _ = node
            _ = parent_execution_id
            if failure == "enter":
                raise RuntimeError(failure)
            try:
                yield
            finally:
                raise RuntimeError(failure)

    class RecordingLayer(Layer):
        @contextmanager
        def node_run_context(
            self,
            node: Node,
            *,
            parent_execution_id: str | None = None,
        ) -> Iterator[None]:
            _ = node
            _ = parent_execution_id
            steps.append("enter")
            try:
                yield
            finally:
                steps.append("exit")

    engine = loads(
        json.dumps({
            "kind": "graph",
            "graph": {
                "nodes": [{"id": "start", "data": {"type": "start", "variables": []}}],
                "edges": [],
            },
        }),
        workers=1,
    )
    engine.add_layer(FailingLayer())
    engine.add_layer(RecordingLayer())

    events = list(engine.run())

    assert isinstance(events[-1], GraphRunSucceededEvent)
    assert steps == ["enter", "exit"]
    assert f"RuntimeError: {failure}" in caplog.text
