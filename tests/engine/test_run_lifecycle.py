"""Terminal state of a run the consumer stops reading before it finishes."""

from __future__ import annotations

import json
from collections.abc import Generator
from threading import Event
from typing import Any

import pytest

from graphon import engine_events
from graphon.dsl import loads
from graphon.enums import WorkflowNodeExecutionStatus
from graphon.node_events.base import NodeEventPayload, NodeRunResult
from graphon.node_events.node import StreamCompletedEvent
from graphon.nodes.template_transform.template_transform_node import (
    TemplateTransformNode,
)


def _node(node_id: str, node_type: str, **data: Any) -> dict[str, Any]:
    return {"id": node_id, "data": {"type": node_type, **data}}


def _dsl(nodes: list[dict[str, Any]], edges: list[tuple[str, str]]) -> str:
    return json.dumps({
        "kind": "graph",
        "graph": {
            "nodes": [_node("start", "start", variables=[]), *nodes],
            "edges": [{"source": source, "target": target} for source, target in edges],
        },
    })


def _engine_with_a_blocking_node(
    monkeypatch: pytest.MonkeyPatch,
    started: Event,
    release: Event,
) -> Any:
    """An engine whose only work node parks until *release* is set."""
    nodes = [
        _node("source", "template-transform", template="unused", variables=[]),
        _node("answer", "answer", answer="{{#source.output#}}"),
    ]
    engine = loads(
        _dsl(nodes, [("start", "source"), ("source", "answer")]),
        start_inputs={},
        workers=1,
    )

    def run(_node: TemplateTransformNode) -> Generator[NodeEventPayload]:
        started.set()
        assert release.wait(5)
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": "2"},
            )
        )

    monkeypatch.setattr(TemplateTransformNode, "_run", run)
    return engine


def test_closing_the_event_stream_early_marks_the_run_aborted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Walking away from `Engine.run()` is an abort, not a silent success.

    `GeneratorExit` is not an `Exception`, so the failure path never ran and the
    dispatcher's `finally` completed the execution (#404).
    """
    started: Event = Event()
    release: Event = Event()
    engine = _engine_with_a_blocking_node(monkeypatch, started, release)

    stream = engine.run()
    try:
        # The dispatcher feeds the event stream, so the node only starts once
        # the consumer keeps pulling.
        for _ in range(50):
            if started.is_set():
                break
            next(stream)
        assert started.is_set(), "the node never started"
    finally:
        release.set()
    stream.close()

    execution = engine.runtime_state.graph_execution
    assert execution.aborted is True
    assert execution.completed is False
    assert execution.error is not None


def test_consuming_the_stream_to_the_end_still_completes_the_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard only fires when the consumer left early."""
    started: Event = Event()
    release: Event = Event()
    release.set()
    engine = _engine_with_a_blocking_node(monkeypatch, started, release)

    events = list(engine.run())

    execution = engine.runtime_state.graph_execution
    assert isinstance(events[-1], engine_events.GraphRunSucceededEvent)
    assert execution.completed is True
    assert execution.aborted is False
    assert execution.error is None
