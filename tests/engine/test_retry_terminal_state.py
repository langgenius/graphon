"""Terminal state of a run whose node only failed while it was being retried."""

from __future__ import annotations

import json
from collections.abc import Generator
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


def _retrying_engine(
    monkeypatch: pytest.MonkeyPatch,
    failures: int,
) -> tuple[Any, list[int]]:
    """An engine whose single work node fails *failures* times before succeeding."""
    nodes = [
        _node(
            "source",
            "template-transform",
            template="unused",
            variables=[],
            retry_config={"retry_enabled": True, "max_retries": 2, "retry_interval": 0},
        ),
        _node("answer", "answer", answer="{{#source.output#}}"),
    ]
    engine = loads(
        _dsl(nodes, [("start", "source"), ("source", "answer")]),
        start_inputs={},
        workers=1,
    )
    attempts: list[int] = []

    def run(_node: TemplateTransformNode) -> Generator[NodeEventPayload]:
        attempts.append(1)
        if len(attempts) <= failures:
            message = "retry this"
            raise ValueError(message)
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": "2"},
            )
        )

    monkeypatch.setattr(TemplateTransformNode, "retry", property(lambda _: True))
    monkeypatch.setattr(TemplateTransformNode, "_run", run)
    return engine, attempts


def test_a_node_that_succeeds_on_retry_leaves_the_run_succeeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One failed attempt that is retried is not an exception of the run.

    The failure is only recorded before the failure handler decides to retry,
    so the run used to end as `GraphRunPartialSucceededEvent` with
    `exceptions_count=1` even though every node finished (#403).
    """
    engine, attempts = _retrying_engine(monkeypatch, failures=1)

    events = list(engine.run())

    assert len(attempts) == 2
    terminal = events[-1]
    assert isinstance(terminal, engine_events.GraphRunSucceededEvent), (
        f"expected a succeeded run, got {type(terminal).__name__}"
    )


def test_a_handled_failure_still_marks_the_run_partially_succeeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half of the contract: a failure that is handled still counts.

    `default-value` resolves the failure instead of retrying it, so the node
    never succeeds and the run has to keep reporting the exception.
    """
    engine = loads(
        _dsl(
            [
                _node(
                    "source",
                    "template-transform",
                    template="unused",
                    variables=[],
                    error_strategy="default-value",
                    default_value=[
                        {"key": "output", "type": "string", "value": "fallback"}
                    ],
                ),
                _node("answer", "answer", answer="{{#source.output#}}"),
            ],
            [("start", "source"), ("source", "answer")],
        ),
        start_inputs={},
        workers=1,
    )

    def fail(_node: TemplateTransformNode) -> Generator[NodeEventPayload]:
        message = "failed source"
        raise ValueError(message)
        yield  # pragma: no cover - unreachable, keeps the signature a generator

    monkeypatch.setattr(TemplateTransformNode, "_run", fail)

    events = list(engine.run())

    terminal = events[-1]
    assert isinstance(terminal, engine_events.GraphRunPartialSucceededEvent), (
        f"expected a partially succeeded run, got {type(terminal).__name__}"
    )
    assert terminal.exceptions_count >= 1
