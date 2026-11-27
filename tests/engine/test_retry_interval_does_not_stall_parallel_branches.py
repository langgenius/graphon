"""A node's retry interval must not hold back independent branches (#402)."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator

import pytest

from graphon.dsl import loads
from graphon.engine_events.node import (
    NodeRunRetryEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.enums import WorkflowNodeExecutionStatus
from graphon.node_events.base import NodeRunResult
from graphon.node_events.node import StreamCompletedEvent
from graphon.nodes.template_transform.template_transform_node import (
    TemplateTransformNode,
)


def test_a_retry_interval_does_not_hold_back_another_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`b` finishes in 0.1 s and is reported at once, while `a` waits its interval."""
    attempts: dict[str, int] = {}

    def fake_run(self: TemplateTransformNode) -> Iterator[StreamCompletedEvent]:
        attempts[self.id] = attempts.get(self.id, 0) + 1

        if self.id == "a" and attempts["a"] == 1:
            transient = "transient"

            raise ValueError(transient)
        if self.id == "b":
            time.sleep(0.1)
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": "ok"},
            )
        )

    monkeypatch.setattr(TemplateTransformNode, "_run", fake_run)
    monkeypatch.setattr(TemplateTransformNode, "retry", property(lambda _self: True))

    def template_node(node_id: str, **data: object) -> dict[str, object]:
        return {
            "id": node_id,
            "data": {
                "type": "template-transform",
                "template": "x",
                "variables": [],
                **data,
            },
        }

    engine = loads(
        json.dumps({
            "kind": "graph",
            "graph": {
                "nodes": [
                    {"id": "start", "data": {"type": "start", "variables": []}},
                    template_node(
                        "a",
                        retry_config={
                            "retry_enabled": True,
                            "max_retries": 1,
                            "retry_interval": 1_000,
                        },
                    ),
                    template_node("b"),
                ],
                "edges": [
                    {"source": "start", "target": "a"},
                    {"source": "start", "target": "b"},
                ],
            },
        }),
        workers=4,
    )

    started = time.monotonic()
    seen = [
        (time.monotonic() - started, type(event), event.node_id)
        for event in engine.run()
        if isinstance(
            event, (NodeRunStartedEvent, NodeRunRetryEvent, NodeRunSucceededEvent)
        )
    ]

    succeeded_b = next(
        at for at, kind, node in seen if kind is NodeRunSucceededEvent and node == "b"
    )
    retried_a = next(
        at for at, kind, node in seen if kind is NodeRunRetryEvent and node == "a"
    )

    # The interval is respected for the node that failed ...
    assert retried_a >= 1.0
    # ... and never delays the branch that did not fail.
    assert succeeded_b < 0.5, seen
