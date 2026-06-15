from __future__ import annotations

from collections.abc import Callable
from time import perf_counter
from typing import cast

import pytest

from graphon.entities.pause_reason import HitlRequired
from graphon.graph_events.node import NodeRunPauseRequestedEvent, NodeRunSucceededEvent
from graphon.nodes.human_input.entities import HumanInputNodeData
from graphon.nodes.human_input.hitl import (
    Completed,
    Expired,
    HITLContext,
    HITLDecision,
    PauseRequested,
)
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.variables.segments import StringSegment

from ...helpers import build_graph_init_params, build_variable_pool


def _build_node(
    callback: Callable[[HITLContext], HITLDecision],
    *,
    workflow_execution_id: str | None = "workflow-exec-1",
) -> HumanInputNode:
    run_context = {}
    if workflow_execution_id is not None:
        run_context["workflow_execution_id"] = workflow_execution_id

    return HumanInputNode(
        node_id="human-node",
        data=HumanInputNodeData(title="Collect Input"),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
            run_context=run_context,
        ),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=build_variable_pool(),
            start_at=perf_counter(),
        ),
        hitl_callback=callback,
    )


def test_human_input_pause_uses_callback_context_and_minimal_pause_reason() -> None:
    captured_contexts: list[HITLContext] = []

    def callback(ctx: HITLContext) -> HITLDecision:
        captured_contexts.append(ctx)
        return PauseRequested(session_id="session-1")

    events = list(_build_node(callback).run())
    paused = next(
        event for event in events if isinstance(event, NodeRunPauseRequestedEvent)
    )

    assert captured_contexts == [
        HITLContext(
            workflow_execution_id="workflow-exec-1",
            node_id="human-node",
            node_title="Collect Input",
            variable_pool=captured_contexts[0].variable_pool,
        )
    ]
    assert paused.reason == HitlRequired(
        session_id="session-1",
        node_id="human-node",
        node_title="Collect Input",
    )
    assert paused.reason.model_dump(mode="json") == {
        "TYPE": "hitl_required",
        "session_id": "session-1",
        "node_id": "human-node",
        "node_title": "Collect Input",
    }


def test_human_input_completed_decision_finishes_on_selected_handle() -> None:
    inputs = {"name": StringSegment(value="Alice")}
    outputs = {
        "name": StringSegment(value="Alice"),
        "approval": StringSegment(value="approved"),
    }

    def callback(ctx: HITLContext) -> HITLDecision:
        _ = ctx
        return Completed(
            selected_handle="approve",
            inputs=inputs,
            outputs=outputs,
        )

    events = list(_build_node(callback).run())
    completed = next(
        event for event in events if isinstance(event, NodeRunSucceededEvent)
    )

    assert completed.node_run_result.inputs == inputs
    assert completed.node_run_result.outputs == outputs
    assert completed.node_run_result.edge_source_handle == "approve"


def test_human_input_expired_decision_finishes_on_timeout_handle() -> None:
    outputs = {"reason": StringSegment(value="expired")}

    def callback(ctx: HITLContext) -> HITLDecision:
        _ = ctx
        return Expired(selected_handle="timeout", outputs=outputs)

    events = list(_build_node(callback).run())
    completed = next(
        event for event in events if isinstance(event, NodeRunSucceededEvent)
    )

    assert completed.node_run_result.inputs == {}
    assert completed.node_run_result.outputs == outputs
    assert completed.node_run_result.edge_source_handle == "timeout"


def test_human_input_requires_workflow_execution_id() -> None:
    def callback(ctx: HITLContext) -> HITLDecision:
        _ = ctx
        return PauseRequested(session_id="session-1")

    node = _build_node(callback, workflow_execution_id=None)

    with pytest.raises(ValueError, match="workflow_execution_id is required"):
        list(node._run())


def test_human_input_rejects_unknown_hitl_decision() -> None:
    def callback(ctx: HITLContext) -> HITLDecision:
        _ = ctx
        return cast(HITLDecision, object())

    node = _build_node(callback)

    with pytest.raises(AssertionError, match="unsupported HITL decision"):
        list(node._run())
