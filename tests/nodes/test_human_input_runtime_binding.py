from __future__ import annotations

from collections.abc import Callable
from time import perf_counter

from graphon.nodes.human_input.entities import (
    HITLContext,
    HITLDecision,
    HumanInputNodeData,
    PauseRequested,
)
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.runtime.runtime_state import RuntimeState

from ..helpers import build_init_params, build_variable_pool


class _RecordingHITLCallback:
    def __init__(self) -> None:
        self.contexts: list[HITLContext] = []

    def __call__(self, ctx: HITLContext) -> HITLDecision:
        self.contexts.append(ctx)
        return PauseRequested(session_id="session-1")


def _build_human_input_node(
    *,
    hitl_callback: Callable[[HITLContext], HITLDecision],
) -> HumanInputNode:
    node = HumanInputNode(
        node_id="human-input-node",
        data=HumanInputNodeData(title="Human Input"),
        init_params=build_init_params(
            graph_config={"nodes": [], "edges": []},
            run_context={"workflow_execution_id": "workflow-exec-1"},
        ),
        runtime_state=RuntimeState(
            workflow_id="workflow",
            variable_pool=build_variable_pool(),
            start_at=perf_counter(),
        ),
        hitl_callback=hitl_callback,
    )
    node.bind_execution_id("human-run")
    return node


def test_human_input_node_uses_injected_hitl_callback() -> None:
    callback = _RecordingHITLCallback()
    node = _build_human_input_node(hitl_callback=callback)

    list(node.run())

    assert [ctx.node_id for ctx in callback.contexts] == ["human-input-node"]
