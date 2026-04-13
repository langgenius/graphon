from types import SimpleNamespace

from graphon.enums import BuiltinNodeTypes, NodeExecutionType, NodeState
from graphon.graph_engine.response_coordinator.coordinator import (
    ResponseStreamCoordinator,
)
from graphon.graph_engine.response_coordinator.session import ResponseSession
from graphon.nodes.base.template import Template, TextSegment
from graphon.runtime.variable_pool import VariablePool


def test_process_text_segment_uses_response_node_text_selector() -> None:
    end_node = SimpleNamespace(
        id="end-node",
        node_type=BuiltinNodeTypes.END,
        execution_type=NodeExecutionType.RESPONSE,
        state=NodeState.UNKNOWN,
        get_streaming_text_selector=lambda: ["end-node", "summary"],
    )
    graph = SimpleNamespace(
        nodes={"end-node": end_node},
        root_node=end_node,
    )
    coordinator = ResponseStreamCoordinator(
        variable_pool=VariablePool(),
        graph=graph,
    )
    coordinator.track_node_execution("end-node", "run-1")
    coordinator.activate_session(
        ResponseSession(
            node_id="end-node",
            template=Template(segments=[TextSegment(text="\n")]),
        )
    )

    [event] = coordinator.process_text_segment(TextSegment(text="\n"))

    assert event.id == "run-1"
    assert event.node_id == "end-node"
    assert event.selector == ["end-node", "summary"]
    assert event.chunk == "\n"
    assert event.is_final is True
