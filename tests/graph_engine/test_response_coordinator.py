from collections.abc import Sequence
from typing import ClassVar

from graphon.enums import BuiltinNodeTypes, NodeExecutionType, NodeState, NodeType
from graphon.graph_engine.response_coordinator.coordinator import (
    ResponseStreamCoordinator,
)
from graphon.graph_engine.response_coordinator.session import ResponseSession
from graphon.nodes.base.template import Template, TextSegment
from graphon.runtime.graph_runtime_state import (
    EdgeProtocol,
    GraphProtocol,
    NodeProtocol,
)
from graphon.runtime.variable_pool import VariablePool


class _TestNode(NodeProtocol):
    node_type: ClassVar[NodeType] = BuiltinNodeTypes.END
    id: str
    execution_type: NodeExecutionType
    state: NodeState

    def __init__(self, node_id: str) -> None:
        self.id = node_id
        self.execution_type = NodeExecutionType.RESPONSE
        self.state = NodeState.UNKNOWN

    def get_streaming_text_selector(self) -> list[str]:
        return [self.id, "summary"]

    def blocks_variable_output(
        self,
        variable_selectors: set[tuple[str, ...]],
    ) -> bool:
        _ = variable_selectors
        return False


class _TestEdge(EdgeProtocol):
    id: str
    state: NodeState
    tail: str
    head: str
    source_handle: str

    def __init__(self) -> None:
        self.id = "edge"
        self.state = NodeState.UNKNOWN
        self.tail = ""
        self.head = ""
        self.source_handle = "source"


class _TestGraph(GraphProtocol):
    def __init__(self, root_node: _TestNode) -> None:
        self._nodes = {root_node.id: root_node}
        self._edges: dict[str, _TestEdge] = {}
        self._root_node = root_node

    @property
    def nodes(self) -> dict[str, _TestNode]:
        return self._nodes

    @property
    def edges(self) -> dict[str, _TestEdge]:
        return self._edges

    @property
    def root_node(self) -> _TestNode:
        return self._root_node

    def get_outgoing_edges(self, node_id: str) -> Sequence[_TestEdge]:
        _ = node_id
        return []


def test_process_text_segment_uses_response_node_text_selector() -> None:
    end_node = _TestNode("end-node")
    graph = _TestGraph(end_node)
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
