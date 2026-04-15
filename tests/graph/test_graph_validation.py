from __future__ import annotations

import time
from collections.abc import Generator, Mapping
from dataclasses import dataclass

import pytest

from graphon.entities.base_node_data import BaseNodeData
from graphon.entities.graph_init_params import GraphInitParams
from graphon.enums import BuiltinNodeTypes, ErrorStrategy, NodeExecutionType, NodeType
from graphon.graph.graph import Graph
from graphon.graph.validation import GraphValidationError
from graphon.node_events.base import NodeEventBase, NodeRunResult
from graphon.nodes.base.node import Node
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool

from ..helpers import build_graph_init_params


class _TestNodeData(BaseNodeData):
    type: NodeType | None = None
    execution_type: NodeExecutionType | str | None = None


class _TestNode(Node[_TestNodeData]):
    node_type = BuiltinNodeTypes.ANSWER
    execution_type = NodeExecutionType.EXECUTABLE

    @classmethod
    def version(cls) -> str:
        return "1"

    def __init__(
        self,
        *,
        node_id: str,
        config: _TestNodeData,
        graph_init_params: GraphInitParams,
        graph_runtime_state: GraphRuntimeState,
    ) -> None:
        super().__init__(
            node_id=node_id,
            config=config,
            graph_init_params=graph_init_params,
            graph_runtime_state=graph_runtime_state,
        )

        node_type_value = self.data.get("type")
        if isinstance(node_type_value, str):
            self.node_type = node_type_value

    def _run(self) -> NodeRunResult | Generator[NodeEventBase, None, None]:
        raise NotImplementedError

    def post_init(self) -> None:
        super().post_init()
        self._maybe_override_execution_type()
        self.data = dict(self.node_data.model_dump())

    def _maybe_override_execution_type(self) -> None:
        execution_type_value = self.node_data.execution_type
        if execution_type_value is None:
            return
        if isinstance(execution_type_value, NodeExecutionType):
            self.execution_type = execution_type_value
        else:
            self.execution_type = NodeExecutionType(execution_type_value)


@dataclass(slots=True)
class _SimpleNodeFactory:
    graph_init_params: GraphInitParams
    graph_runtime_state: GraphRuntimeState

    def create_node(self, node_config: Mapping[str, object]) -> _TestNode:
        return _TestNode(
            node_id=str(node_config["id"]),
            config=_TestNode.validate_node_data(
                node_config["data"],  # type: ignore[arg-type]
            ),
            graph_init_params=self.graph_init_params,
            graph_runtime_state=self.graph_runtime_state,
        )


@pytest.fixture
def graph_init_dependencies() -> tuple[_SimpleNodeFactory, dict[str, object]]:
    graph_config: dict[str, object] = {"edges": [], "nodes": []}
    init_params = build_graph_init_params(
        workflow_id="workflow",
        graph_config=graph_config,
    )
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=time.perf_counter(),
    )
    factory = _SimpleNodeFactory(
        graph_init_params=init_params,
        graph_runtime_state=runtime_state,
    )
    return factory, graph_config


def test_graph_initialization_runs_default_validators(
    graph_init_dependencies: tuple[_SimpleNodeFactory, dict[str, object]],
) -> None:
    node_factory, graph_config = graph_init_dependencies
    graph_config["nodes"] = [
        {
            "id": "start",
            "data": {
                "type": BuiltinNodeTypes.START,
                "title": "Start",
                "execution_type": NodeExecutionType.ROOT,
            },
        },
        {"id": "answer", "data": {"type": BuiltinNodeTypes.ANSWER, "title": "Answer"}},
    ]
    graph_config["edges"] = [
        {"source": "start", "target": "answer", "sourceHandle": "success"},
    ]

    graph = Graph.init(
        graph_config=graph_config,
        node_factory=node_factory,
        root_node_id="start",
    )

    assert graph.root_node.id == "start"
    assert "answer" in graph.nodes


def test_node_data_from_mapping_returns_typed_node_data() -> None:
    node_data = _TestNode.node_data_from_mapping(
        {
            "type": BuiltinNodeTypes.ANSWER,
            "title": "Answer",
            "execution_type": NodeExecutionType.EXECUTABLE,
        },
    )

    assert isinstance(node_data, _TestNodeData)
    assert node_data.type == BuiltinNodeTypes.ANSWER
    assert node_data.execution_type == NodeExecutionType.EXECUTABLE


def test_graph_validation_fails_for_unknown_edge_targets(
    graph_init_dependencies: tuple[_SimpleNodeFactory, dict[str, object]],
) -> None:
    node_factory, graph_config = graph_init_dependencies
    graph_config["nodes"] = [
        {
            "id": "start",
            "data": {
                "type": BuiltinNodeTypes.START,
                "title": "Start",
                "execution_type": NodeExecutionType.ROOT,
            },
        },
    ]
    graph_config["edges"] = [
        {"source": "start", "target": "missing", "sourceHandle": "success"},
    ]

    with pytest.raises(GraphValidationError) as exc:
        Graph.init(
            graph_config=graph_config,
            node_factory=node_factory,
            root_node_id="start",
        )

    assert any(issue.code == "MISSING_NODE" for issue in exc.value.issues)


def test_graph_promotes_fail_branch_nodes_to_branch_execution_type(
    graph_init_dependencies: tuple[_SimpleNodeFactory, dict[str, object]],
) -> None:
    node_factory, graph_config = graph_init_dependencies
    graph_config["nodes"] = [
        {
            "id": "start",
            "data": {
                "type": BuiltinNodeTypes.START,
                "title": "Start",
                "execution_type": NodeExecutionType.ROOT,
            },
        },
        {
            "id": "branch",
            "data": {
                "type": BuiltinNodeTypes.IF_ELSE,
                "title": "Branch",
                "error_strategy": ErrorStrategy.FAIL_BRANCH,
            },
        },
    ]
    graph_config["edges"] = [
        {"source": "start", "target": "branch", "sourceHandle": "success"},
    ]

    graph = Graph.init(
        graph_config=graph_config,
        node_factory=node_factory,
        root_node_id="start",
    )

    assert graph.nodes["branch"].execution_type == NodeExecutionType.BRANCH


def test_graph_init_ignores_custom_note_nodes_before_node_data_validation(
    graph_init_dependencies: tuple[_SimpleNodeFactory, dict[str, object]],
) -> None:
    node_factory, graph_config = graph_init_dependencies
    graph_config["nodes"] = [
        {
            "id": "start",
            "data": {
                "type": BuiltinNodeTypes.START,
                "title": "Start",
                "execution_type": NodeExecutionType.ROOT,
            },
        },
        {"id": "answer", "data": {"type": BuiltinNodeTypes.ANSWER, "title": "Answer"}},
        {
            "id": "note",
            "type": "custom-note",
            "data": {
                "type": "",
                "title": "",
                "desc": "",
                "text": "{}",
                "theme": "blue",
            },
        },
    ]
    graph_config["edges"] = [
        {"source": "start", "target": "answer", "sourceHandle": "success"},
    ]

    graph = Graph.init(
        graph_config=graph_config,
        node_factory=node_factory,
        root_node_id="start",
    )

    assert graph.root_node.id == "start"
    assert "answer" in graph.nodes
    assert "note" not in graph.nodes


def test_graph_init_fails_for_unknown_root_node_id(
    graph_init_dependencies: tuple[_SimpleNodeFactory, dict[str, object]],
) -> None:
    node_factory, graph_config = graph_init_dependencies
    graph_config["nodes"] = [
        {
            "id": "start",
            "data": {
                "type": BuiltinNodeTypes.START,
                "title": "Start",
                "execution_type": NodeExecutionType.ROOT,
            },
        },
    ]
    graph_config["edges"] = []

    with pytest.raises(ValueError, match="Root node id missing not found in the graph"):
        Graph.init(
            graph_config=graph_config,
            node_factory=node_factory,
            root_node_id="missing",
        )
