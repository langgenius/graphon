from unittest.mock import Mock

import pytest

from graphon.enums import BuiltinNodeTypes, NodeExecutionType, NodeState
from graphon.graph.edge import Edge
from graphon.graph.graph import Graph
from graphon.graph.validation import GraphValidationError
from graphon.nodes.base.node import Node


def create_mock_node(
    node_id: str,
    execution_type: NodeExecutionType,
    state: NodeState = NodeState.UNKNOWN,
) -> Node:
    node = Mock(spec=Node)
    node.id = node_id
    node.execution_type = execution_type
    node.state = state
    node.node_type = BuiltinNodeTypes.START
    return node


class TestGraphBuilder:
    def test_build_runs_default_validators(self):
        root = create_mock_node("root", NodeExecutionType.EXECUTABLE)

        with pytest.raises(GraphValidationError) as exc:
            Graph.new().add_root(root).build()

        assert any(issue.code == "INVALID_ROOT" for issue in exc.value.issues)


class TestMarkInactiveRootBranches:
    def test_single_root_no_marking(self):
        nodes = {
            "root1": create_mock_node("root1", NodeExecutionType.ROOT),
            "child1": create_mock_node("child1", NodeExecutionType.EXECUTABLE),
        }

        edges = {
            "edge1": Edge(
                id="edge1",
                tail="root1",
                head="child1",
                source_handle="source",
            ),
        }

        in_edges = {"child1": ["edge1"]}
        out_edges = {"root1": ["edge1"]}

        Graph._mark_inactive_root_branches(nodes, edges, in_edges, out_edges, "root1")

        assert nodes["root1"].state == NodeState.UNKNOWN
        assert nodes["child1"].state == NodeState.UNKNOWN
        assert edges["edge1"].state == NodeState.UNKNOWN

    def test_multiple_roots_mark_inactive(self):
        nodes = {
            "root1": create_mock_node("root1", NodeExecutionType.ROOT),
            "root2": create_mock_node("root2", NodeExecutionType.ROOT),
            "child1": create_mock_node("child1", NodeExecutionType.EXECUTABLE),
            "child2": create_mock_node("child2", NodeExecutionType.EXECUTABLE),
        }

        edges = {
            "edge1": Edge(
                id="edge1",
                tail="root1",
                head="child1",
                source_handle="source",
            ),
            "edge2": Edge(
                id="edge2",
                tail="root2",
                head="child2",
                source_handle="source",
            ),
        }

        in_edges = {"child1": ["edge1"], "child2": ["edge2"]}
        out_edges = {"root1": ["edge1"], "root2": ["edge2"]}

        Graph._mark_inactive_root_branches(nodes, edges, in_edges, out_edges, "root1")

        assert nodes["root1"].state == NodeState.UNKNOWN
        assert nodes["root2"].state == NodeState.SKIPPED
        assert nodes["child1"].state == NodeState.UNKNOWN
        assert nodes["child2"].state == NodeState.SKIPPED
        assert edges["edge1"].state == NodeState.UNKNOWN
        assert edges["edge2"].state == NodeState.SKIPPED

    def test_shared_downstream_node(self):
        nodes = {
            "root1": create_mock_node("root1", NodeExecutionType.ROOT),
            "root2": create_mock_node("root2", NodeExecutionType.ROOT),
            "child1": create_mock_node("child1", NodeExecutionType.EXECUTABLE),
            "child2": create_mock_node("child2", NodeExecutionType.EXECUTABLE),
            "shared": create_mock_node("shared", NodeExecutionType.EXECUTABLE),
        }

        edges = {
            "edge1": Edge(
                id="edge1",
                tail="root1",
                head="child1",
                source_handle="source",
            ),
            "edge2": Edge(
                id="edge2",
                tail="root2",
                head="child2",
                source_handle="source",
            ),
            "edge3": Edge(
                id="edge3",
                tail="child1",
                head="shared",
                source_handle="source",
            ),
            "edge4": Edge(
                id="edge4",
                tail="child2",
                head="shared",
                source_handle="source",
            ),
        }

        in_edges = {
            "child1": ["edge1"],
            "child2": ["edge2"],
            "shared": ["edge3", "edge4"],
        }
        out_edges = {
            "root1": ["edge1"],
            "root2": ["edge2"],
            "child1": ["edge3"],
            "child2": ["edge4"],
        }

        Graph._mark_inactive_root_branches(nodes, edges, in_edges, out_edges, "root1")

        assert nodes["root1"].state == NodeState.UNKNOWN
        assert nodes["root2"].state == NodeState.SKIPPED
        assert nodes["child1"].state == NodeState.UNKNOWN
        assert nodes["child2"].state == NodeState.SKIPPED
        assert nodes["shared"].state == NodeState.UNKNOWN
        assert edges["edge1"].state == NodeState.UNKNOWN
        assert edges["edge2"].state == NodeState.SKIPPED
        assert edges["edge3"].state == NodeState.UNKNOWN
        assert edges["edge4"].state == NodeState.SKIPPED

    def test_deep_branch_marking(self):
        nodes = {
            "root1": create_mock_node("root1", NodeExecutionType.ROOT),
            "root2": create_mock_node("root2", NodeExecutionType.ROOT),
            "level1_a": create_mock_node("level1_a", NodeExecutionType.EXECUTABLE),
            "level1_b": create_mock_node("level1_b", NodeExecutionType.EXECUTABLE),
            "level2_a": create_mock_node("level2_a", NodeExecutionType.EXECUTABLE),
            "level2_b": create_mock_node("level2_b", NodeExecutionType.EXECUTABLE),
            "level3": create_mock_node("level3", NodeExecutionType.EXECUTABLE),
        }

        edges = {
            "edge1": Edge(
                id="edge1",
                tail="root1",
                head="level1_a",
                source_handle="source",
            ),
            "edge2": Edge(
                id="edge2",
                tail="root2",
                head="level1_b",
                source_handle="source",
            ),
            "edge3": Edge(
                id="edge3",
                tail="level1_a",
                head="level2_a",
                source_handle="source",
            ),
            "edge4": Edge(
                id="edge4",
                tail="level1_b",
                head="level2_b",
                source_handle="source",
            ),
            "edge5": Edge(
                id="edge5",
                tail="level2_b",
                head="level3",
                source_handle="source",
            ),
        }

        in_edges = {
            "level1_a": ["edge1"],
            "level1_b": ["edge2"],
            "level2_a": ["edge3"],
            "level2_b": ["edge4"],
            "level3": ["edge5"],
        }
        out_edges = {
            "root1": ["edge1"],
            "root2": ["edge2"],
            "level1_a": ["edge3"],
            "level1_b": ["edge4"],
            "level2_b": ["edge5"],
        }

        Graph._mark_inactive_root_branches(nodes, edges, in_edges, out_edges, "root1")

        assert nodes["root1"].state == NodeState.UNKNOWN
        assert nodes["root2"].state == NodeState.SKIPPED
        assert nodes["level1_a"].state == NodeState.UNKNOWN
        assert nodes["level1_b"].state == NodeState.SKIPPED
        assert nodes["level2_a"].state == NodeState.UNKNOWN
        assert nodes["level2_b"].state == NodeState.SKIPPED
        assert nodes["level3"].state == NodeState.SKIPPED
        assert edges["edge1"].state == NodeState.UNKNOWN
        assert edges["edge2"].state == NodeState.SKIPPED
        assert edges["edge3"].state == NodeState.UNKNOWN
        assert edges["edge4"].state == NodeState.SKIPPED
        assert edges["edge5"].state == NodeState.SKIPPED

    def test_non_root_execution_type(self):
        nodes = {
            "root1": create_mock_node("root1", NodeExecutionType.ROOT),
            "non_root": create_mock_node("non_root", NodeExecutionType.EXECUTABLE),
            "child1": create_mock_node("child1", NodeExecutionType.EXECUTABLE),
            "child2": create_mock_node("child2", NodeExecutionType.EXECUTABLE),
        }

        edges = {
            "edge1": Edge(
                id="edge1",
                tail="root1",
                head="child1",
                source_handle="source",
            ),
            "edge2": Edge(
                id="edge2",
                tail="non_root",
                head="child2",
                source_handle="source",
            ),
        }

        in_edges = {"child1": ["edge1"], "child2": ["edge2"]}
        out_edges = {"root1": ["edge1"], "non_root": ["edge2"]}

        Graph._mark_inactive_root_branches(nodes, edges, in_edges, out_edges, "root1")

        assert nodes["root1"].state == NodeState.UNKNOWN
        assert nodes["non_root"].state == NodeState.UNKNOWN
        assert nodes["child1"].state == NodeState.UNKNOWN
        assert nodes["child2"].state == NodeState.UNKNOWN
        assert edges["edge1"].state == NodeState.UNKNOWN
        assert edges["edge2"].state == NodeState.UNKNOWN

    def test_empty_graph(self):
        Graph._mark_inactive_root_branches({}, {}, {}, {}, "non_existent")

    def test_three_roots_mark_two_inactive(self):
        nodes = {
            "root1": create_mock_node("root1", NodeExecutionType.ROOT),
            "root2": create_mock_node("root2", NodeExecutionType.ROOT),
            "root3": create_mock_node("root3", NodeExecutionType.ROOT),
            "child1": create_mock_node("child1", NodeExecutionType.EXECUTABLE),
            "child2": create_mock_node("child2", NodeExecutionType.EXECUTABLE),
            "child3": create_mock_node("child3", NodeExecutionType.EXECUTABLE),
        }

        edges = {
            "edge1": Edge(
                id="edge1",
                tail="root1",
                head="child1",
                source_handle="source",
            ),
            "edge2": Edge(
                id="edge2",
                tail="root2",
                head="child2",
                source_handle="source",
            ),
            "edge3": Edge(
                id="edge3",
                tail="root3",
                head="child3",
                source_handle="source",
            ),
        }

        in_edges = {
            "child1": ["edge1"],
            "child2": ["edge2"],
            "child3": ["edge3"],
        }
        out_edges = {
            "root1": ["edge1"],
            "root2": ["edge2"],
            "root3": ["edge3"],
        }

        Graph._mark_inactive_root_branches(nodes, edges, in_edges, out_edges, "root2")

        assert nodes["root1"].state == NodeState.SKIPPED
        assert nodes["root2"].state == NodeState.UNKNOWN
        assert nodes["root3"].state == NodeState.SKIPPED
        assert nodes["child1"].state == NodeState.SKIPPED
        assert nodes["child2"].state == NodeState.UNKNOWN
        assert nodes["child3"].state == NodeState.SKIPPED
        assert edges["edge1"].state == NodeState.SKIPPED
        assert edges["edge2"].state == NodeState.UNKNOWN
        assert edges["edge3"].state == NodeState.SKIPPED

    def test_convergent_paths(self):
        nodes = {
            "root1": create_mock_node("root1", NodeExecutionType.ROOT),
            "root2": create_mock_node("root2", NodeExecutionType.ROOT),
            "root3": create_mock_node("root3", NodeExecutionType.ROOT),
            "mid1": create_mock_node("mid1", NodeExecutionType.EXECUTABLE),
            "mid2": create_mock_node("mid2", NodeExecutionType.EXECUTABLE),
            "convergent": create_mock_node("convergent", NodeExecutionType.EXECUTABLE),
        }

        edges = {
            "edge1": Edge(
                id="edge1",
                tail="root1",
                head="mid1",
                source_handle="source",
            ),
            "edge2": Edge(
                id="edge2",
                tail="root2",
                head="mid2",
                source_handle="source",
            ),
            "edge3": Edge(
                id="edge3",
                tail="root3",
                head="convergent",
                source_handle="source",
            ),
            "edge4": Edge(
                id="edge4",
                tail="mid1",
                head="convergent",
                source_handle="source",
            ),
            "edge5": Edge(
                id="edge5",
                tail="mid2",
                head="convergent",
                source_handle="source",
            ),
        }

        in_edges = {
            "mid1": ["edge1"],
            "mid2": ["edge2"],
            "convergent": ["edge3", "edge4", "edge5"],
        }
        out_edges = {
            "root1": ["edge1"],
            "root2": ["edge2"],
            "root3": ["edge3"],
            "mid1": ["edge4"],
            "mid2": ["edge5"],
        }

        Graph._mark_inactive_root_branches(nodes, edges, in_edges, out_edges, "root1")

        assert nodes["root1"].state == NodeState.UNKNOWN
        assert nodes["root2"].state == NodeState.SKIPPED
        assert nodes["root3"].state == NodeState.SKIPPED
        assert nodes["mid1"].state == NodeState.UNKNOWN
        assert nodes["mid2"].state == NodeState.SKIPPED
        assert nodes["convergent"].state == NodeState.UNKNOWN
        assert edges["edge1"].state == NodeState.UNKNOWN
        assert edges["edge2"].state == NodeState.SKIPPED
        assert edges["edge3"].state == NodeState.SKIPPED
        assert edges["edge4"].state == NodeState.UNKNOWN
        assert edges["edge5"].state == NodeState.SKIPPED
