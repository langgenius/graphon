from unittest.mock import MagicMock, create_autospec

from graphon.graph.edge import Edge
from graphon.graph.graph import Graph
from graphon.graph_engine.graph_state_manager import GraphStateManager
from graphon.graph_engine.graph_traversal.skip_propagator import SkipPropagator


class TestSkipPropagator:
    def test_propagate_skip_from_edge_with_unknown_edges_stops_processing(self) -> None:
        mock_graph = create_autospec(Graph)
        mock_state_manager = create_autospec(GraphStateManager)

        mock_edge = MagicMock(spec=Edge)
        mock_edge.id = "edge_1"
        mock_edge.head = "node_2"

        mock_graph.edges = {"edge_1": mock_edge}

        incoming_edges = [MagicMock(spec=Edge), MagicMock(spec=Edge)]
        mock_graph.get_incoming_edges.return_value = incoming_edges

        mock_state_manager.analyze_edge_states.return_value = {
            "has_unknown": True,
            "has_taken": False,
            "all_skipped": False,
        }

        propagator = SkipPropagator(mock_graph, mock_state_manager)

        propagator.propagate_skip_from_edge("edge_1")

        mock_graph.get_incoming_edges.assert_called_once_with("node_2")
        mock_state_manager.analyze_edge_states.assert_called_once_with(incoming_edges)
        mock_state_manager.enqueue_node.assert_not_called()
        mock_state_manager.start_execution.assert_not_called()
        mock_state_manager.mark_node_skipped.assert_not_called()

    def test_propagate_skip_from_edge_with_taken_edge_enqueues_node(self) -> None:
        mock_graph = create_autospec(Graph)
        mock_state_manager = create_autospec(GraphStateManager)

        mock_edge = MagicMock(spec=Edge)
        mock_edge.id = "edge_1"
        mock_edge.head = "node_2"

        mock_graph.edges = {"edge_1": mock_edge}
        incoming_edges = [MagicMock(spec=Edge)]
        mock_graph.get_incoming_edges.return_value = incoming_edges

        mock_state_manager.analyze_edge_states.return_value = {
            "has_unknown": False,
            "has_taken": True,
            "all_skipped": False,
        }

        propagator = SkipPropagator(mock_graph, mock_state_manager)

        propagator.propagate_skip_from_edge("edge_1")

        mock_state_manager.enqueue_node.assert_called_once_with("node_2")
        mock_state_manager.start_execution.assert_called_once_with("node_2")
        mock_state_manager.mark_node_skipped.assert_not_called()

    def test_propagate_skip_from_edge_with_all_skipped_propagates_to_node(self) -> None:
        mock_graph = create_autospec(Graph)
        mock_state_manager = create_autospec(GraphStateManager)

        mock_edge = MagicMock(spec=Edge)
        mock_edge.id = "edge_1"
        mock_edge.head = "node_2"

        mock_graph.edges = {"edge_1": mock_edge}
        incoming_edges = [MagicMock(spec=Edge)]
        mock_graph.get_incoming_edges.return_value = incoming_edges

        mock_state_manager.analyze_edge_states.return_value = {
            "has_unknown": False,
            "has_taken": False,
            "all_skipped": True,
        }

        propagator = SkipPropagator(mock_graph, mock_state_manager)

        propagator.propagate_skip_from_edge("edge_1")

        mock_state_manager.mark_node_skipped.assert_called_once_with("node_2")
        mock_state_manager.enqueue_node.assert_not_called()
        mock_state_manager.start_execution.assert_not_called()

    def test_propagate_skip_to_node_marks_node_and_outgoing_edges_skipped(self) -> None:
        mock_graph = create_autospec(Graph)
        mock_state_manager = create_autospec(GraphStateManager)

        edge1 = MagicMock(spec=Edge)
        edge1.id = "edge_2"
        edge1.head = "node_downstream_1"

        edge2 = MagicMock(spec=Edge)
        edge2.id = "edge_3"
        edge2.head = "node_downstream_2"

        mock_graph.edges = {"edge_2": edge1, "edge_3": edge2}
        mock_graph.get_outgoing_edges.return_value = [edge1, edge2]
        mock_graph.get_incoming_edges.return_value = []

        propagator = SkipPropagator(mock_graph, mock_state_manager)

        propagator._propagate_skip_to_node("node_1")

        mock_state_manager.mark_node_skipped.assert_called_once_with("node_1")
        mock_state_manager.mark_edge_skipped.assert_any_call("edge_2")
        mock_state_manager.mark_edge_skipped.assert_any_call("edge_3")
        assert mock_state_manager.mark_edge_skipped.call_count == 2

    def test_skip_branch_paths_marks_unselected_edges_and_propagates(self) -> None:
        mock_graph = create_autospec(Graph)
        mock_state_manager = create_autospec(GraphStateManager)

        edge1 = MagicMock(spec=Edge)
        edge1.id = "edge_1"
        edge1.head = "node_downstream_1"

        edge2 = MagicMock(spec=Edge)
        edge2.id = "edge_2"
        edge2.head = "node_downstream_2"

        mock_graph.edges = {"edge_1": edge1, "edge_2": edge2}
        mock_graph.get_incoming_edges.return_value = []

        propagator = SkipPropagator(mock_graph, mock_state_manager)

        propagator.skip_branch_paths([edge1, edge2])

        mock_state_manager.mark_edge_skipped.assert_any_call("edge_1")
        mock_state_manager.mark_edge_skipped.assert_any_call("edge_2")
        assert mock_state_manager.mark_edge_skipped.call_count == 2

    def test_propagate_skip_from_edge_recursively_propagates_through_graph(
        self,
    ) -> None:
        mock_graph = create_autospec(Graph)
        mock_state_manager = create_autospec(GraphStateManager)

        edge1 = MagicMock(spec=Edge)
        edge1.id = "edge_1"
        edge1.head = "node_2"

        edge3 = MagicMock(spec=Edge)
        edge3.id = "edge_3"
        edge3.head = "node_4"

        mock_graph.edges = {"edge_1": edge1, "edge_3": edge3}

        def get_incoming_edges_side_effect(node_id: str) -> list[Edge]:
            if node_id == "node_2":
                return [edge1]
            if node_id == "node_4":
                return [edge3]
            return []

        mock_graph.get_incoming_edges.side_effect = get_incoming_edges_side_effect

        def get_outgoing_edges_side_effect(node_id: str) -> list[Edge]:
            if node_id == "node_2":
                return [edge3]
            if node_id == "node_4":
                return []
            return []

        mock_graph.get_outgoing_edges.side_effect = get_outgoing_edges_side_effect

        mock_state_manager.analyze_edge_states.return_value = {
            "has_unknown": False,
            "has_taken": False,
            "all_skipped": True,
        }

        propagator = SkipPropagator(mock_graph, mock_state_manager)

        propagator.propagate_skip_from_edge("edge_1")

        mock_state_manager.mark_node_skipped.assert_any_call("node_2")
        mock_state_manager.mark_edge_skipped.assert_any_call("edge_3")
        mock_state_manager.mark_node_skipped.assert_any_call("node_4")
        assert mock_state_manager.mark_node_skipped.call_count == 2

    def test_propagate_skip_from_edge_with_mixed_edge_states_handles_correctly(
        self,
    ) -> None:
        mock_graph = create_autospec(Graph)
        mock_state_manager = create_autospec(GraphStateManager)

        mock_edge = MagicMock(spec=Edge)
        mock_edge.id = "edge_1"
        mock_edge.head = "node_2"

        mock_graph.edges = {"edge_1": mock_edge}
        incoming_edges = [
            MagicMock(spec=Edge),
            MagicMock(spec=Edge),
            MagicMock(spec=Edge),
        ]
        mock_graph.get_incoming_edges.return_value = incoming_edges

        mock_state_manager.analyze_edge_states.return_value = {
            "has_unknown": True,
            "has_taken": False,
            "all_skipped": False,
        }

        propagator = SkipPropagator(mock_graph, mock_state_manager)

        propagator.propagate_skip_from_edge("edge_1")
        mock_state_manager.enqueue_node.assert_not_called()
        mock_state_manager.mark_node_skipped.assert_not_called()
