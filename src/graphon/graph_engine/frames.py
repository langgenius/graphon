"""Execution frame registry for frame-scoped graph tasks."""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Protocol, cast, final

from graphon.graph.graph import Graph, NodeFactory
from graphon.nodes.base.node import Node
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from .error_handler import ErrorHandler
from .graph_state_manager import GraphStateManager
from .graph_traversal.edge_processor import EdgeProcessor
from .graph_traversal.skip_propagator import SkipPropagator
from .ready_queue import ROOT_FRAME_ID, ReadyTask


class RebindableNodeFactory(NodeFactory, Protocol):
    @abstractmethod
    def with_runtime_state(
        self,
        graph_runtime_state: GraphRuntimeState,
    ) -> RebindableNodeFactory: ...


@dataclass(frozen=True, slots=True)
class ExecutionFrame:
    frame_id: str
    graph: Graph
    graph_runtime_state: GraphRuntimeState
    state_manager: GraphStateManager
    edge_processor: EdgeProcessor
    error_handler: ErrorHandler


@final
class FrameRegistry:
    def __init__(self) -> None:
        self._frames: dict[str, ExecutionFrame] = {}

    def register(self, frame: ExecutionFrame) -> None:
        self._frames[frame.frame_id] = frame

    def get(self, frame_id: str) -> ExecutionFrame:
        return self._frames[frame_id]

    def get_node(self, task: ReadyTask) -> Node:
        return self.get(task.frame_id).graph.nodes[task.node_id]

    def materialize_child_frame(
        self,
        *,
        frame_id: str,
        root_node_id: str,
        graph_runtime_state: GraphRuntimeState,
    ) -> ExecutionFrame:
        root_graph = self.get(ROOT_FRAME_ID).graph
        graph_config = root_graph.graph_config
        if graph_config is None:
            msg = "Root graph does not carry graph_config for frame materialization."
            raise RuntimeError(msg)
        node_factory = root_graph.node_factory
        if node_factory is None:
            msg = "Root graph does not carry node_factory for frame materialization."
            raise RuntimeError(msg)

        rebound_factory = cast(RebindableNodeFactory, node_factory).with_runtime_state(
            graph_runtime_state,
        )
        graph = Graph.init(
            graph_config=graph_config,
            node_factory=rebound_factory,
            root_node_id=root_node_id,
        )
        graph_runtime_state.configure(graph=graph)
        state_manager = GraphStateManager(
            graph,
            graph_runtime_state.ready_queue,
        )
        skip_propagator = SkipPropagator(
            graph=graph,
            state_manager=state_manager,
        )
        edge_processor = EdgeProcessor(
            graph=graph,
            state_manager=state_manager,
            skip_propagator=skip_propagator,
        )
        frame = ExecutionFrame(
            frame_id=frame_id,
            graph=graph,
            graph_runtime_state=graph_runtime_state,
            state_manager=state_manager,
            edge_processor=edge_processor,
            error_handler=ErrorHandler(graph, graph_runtime_state.graph_execution),
        )
        self.register(frame)
        return frame
