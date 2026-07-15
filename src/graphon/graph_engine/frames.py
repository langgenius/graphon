"""Execution frame registry for frame-scoped graph tasks."""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Protocol, cast, final

from graphon.graph.graph import Graph, NodeFactory
from graphon.runtime.container_state import ContainerFrameState
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool

from .error_handler import ErrorHandler
from .graph_state_manager import GraphStateManager
from .graph_traversal.edge_processor import EdgeProcessor
from .graph_traversal.skip_propagator import SkipPropagator
from .ready_queue import ROOT_FRAME_ID


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

    def remove(self, frame_id: str) -> None:
        del self._frames[frame_id]

    def has(self, frame_id: str) -> bool:
        return frame_id in self._frames

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
        graph_runtime_state.attach_graph(graph)
        state_manager = GraphStateManager(
            graph,
            graph_runtime_state,
            frame_id,
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

    def materialize_child_frame_from_state(
        self,
        frame_state: ContainerFrameState,
        *,
        variable_pool: VariablePool,
    ) -> ExecutionFrame:
        runtime_data = frame_state.runtime_data
        root_runtime_state = self.get(ROOT_FRAME_ID).graph_runtime_state
        graph_runtime_state = GraphRuntimeState(
            variable_pool=variable_pool,
            start_at=root_runtime_state.start_at,
            llm_usage=runtime_data.llm_usage,
            outputs=dict(runtime_data.outputs),
            node_run_steps=runtime_data.node_run_steps,
            ready_queue=root_runtime_state.ready_queue,
            deferred_ready_queue=root_runtime_state.deferred_ready_queue,
            graph_execution=root_runtime_state.graph_execution,
        )
        frame = self.materialize_child_frame(
            frame_id=frame_state.frame_id,
            root_node_id=frame_state.root_node_id,
            graph_runtime_state=graph_runtime_state,
        )
        missing_node_ids = sorted(
            set(runtime_data.graph_node_states) - set(frame.graph.nodes),
        )
        missing_edge_ids = sorted(
            set(runtime_data.graph_edge_states) - set(frame.graph.edges),
        )
        if missing_node_ids or missing_edge_ids:
            msg = (
                f"Saved frame state for {frame_state.frame_id} does not match "
                f"rebuilt graph: missing node ids={missing_node_ids}, "
                f"missing edge ids={missing_edge_ids}"
            )
            self.remove(frame_state.frame_id)
            raise RuntimeError(msg)

        for node_id, state in runtime_data.graph_node_states.items():
            frame.graph.nodes[node_id].state = state
        for edge_id, state in runtime_data.graph_edge_states.items():
            frame.graph.edges[edge_id].state = state
        return frame
