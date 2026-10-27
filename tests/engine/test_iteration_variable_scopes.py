from __future__ import annotations

from collections.abc import Generator, Mapping
from dataclasses import dataclass, field, replace
from threading import Event
from typing import Any, Literal, override

import pytest

from graphon.engine import Engine
from graphon.engine.frame import ExecutionFrame, FrameRegistry
from graphon.engine_events.base import NodeEvent
from graphon.engine_events.node import NodeRunFailedEvent, NodeRunVariableUpdatedEvent
from graphon.entities.base_node_data import BaseNodeData
from graphon.entities.graph_config import NodeConfigDict, NodeConfigDictAdapter
from graphon.enums import NodeExecutionType, WorkflowNodeExecutionStatus
from graphon.graph import Graph
from graphon.node_events.base import NodeRunResult
from graphon.node_events.node import StreamCompletedEvent
from graphon.nodes.base.node import Node
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
    ContainerExecutionResult,
    ContainerNodeRunResult,
    ContainerRunResult,
    CustomContainerRequest,
    build_container_value,
)
from graphon.nodes.end.end_node import EndNode
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.nodes.iteration.iteration_start_node import IterationStartNode
from graphon.nodes.start.start_node import StartNode
from graphon.nodes.variable_assigner.v2.node import VariableAssignerNode
from graphon.runtime import InitParams, RuntimeState, VariablePool
from graphon.runtime.container_state import (
    ContainerFrameState,
    CustomContainerFrameState,
)
from graphon.runtime.ready_queue import ResumeTask
from tests.helpers.workflow_events import final_outputs


class _WorkflowNode(Node[BaseNodeData]):
    node_type = "test-variable-scope-workflow"
    execution_type = NodeExecutionType.CONTAINER

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> Generator[CustomContainerRequest, None, None]:
        yield CustomContainerRequest(payload=self.id)

    @override
    def _resume_container_events(
        self, *, result: ContainerRunResult
    ) -> Generator[StreamCompletedEvent, None, None]:
        assert isinstance(result, ContainerExecutionResult)
        yield StreamCompletedEvent(
            node_run_result=result.node_run_result.to_node_run_result()
        )


class _WaitNode(Node[BaseNodeData]):
    node_type = "test-variable-scope-wait"

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> NodeRunResult:
        signals: Mapping[str, Event] = self.require_run_context_value("signals")
        assert signals[self.id].wait(5), f"Timed out waiting for {self.id}"
        return NodeRunResult(status=WorkflowNodeExecutionStatus.SUCCEEDED)


@dataclass(frozen=True)
class _NodeFactory:
    state: RuntimeState
    signals: Mapping[str, Event]
    graph_config: Mapping[str, Any] = field(default_factory=dict)

    def with_runtime_state(self, runtime_state: RuntimeState) -> _NodeFactory:
        return replace(self, state=runtime_state)

    def with_graph_config(self, graph_config: Mapping[str, Any]) -> _NodeFactory:
        return replace(self, graph_config=graph_config)

    @staticmethod
    def _get_node_class(node_config: NodeConfigDict) -> type[Node[Any]]:
        return next(
            node_class
            for node_class in (
                StartNode,
                EndNode,
                IterationNode,
                IterationStartNode,
                VariableAssignerNode,
                _WorkflowNode,
                _WaitNode,
            )
            if node_class.node_type == node_config["data"].type
        )

    def validate_node(self, node_config: NodeConfigDict) -> NodeExecutionType:
        node_class = self._get_node_class(node_config)
        node_class.validate_node_data(node_config["data"])
        return node_class.execution_type

    def create_node(self, node_config: NodeConfigDict) -> Node[Any]:
        node_class = self._get_node_class(node_config)
        return node_class(
            node_id=node_config["id"],
            data=node_class.validate_node_data(node_config["data"]),
            init_params=InitParams(
                workflow_id="scope-test",
                graph_config=self.graph_config,
                run_context={"signals": self.signals},
                call_depth=0,
            ),
            runtime_state=self.state,
        )


def _build_node_config(node_id: str, node_type: str, **data: Any) -> dict[str, Any]:
    return {"id": node_id, "data": {"type": node_type, **data}}


def _build_edge_config(source: str, target: str) -> dict[str, str]:
    return {"source": source, "target": target}


def _build_output_config(name: str, selector: list[str]) -> dict[str, Any]:
    return {"variable": name, "value_selector": selector}


def _build_iteration_graph_config() -> dict[str, Any]:
    return {
        "nodes": [
            _build_node_config("start", "start"),
            _build_node_config("main-ready", _WaitNode.node_type),
            _build_node_config(
                "iteration",
                "iteration",
                start_node_id="iteration-start",
                iterator_selector=["start", "items"],
                output_selector=["session", "status"],
                flatten_output=False,
            ),
            _build_node_config(
                "iteration-start", "iteration-start", container_id="iteration"
            ),
            _build_node_config(
                "assign",
                "assigner",
                version="2",
                container_id="iteration",
                items=[
                    {
                        "variable_selector": selector,
                        "input_type": "constant",
                        "operation": "over-write",
                        "value": value,
                    }
                    for selector, value in (
                        (["session", "status"], "done"),
                        (["conversation", "status"], "done"),
                        (["start", "counter"], 2),
                    )
                ],
            ),
            _build_node_config(
                "end",
                "end",
                outputs=[
                    _build_output_config("status", ["session", "status"]),
                    _build_output_config("conversation", ["conversation", "status"]),
                    _build_output_config("counter", ["start", "counter"]),
                    _build_output_config("inside", ["iteration", "output"]),
                ],
            ),
        ],
        "edges": [
            _build_edge_config("start", "main-ready"),
            _build_edge_config("main-ready", "iteration"),
            _build_edge_config("iteration-start", "assign"),
            _build_edge_config("iteration", "end"),
        ],
    }


class _WorkflowHandler:
    node_type = _WorkflowNode.node_type

    def __init__(
        self,
        frames: FrameRegistry,
        *,
        signals: Mapping[str, Event],
        inherit_pool: bool,
        serialize_pool: bool,
        sibling_state: str,
        sibling_node_id: str,
    ) -> None:
        self.frames = frames
        self.signals = signals
        self.inherit_pool = inherit_pool
        self.serialize_pool = serialize_pool
        self.sibling_state = sibling_state
        self.sibling_node_id = sibling_node_id

    def handle_request(
        self, *, invocation_id: str, request: ContainerAwaitRequest
    ) -> None:
        assert isinstance(request, CustomContainerRequest)
        root_state = self.frames["root"].state
        run_state = root_state.get_container_run(invocation_id)
        parent_frame = self.frames[run_state.frame_id]
        is_main = run_state.node_id == "main-tool"
        pool = (
            parent_frame.state.variable_pool.model_copy(deep=True)
            if self.inherit_pool
            else VariablePool()
        )
        if is_main:
            if pool.get_variable(["session", "status"]) is None:
                pool.add(["session", "status"], "pending")
            pool.add(["conversation", "status"], "pending")
            pool.add(["start", "items"], ["item"])
            pool.add(["start", "counter"], 1)
        if self.serialize_pool:
            pool = VariablePool.model_validate_json(pool.model_dump_json())
        state = RuntimeState(
            variable_pool=pool,
            start_at=parent_frame.state.start_at,
            ready_queue=parent_frame.state.ready_queue,
            deferred_ready_queue=parent_frame.state.deferred_ready_queue,
            graph_execution=parent_frame.state.graph_execution,
        )
        graph_config = (
            _build_iteration_graph_config()
            if is_main
            else {
                "nodes": [
                    _build_node_config("start", "start"),
                    _build_node_config(self.sibling_node_id, _WaitNode.node_type),
                ],
                "edges": [_build_edge_config("start", self.sibling_node_id)],
            }
        )
        graph = Graph.init(
            graph_config=graph_config,
            node_factory=_NodeFactory(state, self.signals),
            root_node_id="start",
        )
        frame = self.frames.create(
            frame_id=f"{invocation_id}:workflow",
            container_id=run_state.node_id,
            graph=graph,
            state=state,
        )
        root_state.put_container_frame(
            CustomContainerFrameState(
                frame_id=frame.frame_id,
                parent_invocation_id=invocation_id,
                runtime_data=state.snapshot_frame(copy_variable_pool=False),
            )
        )
        if not is_main and self.sibling_state == "running":
            self.signals["main-ready"].set()
        frame.scheduler.enqueue_node(graph.root_node.id)

    def prepare_frame_event(self, *, frame: ExecutionFrame, event: NodeEvent) -> None:
        _ = frame
        if isinstance(event, NodeRunVariableUpdatedEvent):
            self.signals[self.sibling_node_id].set()

    def should_emit(self, *, event: NodeEvent) -> bool:
        _ = event
        return True

    def complete_frame_if_ready(self, frame: ExecutionFrame) -> None:
        if not frame.scheduler.is_execution_complete():
            return
        root_state = self.frames["root"].state
        frame_state = root_state.get_container_frame(frame.frame_id)
        run_state = root_state.get_container_run(frame_state.parent_invocation_id)
        result = ContainerExecutionResult(
            metadata={},
            steps=frame.state.node_run_steps,
            node_run_result=ContainerNodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={
                    key: build_container_value(value)
                    for key, value in frame.state.outputs.items()
                },
            ),
        )
        root_state.enqueue_ready_task(
            ResumeTask(invocation_id=run_state.invocation_id, result=result)
        )
        root_state.pop_container_frame(frame.frame_id)
        self.frames.remove(frame.frame_id)
        if run_state.node_id == "sibling-tool" and self.sibling_state == "finished":
            self.signals["main-ready"].set()

    def record_frame_failure(
        self, *, frame: ExecutionFrame, event: NodeRunFailedEvent
    ) -> None:
        frame.state.graph_execution.fail(RuntimeError(event.error))

    def restore_frame(self, frame_state: ContainerFrameState) -> None:
        _ = frame_state
        msg = "This workflow does not pause"
        raise AssertionError(msg)


def _run_workflow(
    *,
    root_graph_kind: Literal["builder", "config"] = "config",
    root_node_id: str = "source",
    inherit_pool: bool = False,
    serialize_pool: bool = False,
    sibling_state: Literal["absent", "running", "finished"] = "absent",
    sibling_node_id: str = "session",
) -> dict[str, object]:
    signals = {"main-ready": Event(), sibling_node_id: Event()}
    if sibling_state == "absent":
        signals["main-ready"].set()
    if sibling_state == "finished":
        signals[sibling_node_id].set()
    pool = VariablePool()
    pool.add([root_node_id, "status"], "pending")
    state = RuntimeState(workflow_id="scope-test", variable_pool=pool, start_at=1)
    node_configs = [
        _build_node_config(root_node_id, "start"),
        _build_node_config("main-tool", _WorkflowNode.node_type),
        _build_node_config(
            "end",
            "end",
            outputs=[
                _build_output_config(name, ["main-tool", name])
                for name in ("status", "conversation", "counter", "inside")
            ],
        ),
    ]
    edge_configs = [
        _build_edge_config(root_node_id, "main-tool"),
        _build_edge_config("main-tool", "end"),
    ]
    if sibling_state != "absent":
        node_configs.append(_build_node_config("sibling-tool", _WorkflowNode.node_type))
        edge_configs.extend([
            _build_edge_config(root_node_id, "sibling-tool"),
            _build_edge_config("sibling-tool", "end"),
        ])
    factory = _NodeFactory(state, signals)
    if root_graph_kind == "builder":
        start, tool, end = [
            factory.create_node(NodeConfigDictAdapter.validate_python(node_config))
            for node_config in node_configs
        ]
        graph = Graph.new().add_root(start).add_node(tool).add_node(end).build()
    else:
        graph = Graph.init(
            graph_config={"nodes": node_configs, "edges": edge_configs},
            node_factory=factory,
            root_node_id=root_node_id,
        )
    engine = Engine(
        graph,
        state,
        workers=3,
        container_handler_factories=[
            lambda frames: _WorkflowHandler(
                frames,
                signals=signals,
                inherit_pool=inherit_pool,
                serialize_pool=serialize_pool,
                sibling_state=sibling_state,
                sibling_node_id=sibling_node_id,
            )
        ],
    )
    outputs = final_outputs(list(engine.run()))
    assert outputs["inside"] == ["done"]
    assert outputs["counter"] == 1
    assert outputs["conversation"] == "done"
    return outputs


@pytest.mark.parametrize("root_graph_kind", ["builder", "config"])
@pytest.mark.parametrize("serialize_pool", [False, True])
def test_iteration_preserves_inherited_node_variables(
    root_graph_kind: Literal["builder", "config"], serialize_pool: bool
) -> None:
    outputs = _run_workflow(
        root_graph_kind=root_graph_kind,
        root_node_id="session",
        inherit_pool=True,
        serialize_pool=serialize_pool,
    )
    assert outputs["status"] == "pending"


@pytest.mark.parametrize("serialize_pool", [False, True])
def test_independent_workflow_host_variable_can_match_ancestor_node(
    serialize_pool: bool,
) -> None:
    outputs = _run_workflow(root_node_id="session", serialize_pool=serialize_pool)
    assert outputs["status"] == "done"


@pytest.mark.parametrize(
    ("sibling_state", "sibling_node_id"),
    [
        ("absent", "session"),
        ("running", "session"),
        ("running", "other"),
        ("finished", "session"),
    ],
)
def test_sibling_workflow_does_not_change_host_variable_writes(
    sibling_state: Literal["absent", "running", "finished"], sibling_node_id: str
) -> None:
    outputs = _run_workflow(
        sibling_state=sibling_state, sibling_node_id=sibling_node_id
    )
    assert outputs["status"] == "done"
