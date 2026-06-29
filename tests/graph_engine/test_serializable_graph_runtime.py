from __future__ import annotations

import queue
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import yaml

from graphon.dsl import inspect, loads
from graphon.dsl.entities import DslCredentials
from graphon.dsl.node_factory import SlimDslNodeFactory
from graphon.entities.graph_init_params import GraphInitParams
from graphon.enums import BuiltinNodeTypes, NodeExecutionType, NodeState
from graphon.graph.graph import Graph
from graphon.graph_engine.command_channels.in_memory_channel import InMemoryChannel
from graphon.graph_engine.config import GraphEngineConfig
from graphon.graph_engine.entities.commands import PauseCommand
from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_engine.frames import ExecutionFrame, FrameRegistry
from graphon.graph_engine.graph_engine import GraphEngine
from graphon.graph_engine.graph_state_manager import GraphStateManager
from graphon.graph_engine.layers.base import GraphEngineLayer
from graphon.graph_engine.loop_container_handler import LoopContainerHandler
from graphon.graph_engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.graph_engine.ready_queue.protocol import ResumeTask, StartTask
from graphon.graph_engine.worker import Worker
from graphon.graph_events.base import GraphEngineEvent
from graphon.graph_events.graph import GraphRunPausedEvent, GraphRunSucceededEvent
from graphon.graph_events.loop import NodeRunLoopSucceededEvent
from graphon.graph_events.node import NodeRunSucceededEvent
from graphon.nodes.container_effects import LoopFrameRequest
from graphon.nodes.loop.loop_node import LoopNode
from graphon.runtime.container_state import ContainerRunState
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool


def _graph_dsl(
    *,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> str:
    return yaml.safe_dump({
        "kind": "graph",
        "graph": {
            "nodes": nodes,
            "edges": edges,
        },
    })


def _start_node() -> dict[str, Any]:
    return {"id": "start", "data": {"type": "start", "variables": []}}


def _end_node(outputs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"id": "end", "data": {"type": "end", "outputs": outputs}}


def _edge(source: str, target: str) -> dict[str, str]:
    return {"source": source, "target": target}


def _loop_dsl() -> str:
    return _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "loop",
                "data": {
                    "type": "loop",
                    "title": "Three rounds",
                    "loop_count": 3,
                    "start_node_id": "loop-start",
                    "break_conditions": [],
                    "logical_operator": "and",
                    "loop_variables": [
                        {
                            "label": "seed",
                            "var_type": "string",
                            "value_type": "constant",
                            "value": "fixed",
                        },
                    ],
                    "outputs": {},
                },
            },
            {
                "id": "loop-start",
                "data": {"type": "loop-start", "loop_id": "loop"},
            },
            {
                "id": "loop-answer",
                "data": {
                    "type": "answer",
                    "loop_id": "loop",
                    "answer": "{{#loop.seed#}}",
                },
            },
            _end_node([
                {"variable": "rounds", "value_selector": ["loop", "loop_round"]},
                {"variable": "seed", "value_selector": ["loop", "seed"]},
            ]),
        ],
        edges=[
            _edge("start", "loop"),
            _edge("loop-start", "loop-answer"),
            _edge("loop", "end"),
        ],
    )


def _iteration_dsl() -> str:
    return _graph_dsl(
        nodes=[
            _start_node(),
            {
                "id": "iteration",
                "data": {
                    "type": "iteration",
                    "title": "For each item",
                    "iterator_selector": ["start", "items"],
                    "output_selector": ["item-answer", "answer"],
                    "start_node_id": "iteration-start",
                    "is_parallel": True,
                    "parallel_nums": 2,
                    "error_handle_mode": "terminated",
                    "flatten_output": True,
                },
            },
            {
                "id": "iteration-start",
                "data": {"type": "iteration-start", "iteration_id": "iteration"},
            },
            {
                "id": "item-answer",
                "data": {
                    "type": "answer",
                    "iteration_id": "iteration",
                    "answer": "{{#iteration.item#}}!",
                },
            },
            _end_node([
                {"variable": "items", "value_selector": ["iteration", "output"]},
            ]),
        ],
        edges=[
            _edge("start", "iteration"),
            _edge("iteration-start", "item-answer"),
            _edge("iteration", "end"),
        ],
    )


def _load_engine(
    dsl: str,
    *,
    start_inputs: Mapping[str, Any] = {},
    runtime_state: GraphRuntimeState | None = None,
    command_channel: InMemoryChannel | None = None,
) -> GraphEngine:
    if runtime_state is None:
        return loads(
            dsl,
            workflow_id="workflow",
            start_inputs=dict(start_inputs),
            command_channel=command_channel or InMemoryChannel(),
            config=GraphEngineConfig(min_workers=1, max_workers=1),
        )

    plan = inspect(dsl)
    graph_config = plan.document.graph_config
    if graph_config is None:
        msg = "test DSL must contain a graph"
        raise AssertionError(msg)
    graph_init_params = GraphInitParams(
        workflow_id="workflow",
        graph_config=graph_config,
        run_context={},
        call_depth=0,
    )
    node_factory = SlimDslNodeFactory(
        graph_config=graph_config,
        graph_init_params=graph_init_params,
        graph_runtime_state=runtime_state,
        credentials=DslCredentials(),
        dependencies=list(plan.dependencies),
    )
    graph = Graph.init(
        graph_config=graph_config,
        node_factory=node_factory,
        root_node_id="start",
    )
    return GraphEngine(
        workflow_id="workflow",
        graph=graph,
        graph_runtime_state=runtime_state,
        command_channel=command_channel or InMemoryChannel(),
        config=GraphEngineConfig(min_workers=1, max_workers=1),
    )


def _final_outputs(events: Sequence[GraphEngineEvent]) -> dict[str, object]:
    for event in reversed(events):
        if isinstance(event, GraphRunSucceededEvent):
            return event.outputs
    msg = "graph did not succeed"
    raise AssertionError(msg)


def _pause_snapshot(
    dsl: str,
    *,
    start_inputs: Mapping[str, Any] = {},
    pause_after: Callable[[NodeRunSucceededEvent], bool],
) -> tuple[str, list[GraphEngineEvent]]:
    command_channel = InMemoryChannel()
    engine = _load_engine(
        dsl,
        start_inputs=start_inputs,
        command_channel=command_channel,
    )
    engine.layer(_PauseOnChildSuccess(command_channel, pause_after))
    events = []
    for event in engine.run():
        events.append(event)
        if isinstance(event, GraphRunPausedEvent):
            return engine.graph_runtime_state.dumps(), events
    msg = "graph did not pause"
    raise AssertionError(msg)


def _resume_from_snapshot(dsl: str, snapshot: str) -> list[GraphEngineEvent]:
    runtime_state = GraphRuntimeState.from_snapshot(snapshot)
    engine = _load_engine(dsl, runtime_state=runtime_state)
    return list(engine.run())


def _deferred_tasks(snapshot: str) -> list[object]:
    return GraphRuntimeState.from_snapshot(snapshot).drain_deferred_ready_tasks()


class _PauseOnChildSuccess(GraphEngineLayer):
    def __init__(
        self,
        command_channel: InMemoryChannel,
        pause_after: Callable[[NodeRunSucceededEvent], bool],
    ) -> None:
        super().__init__()
        self._pause_channel = command_channel
        self.pause_after = pause_after
        self.pause_sent = False

    def on_graph_start(self) -> None:
        return

    def on_event(self, event: GraphEngineEvent) -> None:
        if (
            not self.pause_sent
            and isinstance(event, NodeRunSucceededEvent)
            and self.pause_after(event)
        ):
            self._pause_channel.send_command(PauseCommand(reason="snapshot"))
            self.pause_sent = True

    def on_graph_end(self, error: Exception | None) -> None:
        _ = error


def _execution_frame(
    *,
    frame_id: str,
    graph: Graph,
    graph_runtime_state: GraphRuntimeState,
) -> ExecutionFrame:
    return ExecutionFrame(
        frame_id=frame_id,
        graph=graph,
        graph_runtime_state=graph_runtime_state,
        state_manager=GraphStateManager(graph, graph_runtime_state),
        edge_processor=cast(Any, SimpleNamespace()),
        error_handler=cast(Any, SimpleNamespace()),
    )


class _FrameFactory:
    def with_runtime_state(
        self,
        graph_runtime_state: GraphRuntimeState,
    ) -> _FrameFactory:
        _ = graph_runtime_state
        return self

    def create_node(self, node_config: dict[str, object]) -> object:
        node_data = cast(dict[str, object], node_config["data"])
        return SimpleNamespace(
            id=str(node_config["id"]),
            node_type=cast(BuiltinNodeTypes, node_data["type"]),
            execution_type=NodeExecutionType.EXECUTABLE,
            error_strategy=None,
            state=NodeState.UNKNOWN,
        )


class _GraphNode:
    id = "loop"
    node_type = BuiltinNodeTypes.LOOP
    execution_type = NodeExecutionType.CONTAINER
    error_strategy: ClassVar[None] = None
    state = NodeState.UNKNOWN


def _loop_graph(runtime_state: GraphRuntimeState) -> Graph:
    graph_config = {
        "nodes": [
            {"id": "loop-start", "data": {"type": BuiltinNodeTypes.LOOP_START}},
        ],
        "edges": [],
    }
    loop_node = LoopNode.__new__(LoopNode)
    loop_node.init_node_identity("loop")
    loop_node.init_node_data({
        "type": "loop",
        "title": "Loop",
        "loop_count": 1,
        "start_node_id": "loop-start",
        "break_conditions": [],
        "logical_operator": "and",
        "outputs": {},
    })
    loop_node.bind_execution_id("loop-run")
    loop_node.graph_runtime_state = runtime_state
    loop_node.graph_config = graph_config
    return cast(
        Graph,
        SimpleNamespace(
            nodes={"loop": loop_node},
            graph_config=graph_config,
            node_factory=_FrameFactory(),
            root_node=_GraphNode(),
        ),
    )


def _runtime_with_live_resume_task() -> GraphRuntimeState:
    ready_queue = InMemoryReadyQueue()
    graph_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
    )
    graph = _loop_graph(graph_runtime_state)
    graph_execution = graph_runtime_state.graph_execution
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=graph,
            graph_runtime_state=graph_runtime_state,
        ),
    )
    request = LoopFrameRequest(
        started_at=datetime.now(UTC).replace(tzinfo=None),
        inputs={"loop_count": 1},
        loop_count=1,
        root_node_id="loop-start",
        loop_variable_selectors={},
        loop_node_ids=frozenset(),
        index=0,
    )
    graph_runtime_state.put_container_run(
        ContainerRunState(
            invocation_id="loop-invocation",
            kind="loop",
            frame_id="root",
            node_id="loop",
            execution_id="loop-run",
            started_at=request.started_at,
            phase_data={
                "inputs": dict(request.inputs),
                "loop_count": request.loop_count,
                "root_node_id": request.root_node_id,
                "loop_variable_selectors": {},
                "loop_node_ids": (),
            },
        ),
    )
    loop_handler = LoopContainerHandler(
        frame_registry=frame_registry,
        graph_execution=graph_execution,
    )
    loop_handler.start_await(
        frame_id="root",
        node_id="loop",
        invocation_id="loop-invocation",
        request=request,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="loop-run:loop:0",
        node_id="loop-start",
    )
    child_frame = frame_registry.get("loop-run:loop:0")
    child_frame.state_manager.finish_execution(
        frame_id=child_frame.frame_id,
        node_id="loop-start",
    )

    assert loop_handler.complete_frame(child_frame) is True
    resume_task = ready_queue.get(timeout=0.01)
    assert isinstance(resume_task, ResumeTask)
    ready_queue.put(resume_task)
    return graph_runtime_state


def _resume_loop_snapshot(snapshot: str) -> list[TaskEvent]:
    runtime_state = GraphRuntimeState.from_snapshot(snapshot)
    runtime_state.graph_execution.paused = False
    for task in runtime_state.drain_deferred_ready_tasks():
        runtime_state.enqueue_ready_task(task)

    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=_loop_graph(runtime_state),
            graph_runtime_state=runtime_state,
        ),
    )
    event_queue: queue.Queue[TaskEvent] = queue.Queue()
    worker = Worker(
        ready_queue=cast(InMemoryReadyQueue, runtime_state.ready_queue),
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
        container_handlers={},
    )
    worker.start()
    try:
        return [event_queue.get(timeout=1), event_queue.get(timeout=1)]
    finally:
        worker.stop()
        worker.join(timeout=1)


def test_loop_graph_pause_snapshot_resume_outputs_match_uninterrupted_run() -> None:
    dsl = _loop_dsl()
    baseline_outputs = _final_outputs(list(_load_engine(dsl).run()))

    snapshot, paused_events = _pause_snapshot(
        dsl,
        pause_after=lambda event: (
            event.in_loop_id == "loop" and event.node_id == "loop-answer"
        ),
    )
    restored_outputs = _final_outputs(_resume_from_snapshot(dsl, snapshot))

    assert any(isinstance(event, GraphRunPausedEvent) for event in paused_events)
    assert restored_outputs == baseline_outputs
    assert restored_outputs["rounds"] == 3
    assert restored_outputs["seed"] == "fixed"


def test_parallel_iteration_pause_snapshot_resume_outputs_match_uninterrupted_run() -> (
    None
):
    dsl = _iteration_dsl()
    start_inputs = {"items": ["alpha", "beta", "gamma"]}
    baseline_outputs = _final_outputs(
        list(_load_engine(dsl, start_inputs=start_inputs).run())
    )

    snapshot, paused_events = _pause_snapshot(
        dsl,
        start_inputs=start_inputs,
        pause_after=lambda event: (
            event.in_iteration_id == "iteration" and event.node_id == "item-answer"
        ),
    )
    restored_for_assert = GraphRuntimeState.from_snapshot(snapshot)
    deferred_tasks = restored_for_assert.drain_deferred_ready_tasks()
    restored_outputs = _final_outputs(_resume_from_snapshot(dsl, snapshot))

    assert any(isinstance(event, GraphRunPausedEvent) for event in paused_events)
    assert restored_for_assert.ready_queue.qsize() == 0
    assert deferred_tasks
    assert restored_outputs == baseline_outputs
    assert restored_outputs["items"] == ["alpha!", "beta!", "gamma!"]


def test_deferred_resume_task_round_trips_and_resumes_parent_container() -> None:
    runtime_state = _runtime_with_live_resume_task()
    runtime_state.graph_execution.paused = True
    runtime_state.drain_ready_tasks_to_deferred()
    snapshot = runtime_state.dumps()
    restored_for_assert = GraphRuntimeState.from_snapshot(snapshot)
    deferred_tasks = restored_for_assert.drain_deferred_ready_tasks()

    task_events = _resume_loop_snapshot(snapshot)

    assert restored_for_assert.ready_queue.qsize() == 0
    assert any(isinstance(task, ResumeTask) for task in deferred_tasks)
    assert isinstance(task_events[0].event, NodeRunLoopSucceededEvent)
    assert isinstance(task_events[1].event, NodeRunSucceededEvent)
    assert task_events[1].event.node_id == "loop"
