from __future__ import annotations

import queue
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Event
from time import monotonic, sleep
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock, call

import yaml

from graphon.dsl import inspect
from graphon.dsl.entities import DslCredentials
from graphon.dsl.node_factory import SlimDslNodeFactory
from graphon.entities.graph_config import NodeConfigDict
from graphon.entities.graph_init_params import GraphInitParams
from graphon.entities.pause_reason import HitlRequired
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    NodeState,
    WorkflowNodeExecutionMetadataKey,
)
from graphon.graph.graph import Graph
from graphon.graph_engine.command_channels.in_memory_channel import InMemoryChannel
from graphon.graph_engine.config import GraphEngineConfig
from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_engine.frames import ExecutionFrame, FrameRegistry
from graphon.graph_engine.graph_engine import GraphEngine
from graphon.graph_engine.graph_state_manager import GraphStateManager
from graphon.graph_engine.loop_container_handler import LoopContainerHandler
from graphon.graph_engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.graph_engine.ready_queue.protocol import ResumeTask, StartTask
from graphon.graph_engine.worker import Worker
from graphon.graph_events.base import GraphEngineEvent
from graphon.graph_events.graph import GraphRunPausedEvent, GraphRunStartedEvent
from graphon.graph_events.iteration import (
    NodeRunIterationStartedEvent,
    NodeRunIterationSucceededEvent,
)
from graphon.graph_events.loop import (
    NodeRunLoopStartedEvent,
    NodeRunLoopSucceededEvent,
)
from graphon.graph_events.node import (
    NodeRunPauseRequestedEvent,
    NodeRunSucceededEvent,
)
from graphon.nodes.base.node import Node
from graphon.nodes.container_effects import IterationFrameRequest, LoopFrameRequest
from graphon.nodes.human_input.entities import (
    Completed,
    HITLCallback,
    HITLContext,
    PauseRequested,
)
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.nodes.loop.loop_node import LoopNode
from graphon.runtime.container_state import (
    ContainerFrameState,
    ContainerRunState,
    FrameRuntimeData,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import StringSegment
from tests.helpers.workflow_events import final_outputs


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
                },
            },
            {
                "id": "loop-start",
                "data": {"type": "loop-start", "loop_id": "loop"},
            },
            {
                "id": "human-input",
                "data": {
                    "type": "human-input",
                    "title": "Approve loop round",
                    "loop_id": "loop",
                },
            },
            _end_node([
                {"variable": "rounds", "value_selector": ["loop", "loop_round"]},
                {"variable": "seed", "value_selector": ["loop", "seed"]},
            ]),
        ],
        edges=[
            _edge("start", "loop"),
            _edge("loop-start", "human-input"),
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
                    "output_selector": ["human-input", "answer"],
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
                "id": "human-input",
                "data": {
                    "type": "human-input",
                    "title": "Approve iteration item",
                    "iteration_id": "iteration",
                },
            },
            _end_node([
                {"variable": "items", "value_selector": ["iteration", "output"]},
            ]),
        ],
        edges=[
            _edge("start", "iteration"),
            _edge("iteration-start", "human-input"),
            _edge("iteration", "end"),
        ],
    )


@dataclass(frozen=True, slots=True)
class _HitlNodeFactory:
    base_factory: SlimDslNodeFactory
    callback: HITLCallback

    def with_runtime_state(
        self,
        graph_runtime_state: GraphRuntimeState,
    ) -> _HitlNodeFactory:
        return _HitlNodeFactory(
            base_factory=self.base_factory.with_runtime_state(graph_runtime_state),
            callback=self.callback,
        )

    def create_node(self, node_config: NodeConfigDict) -> Node:
        if node_config["data"].type != BuiltinNodeTypes.HUMAN_INPUT:
            return self.base_factory.create_node(node_config)
        return HumanInputNode(
            node_id=node_config["id"],
            data=HumanInputNode.validate_node_data(node_config["data"]),
            graph_init_params=self.base_factory.graph_init_params,
            graph_runtime_state=self.base_factory.graph_runtime_state,
            hitl_callback=self.callback,
        )


def _new_runtime_state(start_inputs: Mapping[str, object]) -> GraphRuntimeState:
    variable_pool = VariablePool()
    variable_pool.add(("sys", "workflow_execution_id"), "workflow-execution")
    for key, value in start_inputs.items():
        variable_pool.add(("start", key), value)
        variable_pool.add(("sys", key), value)
    return GraphRuntimeState(variable_pool=variable_pool, start_at=0)


def _hitl_engine(
    dsl: str,
    *,
    runtime_state: GraphRuntimeState,
    callback: HITLCallback,
) -> GraphEngine:
    plan = inspect(dsl)
    graph_config = plan.document.graph_config
    if graph_config is None:
        msg = "test DSL must contain a graph"
        raise AssertionError(msg)
    graph_init_params = GraphInitParams(
        workflow_id="workflow",
        graph_config=graph_config,
        run_context={"workflow_execution_id": "workflow-execution"},
        call_depth=0,
    )
    base_factory = SlimDslNodeFactory(
        graph_config=graph_config,
        graph_init_params=graph_init_params,
        graph_runtime_state=runtime_state,
        credentials=DslCredentials(),
        dependencies=list(plan.dependencies),
    )
    graph = Graph.init(
        graph_config=graph_config,
        node_factory=_HitlNodeFactory(
            base_factory=base_factory,
            callback=callback,
        ),
        root_node_id="start",
    )
    return GraphEngine(
        workflow_id="workflow",
        graph=graph,
        graph_runtime_state=runtime_state,
        command_channel=InMemoryChannel(),
        config=GraphEngineConfig(min_workers=2, max_workers=2),
    )


def _snapshot_after_hitl_pause(
    engine: GraphEngine,
) -> tuple[str, list[GraphEngineEvent]]:
    events = list(engine.run())
    assert any(isinstance(event, NodeRunPauseRequestedEvent) for event in events)
    assert any(isinstance(event, GraphRunPausedEvent) for event in events)
    return engine.graph_runtime_state.dumps(), events


def _completed_hitl(answer: str) -> Completed:
    return Completed(
        selected_handle="source",
        inputs={},
        outputs={"answer": StringSegment(value=answer)},
    )


def _complete_loop_hitl(context: HITLContext) -> Completed:
    _ = context
    return _completed_hitl("approved")


def _complete_iteration_hitl(context: HITLContext) -> Completed:
    item = context.variable_pool.get(("iteration", "item"))
    assert item is not None
    return _completed_hitl(f"{item.text}!")


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
        state_manager=GraphStateManager(graph, graph_runtime_state, frame_id),
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
    })
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
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=graph,
            graph_runtime_state=graph_runtime_state,
        ),
    )
    request = LoopFrameRequest(
        inputs={"loop_count": 1},
        outputs={},
        loop_count=1,
        root_node_id="loop-start",
        loop_variable_selectors={},
        loop_node_ids=frozenset(),
        index=0,
    )
    graph_runtime_state.put_container_run(
        ContainerRunState(
            invocation_id="loop-invocation",
            frame_id="root",
            node_id="loop",
            started_at=datetime.now(UTC).replace(tzinfo=None),
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
    )
    loop_handler.start_await(
        invocation_id="loop-invocation",
        request=request,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="loop-invocation:loop:0",
        node_id="loop-start",
    )
    child_frame = frame_registry.get("loop-invocation:loop:0")
    child_frame.state_manager.finish_execution("loop-start")

    loop_handler.complete_frame(child_frame)
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


def test_resume_restores_container_runs_before_workers_start() -> None:
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
    )
    runtime_state.put_container_run(
        ContainerRunState(
            invocation_id="loop-invocation",
            frame_id="root",
            node_id="loop",
            started_at=datetime.now(UTC).replace(tzinfo=None),
            phase_data={},
        ),
    )
    runtime_state.defer_ready_task(StartTask(frame_id="root", node_id="start"))
    state_manager = MagicMock()
    worker_pool = MagicMock()

    def assert_replay_before_workers_start() -> None:
        assert runtime_state.ready_queue.qsize() == 1
        assert state_manager.track_unfinished.call_args_list == [
            call("loop"),
            call("start"),
        ]

    worker_pool.start.side_effect = assert_replay_before_workers_start
    frame_registry = MagicMock()
    frame_registry.get.return_value.state_manager = state_manager
    engine = object.__new__(GraphEngine)
    engine._graph_runtime_state = runtime_state
    engine._frame_registry = frame_registry
    engine._worker_pool = worker_pool
    engine._dispatcher = MagicMock()

    engine._start_execution(resume=True)

    worker_pool.start.assert_called_once_with()
    engine._dispatcher.start.assert_called_once_with()


def test_loop_frame_restore_shares_parent_variable_pool() -> None:
    parent_pool = VariablePool()
    parent_pool.add(["loop", "seed"], "parent")
    runtime_state = GraphRuntimeState(variable_pool=parent_pool, start_at=1)
    run_state = ContainerRunState(
        invocation_id="loop-invocation",
        frame_id="root",
        node_id="loop",
        started_at=datetime.now(UTC).replace(tzinfo=None),
    )
    runtime_state.put_container_run(run_state)
    frame_state = ContainerFrameState(
        frame_id="loop-invocation:loop:0",
        kind="loop",
        parent_invocation_id=run_state.invocation_id,
        root_node_id="loop-start",
        index=0,
        started_at=datetime.now(UTC).replace(tzinfo=None),
        runtime_data=FrameRuntimeData(
            variable_pool="parent",
            outputs={},
            llm_usage=runtime_state.llm_usage,
            node_run_steps=0,
            graph_node_states={"loop-start": NodeState.UNKNOWN},
            graph_edge_states={},
        ),
    )
    runtime_state.put_container_frame(frame_state)

    restored_state = GraphRuntimeState.from_snapshot(runtime_state.dumps())
    restored_frame_state = restored_state.get_container_frame(frame_state.frame_id)
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=_loop_graph(restored_state),
            graph_runtime_state=restored_state,
        ),
    )

    LoopContainerHandler(frame_registry=frame_registry).restore_frame(
        restored_frame_state,
    )

    restored_pool = frame_registry.get(
        restored_frame_state.frame_id,
    ).graph_runtime_state.variable_pool
    assert restored_frame_state.runtime_data.variable_pool == "parent"
    assert restored_pool is restored_state.variable_pool
    restored_seed = restored_pool.get(["loop", "seed"])
    assert restored_seed is not None
    assert restored_seed.to_object() == "parent"


def test_loop_hitl_runtime_state_round_trip_preserves_progress() -> None:
    completed_rounds = 0

    def pause_after_one_round(context: HITLContext) -> Completed | PauseRequested:
        nonlocal completed_rounds
        if completed_rounds == 0:
            completed_rounds += 1
            return _complete_loop_hitl(context)
        return PauseRequested(session_id="session-human-input")

    snapshot, paused_events = _snapshot_after_hitl_pause(
        _hitl_engine(
            _loop_dsl(),
            runtime_state=_new_runtime_state({}),
            callback=pause_after_one_round,
        )
    )
    paused_state = GraphRuntimeState.from_snapshot(snapshot)
    run_state = paused_state.container_runs()[0]
    frame_state = paused_state.container_frames()[0]
    deferred_tasks = paused_state.drain_deferred_ready_tasks()
    resumed_events = list(
        _hitl_engine(
            _loop_dsl(),
            runtime_state=GraphRuntimeState.from_snapshot(snapshot),
            callback=_complete_loop_hitl,
        ).run()
    )
    pause_requests = [
        event
        for event in paused_events
        if isinstance(event, NodeRunPauseRequestedEvent)
    ]
    assert len(pause_requests) == 1
    assert pause_requests[0].in_loop_id == "loop"
    assert pause_requests[0].reason == HitlRequired(
        session_id="session-human-input",
        node_id="human-input",
        node_title="Approve loop round",
    )
    paused_successes = [
        event
        for event in paused_events
        if isinstance(event, NodeRunSucceededEvent)
        and event.node_id == "human-input"
        and event.in_loop_id == "loop"
    ]
    resumed_successes = [
        event
        for event in resumed_events
        if isinstance(event, NodeRunSucceededEvent)
        and event.node_id == "human-input"
        and event.in_loop_id == "loop"
    ]
    loop_started = next(
        event for event in paused_events if isinstance(event, NodeRunLoopStartedEvent)
    )
    loop_succeeded = next(
        event
        for event in resumed_events
        if isinstance(event, NodeRunLoopSucceededEvent)
    )
    assert completed_rounds == 1
    assert run_state.phase_data["completed_count"] == 1
    assert run_state.phase_data["outputs"] == {"seed": "fixed", "loop_round": 1}
    assert frame_state.index == 1
    assert deferred_tasks == [
        StartTask(frame_id=frame_state.frame_id, node_id="human-input")
    ]
    assert [
        event.node_run_result.metadata[WorkflowNodeExecutionMetadataKey.LOOP_INDEX]
        for event in paused_successes + resumed_successes
    ] == [0, 1, 2]
    assert len(paused_successes) == 1
    assert len(resumed_successes) == 2
    assert loop_started.id == loop_succeeded.id
    assert not any(
        isinstance(event, NodeRunLoopStartedEvent) for event in resumed_events
    )
    assert final_outputs(resumed_events) == {"rounds": 3, "seed": "fixed"}


def test_paused_engine_can_resume_same_instance() -> None:
    callback_count = 0

    def pause_second_round(context: HITLContext) -> Completed | PauseRequested:
        nonlocal callback_count
        callback_count += 1
        if callback_count == 2:
            return PauseRequested(session_id="session-second-round")
        return _complete_loop_hitl(context)

    engine = _hitl_engine(
        _loop_dsl(),
        runtime_state=_new_runtime_state({}),
        callback=pause_second_round,
    )

    paused_events = list(engine.run())
    resumed_events = list(engine.run())

    assert isinstance(paused_events[-1], GraphRunPausedEvent)
    assert isinstance(resumed_events[0], GraphRunStartedEvent)
    assert resumed_events[0].reason == WorkflowStartReason.RESUMPTION
    assert not any(
        isinstance(event, NodeRunPauseRequestedEvent) for event in resumed_events
    )
    assert final_outputs(resumed_events) == {"rounds": 3, "seed": "fixed"}
    assert callback_count == 4
    assert engine.graph_runtime_state.container_runs() == ()
    assert engine.graph_runtime_state.container_frames() == ()


def test_parallel_iteration_hitl_runtime_state_round_trip_preserves_order() -> None:
    start_inputs = {"items": ["alpha", "beta", "gamma"]}
    runtime_state = _new_runtime_state(start_inputs)
    alpha_started = Event()

    def pause_with_active_sibling(
        context: HITLContext,
    ) -> Completed | PauseRequested:
        item = context.variable_pool.get(("iteration", "item"))
        assert item is not None
        if item.text == "beta":
            assert alpha_started.wait(timeout=1)
            return PauseRequested(session_id="session-beta")
        assert item.text == "alpha"
        alpha_started.set()
        deadline = monotonic() + 1
        while not runtime_state.graph_execution.paused:
            assert monotonic() < deadline
            sleep(0.001)
        return _completed_hitl("alpha!")

    snapshot, paused_events = _snapshot_after_hitl_pause(
        _hitl_engine(
            _iteration_dsl(),
            runtime_state=runtime_state,
            callback=pause_with_active_sibling,
        )
    )
    paused_state = GraphRuntimeState.from_snapshot(snapshot)
    run_state = paused_state.container_runs()[0]
    frame_state = paused_state.container_frames()[0]
    deferred_tasks = paused_state.drain_deferred_ready_tasks()
    resumed_events = list(
        _hitl_engine(
            _iteration_dsl(),
            runtime_state=GraphRuntimeState.from_snapshot(snapshot),
            callback=_complete_iteration_hitl,
        ).run()
    )
    pause_requests = [
        event
        for event in paused_events
        if isinstance(event, NodeRunPauseRequestedEvent)
    ]
    paused_successes = [
        event
        for event in paused_events
        if isinstance(event, NodeRunSucceededEvent)
        and event.node_id == "human-input"
        and event.in_iteration_id == "iteration"
    ]
    resumed_successes = [
        event
        for event in resumed_events
        if isinstance(event, NodeRunSucceededEvent)
        and event.node_id == "human-input"
        and event.in_iteration_id == "iteration"
    ]
    start_tasks = [task for task in deferred_tasks if isinstance(task, StartTask)]
    resume_tasks = [task for task in deferred_tasks if isinstance(task, ResumeTask)]
    assert len(pause_requests) == 1
    assert pause_requests[0].in_iteration_id == "iteration"
    assert pause_requests[0].reason == HitlRequired(
        session_id="session-beta",
        node_id="human-input",
        node_title="Approve iteration item",
    )
    assert paused_events.index(pause_requests[0]) < paused_events.index(
        paused_successes[0]
    )
    assert run_state.phase_data["scheduled_count"] == 2
    assert run_state.phase_data["completed_count"] == 1
    assert run_state.phase_data["outputs"] == {"0": "alpha!"}
    assert frame_state.index == 1
    assert len(start_tasks) == 1
    assert start_tasks[0] == StartTask(
        frame_id=frame_state.frame_id,
        node_id="human-input",
    )
    assert len(resume_tasks) == 1
    assert isinstance(resume_tasks[0].result, IterationFrameRequest)
    assert resume_tasks[0].result.indexes == (2,)
    assert [
        event.node_run_result.metadata[WorkflowNodeExecutionMetadataKey.ITERATION_INDEX]
        for event in paused_successes
    ] == [0]
    assert sorted(
        event.node_run_result.metadata[WorkflowNodeExecutionMetadataKey.ITERATION_INDEX]
        for event in resumed_successes
    ) == [1, 2]
    assert next(
        event.id
        for event in paused_events
        if isinstance(event, NodeRunIterationStartedEvent)
    ) == next(
        event.id
        for event in resumed_events
        if isinstance(event, NodeRunIterationSucceededEvent)
    )
    assert not any(
        isinstance(event, NodeRunIterationStartedEvent) for event in resumed_events
    )
    assert final_outputs(resumed_events) == {"items": ["alpha!", "beta!", "gamma!"]}


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
