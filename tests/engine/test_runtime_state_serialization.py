from __future__ import annotations

import json
import queue
from collections.abc import Callable, Generator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from threading import Event, Lock, Thread, current_thread
from time import monotonic, sleep
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest
import yaml

from graphon.dsl import inspect
from graphon.dsl.entities import DslCredentials
from graphon.dsl.node_factory import SlimDslNodeFactory
from graphon.engine import Engine
from graphon.engine.command import AbortCommand, InMemoryChannel
from graphon.engine.container_handler import LoopContainerHandler
from graphon.engine.frame import ExecutionFrame, FrameRegistry
from graphon.engine.layer import Layer
from graphon.engine.ready_queue.entities import ResumeTask, StartTask
from graphon.engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.engine.scheduler import Scheduler
from graphon.engine.worker import DispatchTask, NodeEventTask, Worker
from graphon.engine_events.base import EngineEvent, NodeEvent
from graphon.engine_events.graph import (
    GraphRunAbortedEvent,
    GraphRunPartialSucceededEvent,
    GraphRunPausedEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from graphon.engine_events.iteration import (
    NodeRunIterationStartedEvent,
    NodeRunIterationSucceededEvent,
)
from graphon.engine_events.loop import (
    NodeRunLoopStartedEvent,
    NodeRunLoopSucceededEvent,
)
from graphon.engine_events.node import (
    NodeRunFailedEvent,
    NodeRunPauseRequestedEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.entities.base_node_data import BaseNodeData
from graphon.entities.graph_config import NodeConfigDict
from graphon.entities.pause_reason import HitlRequired
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    NodeState,
    WorkflowNodeExecutionMetadataKey,
    WorkflowNodeExecutionStatus,
)
from graphon.graph.graph import Graph
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events import NodeRunResult, StreamCompletedEvent
from graphon.nodes.base.node import Node
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
    ContainerExecutionResult,
    ContainerNodeRunResult,
    ContainerRunResult,
    CustomContainerRequest,
    IterationFrameRequest,
    LoopFrameRequest,
    build_container_value,
)
from graphon.nodes.human_input.entities import (
    Completed,
    HITLCallback,
    HITLContext,
    PauseRequested,
)
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.nodes.loop.loop_node import LoopNode
from graphon.nodes.loop.loop_start_node import LoopStartNode
from graphon.nodes.start.entities import StartNodeData
from graphon.nodes.start.start_node import StartNode
from graphon.runtime.container_state import (
    ContainerFrameState,
    CustomContainerFrameState,
    CustomContainerRunState,
    FrameRuntimeData,
    IterationFrameState,
    IterationRunState,
    LoopFrameState,
    LoopRunState,
    create_container_run_state,
)
from graphon.runtime.init_params import InitParams
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import StringSegment
from tests.helpers.workflow_events import final_outputs

_V2_PAUSED_LOOP_SNAPSHOT = (
    Path(__file__).with_name("fixtures") / "runtime_state_v2_1d0f32c_paused_loop.json"
).read_text()


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


def _edge(source: str, target: str, *, edge_id: str | None = None) -> dict[str, str]:
    edge = {"source": source, "target": target}
    if edge_id is not None:
        edge["id"] = edge_id
    return edge


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
            _edge("start", "loop", edge_id="start-to-loop"),
            _edge(
                "loop-start",
                "human-input",
                edge_id="loop-start-to-human-input",
            ),
            _edge("loop", "end", edge_id="loop-to-end"),
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

    def with_graph_config(
        self,
        graph_config: Mapping[str, Any],
    ) -> _HitlNodeFactory:
        """Copy the wrapper while preserving the base factory's graph scope."""
        return _HitlNodeFactory(
            base_factory=self.base_factory.with_graph_config(graph_config),
            callback=self.callback,
        )

    def validate_node(self, node_config: NodeConfigDict) -> NodeExecutionType:
        """Validate built-ins and the wrapper's custom Human Input node."""
        if node_config["data"].type == BuiltinNodeTypes.HUMAN_INPUT:
            HumanInputNode.validate_node_data(node_config["data"])
            return HumanInputNode.execution_type
        return self.base_factory.validate_node(node_config)

    def with_runtime_state(
        self,
        runtime_state: RuntimeState,
    ) -> _HitlNodeFactory:
        return _HitlNodeFactory(
            base_factory=self.base_factory.with_runtime_state(runtime_state),
            callback=self.callback,
        )

    def create_node(self, node_config: NodeConfigDict) -> Node:
        if node_config["data"].type != BuiltinNodeTypes.HUMAN_INPUT:
            return self.base_factory.create_node(node_config)
        return HumanInputNode(
            node_id=node_config["id"],
            data=HumanInputNode.validate_node_data(node_config["data"]),
            init_params=self.base_factory.init_params,
            runtime_state=self.base_factory.runtime_state,
            hitl_callback=self.callback,
        )


def _new_runtime_state(start_inputs: Mapping[str, object]) -> RuntimeState:
    variable_pool = VariablePool()
    variable_pool.add(("sys", "workflow_execution_id"), "workflow-execution")
    for key, value in start_inputs.items():
        variable_pool.add(("start", key), value)
        variable_pool.add(("sys", key), value)
    return RuntimeState(workflow_id="workflow", variable_pool=variable_pool, start_at=0)


def _hitl_engine(
    dsl: str,
    *,
    runtime_state: RuntimeState,
    callback: HITLCallback,
    command_channel: InMemoryChannel | None = None,
) -> Engine:
    plan = inspect(dsl)
    graph_config = plan.document.graph_config
    if graph_config is None:
        msg = "test DSL must contain a graph"
        raise AssertionError(msg)
    init_params = InitParams(
        workflow_id="workflow",
        graph_config=graph_config,
        run_context={"workflow_execution_id": "workflow-execution"},
        call_depth=0,
    )
    base_factory = SlimDslNodeFactory(
        graph_config=graph_config,
        init_params=init_params,
        runtime_state=runtime_state,
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
    return Engine(
        graph=graph,
        runtime_state=runtime_state,
        workers=2,
        command_channel=command_channel,
    )


def _snapshot_after_hitl_pause(
    engine: Engine,
) -> tuple[str, list[EngineEvent]]:
    events = list(engine.run())
    assert any(isinstance(event, NodeRunPauseRequestedEvent) for event in events)
    assert any(isinstance(event, GraphRunPausedEvent) for event in events)
    return engine.runtime_state.dumps(), events


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


def _try_snapshot(dumps: Callable[[], str]) -> str | Exception:
    """Keep snapshot failures observable outside exception-isolating callbacks."""
    try:
        return dumps()
    except Exception as error:  # ruff: ignore[blind-except]
        return error


def test_runtime_snapshot_rejects_active_callbacks() -> None:
    state = _new_runtime_state({})
    snapshots: dict[str, str | Exception] = {}

    class SnapshotLayer(Layer):
        def on_graph_start(self) -> None:
            snapshots["graph_start"] = _try_snapshot(self.runtime_state.dumps)

        def on_event(self, event: EngineEvent) -> None:
            if isinstance(
                event,
                (GraphRunStartedEvent, NodeRunSucceededEvent, GraphRunSucceededEvent),
            ):
                snapshots[type(event).__name__] = _try_snapshot(
                    self.runtime_state.dumps
                )

        def on_node_run_start(self, node: Node) -> None:
            snapshots["node_start"] = _try_snapshot(self.runtime_state.dumps)
            snapshots["node_runtime"] = _try_snapshot(node.runtime_state.dumps)

        def on_graph_end(self, error: Exception | None) -> None:
            _ = error
            snapshots["graph_end"] = _try_snapshot(self.runtime_state.dumps)

    engine = _hitl_engine(
        _graph_dsl(nodes=[_start_node()], edges=[]),
        runtime_state=state,
        callback=_complete_loop_hitl,
    )
    engine.add_layer(SnapshotLayer())
    assert RuntimeState.from_snapshot(state.dumps()).dumps()
    events = engine.run()
    assert isinstance(next(events), GraphRunStartedEvent)
    snapshots["started_iterator"] = _try_snapshot(state.dumps)
    assert isinstance(list(events)[-1], GraphRunSucceededEvent)

    completed = state.dumps()
    assert RuntimeState.from_snapshot(completed).dumps()
    assert isinstance(snapshots.pop("graph_end"), str)
    assert set(snapshots) == {
        "graph_start",
        "GraphRunStartedEvent",
        "GraphRunSucceededEvent",
        "NodeRunSucceededEvent",
        "node_start",
        "node_runtime",
        "started_iterator",
    }
    assert all(isinstance(result, RuntimeError) for result in snapshots.values()), {
        name: type(result).__name__ for name, result in snapshots.items()
    }


def test_runtime_snapshot_rejects_paused_workers_and_child_frames() -> None:
    state = _new_runtime_state({"items": ["alpha", "beta"]})
    alpha_started = Event()
    child_states: list[RuntimeState] = []
    snapshots: list[str | Exception] = []

    class ChildStateLayer(Layer):
        def on_node_run_start(self, node: Node) -> None:
            if node.id == "human-input":
                item = node.runtime_state.variable_pool.get(("iteration", "item"))
                if item is not None and item.text == "alpha":
                    child_states.append(node.runtime_state)

    layer = ChildStateLayer()

    def pause_with_active_sibling(context: HITLContext) -> Completed | PauseRequested:
        item = context.variable_pool.get(("iteration", "item"))
        assert item is not None
        if item.text == "beta":
            assert alpha_started.wait(timeout=2)
            return PauseRequested(session_id="session-beta")
        alpha_started.set()
        deadline = monotonic() + 2
        while not state.graph_execution.paused:
            assert monotonic() < deadline
            sleep(0.001)
        snapshots.extend([
            _try_snapshot(layer.runtime_state.dumps),
            _try_snapshot(child_states[0].dumps),
        ])
        return _completed_hitl("alpha!")

    engine = _hitl_engine(
        _iteration_dsl(), runtime_state=state, callback=pause_with_active_sibling
    )
    engine.add_layer(layer)
    snapshot, _ = _snapshot_after_hitl_pause(engine)
    restored = RuntimeState.from_snapshot(snapshot)
    assert restored.dumps()
    resumed = list(
        _hitl_engine(
            _iteration_dsl(),
            runtime_state=restored,
            callback=_complete_iteration_hitl,
        ).run()
    )

    assert child_states[0] is not state
    assert final_outputs(resumed) == {"items": ["alpha!", "beta!"]}
    assert len(snapshots) == 2
    assert all(isinstance(result, RuntimeError) for result in snapshots), [
        type(result).__name__ for result in snapshots
    ]


@pytest.mark.parametrize("thread_kind", ["worker", "dispatcher"])
def test_runtime_snapshot_rejects_thread_alive_after_iterator_close(  # ruff: ignore[complex-structure]
    thread_kind: str,
) -> None:
    state = _new_runtime_state({})
    thread_blocked = Event()
    release_thread = Event()
    blocked_threads: list[Thread] = []
    worker_threads: set[Thread] = set()
    released: list[bool] = []

    def block_thread() -> None:
        blocked_threads.append(current_thread())
        thread_blocked.set()
        released.append(release_thread.wait(timeout=10))

    class BlockingLayer(Layer):
        def on_node_run_start(self, node: Node) -> None:
            _ = node
            worker_threads.add(current_thread())

        def on_event(self, event: EngineEvent) -> None:
            if (
                thread_kind == "dispatcher"
                and isinstance(event, NodeRunStartedEvent)
                and event.node_id == "human-input"
            ):
                block_thread()

    def wait_for_release(context: HITLContext) -> Completed:
        _ = context
        if thread_kind == "worker":
            block_thread()
        return _completed_hitl("done")

    engine = _hitl_engine(
        _graph_dsl(
            nodes=[
                _start_node(),
                {"id": "human-input", "data": {"type": "human-input"}},
                _end_node([]),
            ],
            edges=[_edge("start", "human-input"), _edge("human-input", "end")],
        ),
        runtime_state=state,
        callback=wait_for_release,
    )
    engine.add_layer(BlockingLayer())
    events = engine.run()
    try:
        for event in events:
            if (
                isinstance(event, NodeRunStartedEvent)
                and event.node_id == "human-input"
            ):
                break
        assert thread_blocked.wait(timeout=2)
        events.close()
        assert list(events) == []
        assert blocked_threads[0].is_alive()
        if thread_kind == "dispatcher":
            assert worker_threads
            assert all(not worker.is_alive() for worker in worker_threads)
        snapshot = _try_snapshot(state.dumps)
    finally:
        release_thread.set()
        events.close()
        for thread in {*worker_threads, *blocked_threads}:
            thread.join(timeout=2)

    assert released == [True]
    assert all(not thread.is_alive() for thread in {*worker_threads, *blocked_threads})
    assert state.dumps()
    assert isinstance(snapshot, RuntimeError), type(snapshot).__name__


@pytest.mark.parametrize("operation", ["run", "dump"])
def test_runtime_snapshot_excludes_concurrent_operations(
    operation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = _new_runtime_state({})
    snapshot_started = Event()
    release_snapshot = Event()
    operation_started = Event()
    overlap = Event()
    results: list[object] = []
    queue_dumps = state.ready_queue.dumps

    def hold_snapshot() -> str:
        if snapshot_started.is_set():
            if not release_snapshot.is_set():
                overlap.set()
        else:
            snapshot_started.set()
            assert release_snapshot.wait(timeout=2)
        return queue_dumps()

    class SnapshotOverlapLayer(Layer):
        def on_graph_start(self) -> None:
            if not release_snapshot.is_set():
                overlap.set()

    engine = _hitl_engine(
        _graph_dsl(nodes=[_start_node()], edges=[]),
        runtime_state=state,
        callback=_complete_loop_hitl,
    )
    engine.add_layer(SnapshotOverlapLayer())
    monkeypatch.setattr(state.ready_queue, "dumps", hold_snapshot)

    def run_operation() -> None:
        operation_started.set()
        if operation == "run":
            results.append(list(engine.run()))
        else:
            results.append(_try_snapshot(state.dumps))

    snapshot_thread = Thread(target=lambda: results.append(_try_snapshot(state.dumps)))
    competing_thread = Thread(target=run_operation)
    snapshot_thread.start()
    try:
        assert snapshot_started.wait(timeout=1)
        competing_thread.start()
        assert operation_started.wait(timeout=1)
        overlapped = overlap.wait(timeout=0.1)
    finally:
        release_snapshot.set()
        snapshot_thread.join(timeout=2)
        if competing_thread.ident is not None:
            competing_thread.join(timeout=2)

    assert not snapshot_thread.is_alive()
    assert not competing_thread.is_alive()
    assert len(results) == 2
    assert not any(isinstance(result, Exception) for result in results)
    assert not overlapped


def _execution_frame(
    *,
    frame_id: str,
    graph: Graph,
    runtime_state: RuntimeState,
) -> ExecutionFrame:
    return ExecutionFrame(
        frame_id=frame_id,
        graph=graph,
        state=runtime_state,
        scheduler=Scheduler(graph, runtime_state, frame_id),
        failure_handler=cast(Any, SimpleNamespace()),
    )


class _FrameFactory:
    def with_graph_config(
        self,
        graph_config: Mapping[str, Any],
    ) -> _FrameFactory:
        """Accept graph scoping for this stateless frame test factory."""
        _ = graph_config
        return self

    def validate_node(self, node_config: NodeConfigDict) -> NodeExecutionType:
        """Accept minimal frame configs and report executable semantics."""
        _ = node_config
        return NodeExecutionType.EXECUTABLE

    def with_runtime_state(
        self,
        runtime_state: RuntimeState,
    ) -> _FrameFactory:
        _ = runtime_state
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


def _loop_graph(runtime_state: RuntimeState) -> Graph:
    graph_config = {
        "nodes": [
            {
                "id": "loop-start",
                "data": {
                    "type": BuiltinNodeTypes.LOOP_START,
                    "container_id": "loop",
                },
            },
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
    loop_node.runtime_state = runtime_state
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


def _runtime_with_live_resume_task() -> RuntimeState:
    ready_queue = InMemoryReadyQueue()
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
    )
    graph = _loop_graph(runtime_state)
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=graph,
            runtime_state=runtime_state,
        ),
    )
    request = LoopFrameRequest(
        inputs={"loop_count": build_container_value(1)},
        outputs={},
        loop_count=1,
        root_node_id="loop-start",
        loop_variable_selectors={},
        loop_node_ids=frozenset(),
        index=0,
    )
    runtime_state.put_container_run(
        create_container_run_state(
            invocation_id="loop-invocation",
            frame_id="root",
            node_id="loop",
            started_at=datetime.now(UTC).replace(tzinfo=None),
            request=request,
        ),
    )
    loop_handler = LoopContainerHandler(
        frame_registry=frame_registry,
    )
    loop_handler.handle_request(
        invocation_id="loop-invocation",
        request=request,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="loop-invocation:loop:0",
        node_id="loop-start",
    )
    child_frame = frame_registry["loop-invocation:loop:0"]
    child_frame.scheduler.finish_execution("loop-start")

    loop_handler.complete_frame_if_ready(child_frame)
    resume_task = ready_queue.get(timeout=0.01)
    assert isinstance(resume_task, ResumeTask)
    ready_queue.put(resume_task)
    return runtime_state


def _resume_loop_snapshot(snapshot: str) -> list[NodeEventTask]:
    runtime_state = RuntimeState.from_snapshot(snapshot)
    runtime_state.graph_execution.paused = False
    for task in runtime_state.take_deferred_ready_tasks():
        runtime_state.enqueue_ready_task(task)

    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=_loop_graph(runtime_state),
            runtime_state=runtime_state,
        ),
    )
    dispatch_queue: queue.Queue[DispatchTask] = queue.Queue()
    task_acquisition_enabled = Event()
    task_acquisition_enabled.set()
    worker = Worker(
        ready_queue=cast(InMemoryReadyQueue, runtime_state.ready_queue),
        dispatch_queue=dispatch_queue,
        frame_registry=frame_registry,
        layers=[],
        task_acquisition_lock=Lock(),
        task_acquisition_enabled=task_acquisition_enabled,
    )
    worker.start()
    try:
        first_event = dispatch_queue.get(timeout=1)
        second_event = dispatch_queue.get(timeout=1)
        assert isinstance(first_event, NodeEventTask)
        assert isinstance(second_event, NodeEventTask)
        return [first_event, second_event]
    finally:
        worker.stop()
        worker.join(timeout=1)


def test_loop_frame_restore_copies_parent_variable_pool() -> None:
    parent_pool = VariablePool()
    parent_pool.add(["loop", "seed"], "parent")
    runtime_state = RuntimeState(
        workflow_id="workflow", variable_pool=parent_pool, start_at=1
    )
    request = LoopFrameRequest(
        inputs={"loop_count": build_container_value(1)},
        outputs={},
        loop_count=1,
        root_node_id="loop-start",
        loop_variable_selectors={},
        loop_node_ids=frozenset(),
        index=0,
    )
    run_state = create_container_run_state(
        invocation_id="loop-invocation",
        frame_id="root",
        node_id="loop",
        started_at=datetime.now(UTC).replace(tzinfo=None),
        request=request,
    )
    assert isinstance(run_state, LoopRunState)
    runtime_state.put_container_run(run_state)
    frame_state = LoopFrameState(
        frame_id="loop-invocation:loop:0",
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

    restored_state = RuntimeState.from_snapshot(runtime_state.dumps())
    restored_frame_state = restored_state.get_container_frame(frame_state.frame_id)
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=_loop_graph(restored_state),
            runtime_state=restored_state,
        ),
    )

    LoopContainerHandler(frame_registry=frame_registry).restore_frame(
        restored_frame_state,
    )

    restored_pool = frame_registry[restored_frame_state.frame_id].state.variable_pool
    restored_frame_state = restored_state.get_container_frame(frame_state.frame_id)
    assert not isinstance(restored_frame_state.runtime_data.variable_pool, str)
    assert restored_pool is not restored_state.variable_pool
    restored_seed = restored_pool.get(["loop", "seed"])
    assert restored_seed is not None
    assert restored_seed.to_object() == "parent"
    restored_pool.add(["loop", "seed"], "child")
    parent_seed = restored_state.variable_pool.get(["loop", "seed"])
    assert parent_seed is not None
    assert parent_seed.to_object() == "parent"


@pytest.mark.parametrize("outcome", ["paused", "resumed", "aborted", "stopped"])
def test_root_statistics_include_unfinished_child_work(
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    def run_with_usage(_node: LoopStartNode) -> NodeRunResult:
        return NodeRunResult(
            status="succeeded",
            llm_usage=LLMUsage.from_metadata({
                "total_tokens": 100,
                "total_price": "0.01",
            }),
        )

    monkeypatch.setattr(LoopStartNode, "_run", run_with_usage)
    channel = InMemoryChannel()
    approvals = 0

    def pause_second_round(context: HITLContext) -> Completed | PauseRequested:
        nonlocal approvals
        approvals += 1
        if approvals == 2:
            if outcome == "stopped":
                channel.send_command(AbortCommand())
            return PauseRequested(session_id="pending-approval")
        return _complete_loop_hitl(context)

    engine = _hitl_engine(
        _loop_dsl(),
        runtime_state=_new_runtime_state({}),
        callback=pause_second_round,
        command_channel=channel,
    )
    events = list(engine.run())
    state = engine.runtime_state
    if outcome in {"paused", "stopped"}:
        expected_event = (
            GraphRunPausedEvent if outcome == "paused" else GraphRunAbortedEvent
        )
        assert isinstance(events[-1], expected_event)
        assert (state.node_run_steps, state.total_tokens) == (6, 200)
        assert state.llm_usage.total_price == Decimal("0.02")
        return

    restored = RuntimeState.from_snapshot(state.dumps())
    assert (restored.node_run_steps, restored.total_tokens) == (6, 200)
    resume_channel = InMemoryChannel()

    def resume_approval(context: HITLContext) -> Completed | PauseRequested:
        if outcome == "aborted":
            resume_channel.send_command(AbortCommand())
            return PauseRequested(session_id="pending-approval")
        return _complete_loop_hitl(context)

    resumed_events = list(
        _hitl_engine(
            _loop_dsl(),
            runtime_state=restored,
            callback=resume_approval,
            command_channel=resume_channel,
        ).run()
    )
    if outcome == "aborted":
        assert isinstance(resumed_events[-1], GraphRunAbortedEvent)
        assert (restored.node_run_steps, restored.total_tokens) == (7, 200)
        assert restored.llm_usage.total_price == Decimal("0.02")
    else:
        assert isinstance(resumed_events[-1], GraphRunSucceededEvent)
        assert (restored.node_run_steps, restored.total_tokens) == (10, 300)
        assert restored.llm_usage.total_price == Decimal("0.03")
        completed = next(
            event
            for event in resumed_events
            if isinstance(event, NodeRunSucceededEvent) and event.node_id == "loop"
        )
        assert completed.node_run_result.llm_usage == restored.llm_usage


class _HostContainerNode(Node[BaseNodeData]):
    node_type = "statistics-host-container"
    execution_type = NodeExecutionType.CONTAINER

    @classmethod
    def version(cls) -> str:
        return "1"

    def _run(self) -> Generator[CustomContainerRequest, None, None]:
        yield CustomContainerRequest(payload="{}")

    def _resume_container_events(
        self, *, result: ContainerRunResult
    ) -> Generator[StreamCompletedEvent, None, None]:
        assert isinstance(result, ContainerExecutionResult)
        yield StreamCompletedEvent(
            node_run_result=result.node_run_result.to_node_run_result()
        )


class _HostContainerHandler:
    node_type = _HostContainerNode.node_type

    def __init__(self, frame_registry: FrameRegistry) -> None:
        self.frames = frame_registry
        self.state = frame_registry["root"].state

    def handle_request(
        self, *, invocation_id: str, request: ContainerAwaitRequest
    ) -> None:
        assert isinstance(request, CustomContainerRequest)
        child_state = RuntimeState(
            variable_pool=VariablePool(),
            start_at=0,
            graph_execution=self.state.graph_execution,
            ready_queue=self.state.ready_queue,
            deferred_ready_queue=self.state.deferred_ready_queue,
        )
        child_graph = _hitl_engine(
            _loop_dsl(),
            runtime_state=child_state,
            callback=lambda _: PauseRequested(session_id="host-child-approval"),
        ).graph
        child_frame = self.frames.create(
            frame_id="host-child",
            container_id="host",
            graph=child_graph,
            state=child_state,
        )
        self.state.put_container_frame(
            CustomContainerFrameState(
                frame_id=child_frame.frame_id,
                parent_invocation_id=invocation_id,
                runtime_data=child_state.snapshot_frame(),
            )
        )
        child_frame.scheduler.enqueue_node(child_graph.root_node.id)

    def restore_frame(self, frame_state: ContainerFrameState) -> None:
        raise AssertionError(frame_state)

    def prepare_frame_event(self, *, frame: ExecutionFrame, event: NodeEvent) -> None:
        pass

    def should_emit(self, *, event: NodeEvent) -> bool:
        _ = event
        return True

    def record_frame_failure(
        self, *, frame: ExecutionFrame, event: NodeRunFailedEvent
    ) -> None:
        raise AssertionError((frame, event))

    def complete_frame_if_ready(self, frame: ExecutionFrame) -> None:
        assert not frame.scheduler.is_execution_complete()


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("status", ["succeeded", "failed"])
def test_custom_container_counts_its_own_model_usage(
    monkeypatch: pytest.MonkeyPatch, resumed: bool, status: str
) -> None:
    result = ContainerNodeRunResult(
        status=WorkflowNodeExecutionStatus(status),
        error="model unavailable" if status == "failed" else "",
        llm_usage=LLMUsage.from_metadata({"total_tokens": 70}),
    )

    def return_result(
        handler: _HostContainerHandler,
        *,
        invocation_id: str,
        request: ContainerAwaitRequest,
    ) -> None:
        assert isinstance(request, CustomContainerRequest)
        handler.state.enqueue_ready_task(
            ResumeTask(
                invocation_id=invocation_id,
                result=ContainerExecutionResult(
                    metadata={}, steps=0, node_run_result=result
                ),
            )
        )

    if resumed:
        monkeypatch.setattr(_HostContainerHandler, "handle_request", return_result)
    else:
        monkeypatch.setattr(_HostContainerNode, "_run", lambda _: result)
    state = _new_runtime_state({})
    node = _HostContainerNode(
        node_id="host",
        data=BaseNodeData(
            type=_HostContainerNode.node_type,
            title="Host container",
            error_strategy="default-value",
        ),
        init_params=InitParams(
            workflow_id="workflow", graph_config={}, run_context={}, call_depth=0
        ),
        runtime_state=state,
    )
    start = StartNode(
        node_id="start",
        data=StartNodeData(title="Start", variables=[]),
        init_params=node.init_params,
        runtime_state=state,
    )
    events = list(
        Engine(
            graph=Graph.new().add_root(start).add_node(node).build(),
            runtime_state=state,
            workers=1,
            container_handler_factories=(_HostContainerHandler,),
        ).run()
    )
    expected_event = (
        GraphRunPartialSucceededEvent if status == "failed" else GraphRunSucceededEvent
    )
    assert isinstance(events[-1], expected_event)
    assert (state.node_run_steps, state.total_tokens) == (2, 70)


def test_root_statistics_include_loop_inside_host_created_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        LoopStartNode,
        "_run",
        lambda _: NodeRunResult(
            status=WorkflowNodeExecutionStatus.SUCCEEDED,
            llm_usage=LLMUsage.from_metadata({"total_tokens": 100}),
        ),
    )
    state = _new_runtime_state({})

    node = _HostContainerNode(
        node_id="host",
        data=BaseNodeData(type=_HostContainerNode.node_type, title="Host container"),
        init_params=InitParams(
            workflow_id="workflow", graph_config={}, run_context={}, call_depth=0
        ),
        runtime_state=state,
    )
    start = StartNode(
        node_id="root-start",
        data=StartNodeData(title="Start", variables=[]),
        init_params=node.init_params,
        runtime_state=state,
    )
    events = list(
        Engine(
            graph=Graph.new().add_root(start).add_node(node).build(),
            runtime_state=state,
            workers=1,
            container_handler_factories=(_HostContainerHandler,),
        ).run()
    )
    assert isinstance(events[-1], GraphRunPausedEvent)
    assert (state.node_run_steps, state.total_tokens) == (6, 100)


def test_container_counts_own_usage_alongside_child_totals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    child_results: list[NodeRunResult] = []

    def run_with_usage(_node: LoopStartNode) -> NodeRunResult:
        result = NodeRunResult(
            status="succeeded",
            llm_usage=LLMUsage.from_metadata({"total_tokens": 100}),
        )
        child_results.append(result)
        return result

    complete_loop = LoopContainerHandler._complete_loop

    def complete_with_own_usage(
        handler: LoopContainerHandler, *, run_state: LoopRunState, steps: int
    ) -> ContainerExecutionResult:
        result = complete_loop(handler, run_state=run_state, steps=steps)
        own_usage = LLMUsage.from_metadata({"total_tokens": 70})
        result.node_run_result.llm_usage = result.node_run_result.llm_usage.plus(
            own_usage
        )
        result.node_run_result.own_llm_usage = own_usage
        return result

    monkeypatch.setattr(LoopStartNode, "_run", run_with_usage)
    monkeypatch.setattr(LoopContainerHandler, "_complete_loop", complete_with_own_usage)
    engine = _hitl_engine(
        _loop_dsl(),
        runtime_state=_new_runtime_state({}),
        callback=_complete_loop_hitl,
    )
    events = list(engine.run())

    assert engine.runtime_state.total_tokens == 370
    completed = next(
        event
        for event in events
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "loop"
    )
    assert completed.node_run_result.llm_usage.total_tokens == 370
    assert [result.llm_usage.total_tokens for result in child_results] == [100] * 3


@pytest.mark.parametrize("deferred", [False, True])
def test_legacy_custom_result_usage_survives_until_parent_resumes(
    deferred: bool,
) -> None:
    state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        node_run_steps=2,
        llm_usage=LLMUsage.from_metadata({"total_tokens": 10}),
    )
    run = CustomContainerRunState(
        invocation_id="invocation",
        frame_id="root",
        node_id="tool",
        started_at=datetime.now(UTC).replace(tzinfo=None),
        payload="{}",
    )
    state.graph_execution.start()
    state.put_container_run(run)
    pending = ResumeTask(
        invocation_id=run.invocation_id,
        result=ContainerExecutionResult(
            metadata={},
            steps=3,
            node_run_result=ContainerNodeRunResult(
                status="succeeded",
                llm_usage=LLMUsage.from_metadata({"total_tokens": 70}),
            ),
        ),
    )
    target_queue = state.deferred_ready_queue if deferred else state.ready_queue
    target_queue.put(pending)
    # Version 3 stored this completed child's work only on the pending result.
    legacy = json.loads(state.dumps())
    legacy["version"] = "3.0"
    restored = RuntimeState.from_snapshot(json.dumps(legacy))

    assert (restored.node_run_steps, restored.total_tokens) == (2, 80)
    restored = RuntimeState.from_snapshot(restored.dumps())
    assert (restored.node_run_steps, restored.total_tokens) == (2, 80)
    node = _HostContainerNode(
        node_id="tool",
        data=BaseNodeData(type=_HostContainerNode.node_type, title="Host container"),
        init_params=InitParams(
            workflow_id="workflow", graph_config={}, run_context={}, call_depth=0
        ),
        runtime_state=restored,
    )
    start = StartNode(
        node_id="start",
        data=StartNodeData(title="Start", variables=[]),
        init_params=node.init_params,
        runtime_state=restored,
    )
    events = list(
        Engine(
            graph=Graph.new().add_root(start).add_node(node).build(),
            runtime_state=restored,
            workers=1,
        ).run()
    )
    assert isinstance(events[-1], GraphRunSucceededEvent)
    assert (restored.node_run_steps, restored.total_tokens) == (2, 80)
    completed = next(
        event for event in events if isinstance(event, NodeRunSucceededEvent)
    )
    assert completed.node_run_result.llm_usage.total_tokens == 70


def test_handled_child_failure_usage_is_counted_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = yaml.safe_load(_loop_dsl())
    child = next(
        node for node in document["graph"]["nodes"] if node["id"] == "loop-start"
    )
    child["data"]["error_strategy"] = "default-value"
    monkeypatch.setattr(
        LoopStartNode,
        "_run",
        lambda _: NodeRunResult(
            status=WorkflowNodeExecutionStatus.FAILED,
            error="model unavailable",
            llm_usage=LLMUsage.from_metadata({"total_tokens": 100}),
        ),
    )
    engine = _hitl_engine(
        yaml.safe_dump(document),
        runtime_state=_new_runtime_state({}),
        callback=_complete_loop_hitl,
    )
    events = list(engine.run())
    assert engine.runtime_state.total_tokens == 300
    completed = next(
        event
        for event in events
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "loop"
    )
    assert completed.node_run_result.llm_usage.total_tokens == 300


def test_legacy_pause_restores_child_usage_without_merging_it_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run_with_usage(_node: LoopStartNode) -> NodeRunResult:
        return NodeRunResult(
            status="succeeded",
            llm_usage=LLMUsage.from_metadata({
                "total_tokens": 100,
                "total_price": "0.01",
            }),
        )

    monkeypatch.setattr(LoopStartNode, "_run", run_with_usage)
    snapshot = (
        Path(__file__).with_name("fixtures") / "runtime_state_v3_0_8_0_paused_loop.json"
    ).read_text()
    restored = RuntimeState.from_snapshot(snapshot)
    assert (restored.node_run_steps, restored.total_tokens) == (4, 200)
    restored = RuntimeState.from_snapshot(restored.dumps())
    assert (restored.node_run_steps, restored.total_tokens) == (4, 200)
    events = list(
        _hitl_engine(
            _loop_dsl(),
            runtime_state=restored,
            callback=_complete_loop_hitl,
        ).run()
    )
    assert isinstance(events[-1], GraphRunSucceededEvent)
    assert (restored.node_run_steps, restored.total_tokens) == (8, 300)


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
    paused_state = RuntimeState.from_snapshot(snapshot)
    run_state = paused_state.container_runs()[0]
    frame_state = paused_state.container_frames()[0]
    deferred_tasks = paused_state.take_deferred_ready_tasks()
    resumed_events = list(
        _hitl_engine(
            _loop_dsl(),
            runtime_state=RuntimeState.from_snapshot(snapshot),
            callback=_complete_loop_hitl,
        ).run()
    )
    pause_requests = [
        event
        for event in paused_events
        if isinstance(event, NodeRunPauseRequestedEvent)
    ]
    assert len(pause_requests) == 1
    assert pause_requests[0].container_id == "loop"
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
        and event.container_id == "loop"
    ]
    resumed_successes = [
        event
        for event in resumed_events
        if isinstance(event, NodeRunSucceededEvent)
        and event.node_id == "human-input"
        and event.container_id == "loop"
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
    assert isinstance(run_state, LoopRunState)
    assert run_state.completed_count == 1
    assert {key: value.to_object() for key, value in run_state.outputs.items()} == {
        "seed": "fixed",
        "loop_round": 1,
    }
    assert isinstance(frame_state, LoopFrameState)
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


def test_version_2_full_graph_snapshot_restores_scoped_loop_frames() -> None:
    """Resume a frozen v2 paused-loop snapshot with a live child frame.

    Commit 1d0f32c, the direct parent of the v3 writer, generated this fixture.
    Version 2 materialized every frame from the complete workflow graph, so the
    root and live Loop frame both contain every node and positional ``edge_N``.
    """
    assert json.loads(_V2_PAUSED_LOOP_SNAPSHOT)["container_frames"]
    resumed_events = list(
        _hitl_engine(
            _loop_dsl(),
            runtime_state=RuntimeState.from_snapshot(_V2_PAUSED_LOOP_SNAPSHOT),
            callback=_complete_loop_hitl,
        ).run()
    )

    assert [
        event.node_run_result.metadata[WorkflowNodeExecutionMetadataKey.LOOP_INDEX]
        for event in resumed_events
        if isinstance(event, NodeRunSucceededEvent)
        and event.node_id == "human-input"
        and event.container_id == "loop"
    ] == [1, 2]
    assert final_outputs(resumed_events) == {"rounds": 3, "seed": "fixed"}


def test_version_1_child_task_is_rejected_before_worker_start() -> None:
    initial_state = _new_runtime_state({})
    snapshot = json.loads(initial_state.dumps())
    snapshot.update({
        "version": "1.0",
        "paused_nodes": ["human-input"],
        "deferred_nodes": [],
        "graph_state": {
            "nodes": dict.fromkeys(
                ("start", "loop", "loop-start", "human-input", "end"),
                NodeState.UNKNOWN,
            ),
            "edges": {
                "edge_0": NodeState.UNKNOWN,
                "edge_1": NodeState.UNKNOWN,
                "edge_2": NodeState.UNKNOWN,
            },
        },
    })
    restored = RuntimeState.from_snapshot(json.dumps(snapshot))

    with pytest.raises(
        RuntimeError,
        match="child-frame tasks that cannot be restored without frame state",
    ):
        _hitl_engine(
            _loop_dsl(),
            runtime_state=restored,
            callback=_complete_loop_hitl,
        )

    assert restored.take_deferred_ready_tasks() == [
        StartTask(frame_id="root", node_id="human-input")
    ]


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
    assert engine.runtime_state.container_runs() == ()
    assert engine.runtime_state.container_frames() == ()


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
    paused_state = RuntimeState.from_snapshot(snapshot)
    run_state = paused_state.container_runs()[0]
    frame_state = paused_state.container_frames()[0]
    deferred_tasks = paused_state.take_deferred_ready_tasks()
    resumed_events = list(
        _hitl_engine(
            _iteration_dsl(),
            runtime_state=RuntimeState.from_snapshot(snapshot),
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
        and event.container_id == "iteration"
    ]
    resumed_successes = [
        event
        for event in resumed_events
        if isinstance(event, NodeRunSucceededEvent)
        and event.node_id == "human-input"
        and event.container_id == "iteration"
    ]
    start_tasks = [task for task in deferred_tasks if isinstance(task, StartTask)]
    resume_tasks = [task for task in deferred_tasks if isinstance(task, ResumeTask)]
    assert len(pause_requests) == 1
    assert pause_requests[0].container_id == "iteration"
    assert pause_requests[0].reason == HitlRequired(
        session_id="session-beta",
        node_id="human-input",
        node_title="Approve iteration item",
    )
    assert paused_events.index(pause_requests[0]) < paused_events.index(
        paused_successes[0]
    )
    assert isinstance(run_state, IterationRunState)
    assert run_state.scheduled_count == 2
    assert run_state.completed_count == 1
    assert {key: value.to_object() for key, value in run_state.outputs.items()} == {
        "0": "alpha!"
    }
    assert isinstance(frame_state, IterationFrameState)
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
    for task in runtime_state.ready_queue.take_all():
        runtime_state.defer_ready_task(task)
    snapshot = runtime_state.dumps()
    restored_for_assert = RuntimeState.from_snapshot(snapshot)
    deferred_tasks = restored_for_assert.take_deferred_ready_tasks()

    task_events = _resume_loop_snapshot(snapshot)

    assert restored_for_assert.ready_queue.qsize() == 0
    assert any(isinstance(task, ResumeTask) for task in deferred_tasks)
    assert isinstance(task_events[0].event, NodeRunLoopSucceededEvent)
    assert isinstance(task_events[1].event, NodeRunSucceededEvent)
    assert task_events[1].event.node_id == "loop"
