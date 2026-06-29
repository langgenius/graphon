import queue
import threading
from collections.abc import Generator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from time import time
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock

import pytest

from graphon.enums import (
    BuiltinNodeTypes,
    ErrorStrategy,
    NodeExecutionType,
    NodeState,
    NodeType,
    WorkflowNodeExecutionMetadataKey,
)
from graphon.graph.graph import Graph
from graphon.graph_engine.command_channels.redis_channel import RedisChannel
from graphon.graph_engine.container_handlers import ContainerHandler
from graphon.graph_engine.domain.graph_execution import GraphExecution
from graphon.graph_engine.entities.commands import (
    AbortCommand,
    CommandType,
    PauseCommand,
    UpdateVariablesCommand,
)
from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_engine.event_management.event_handlers import EventHandler
from graphon.graph_engine.event_management.event_manager import EventManager
from graphon.graph_engine.frames import ExecutionFrame, FrameRegistry
from graphon.graph_engine.graph_engine import GraphEngine
from graphon.graph_engine.graph_state_manager import GraphStateManager
from graphon.graph_engine.iteration_container_handler import IterationContainerHandler
from graphon.graph_engine.layers.execution_limits import (
    ExecutionLimitsLayer,
    LimitType,
)
from graphon.graph_engine.loop_container_handler import LoopContainerHandler
from graphon.graph_engine.orchestration.dispatcher import Dispatcher
from graphon.graph_engine.orchestration.execution_coordinator import (
    ExecutionCoordinator as RealExecutionCoordinator,
)
from graphon.graph_engine.ready_queue.factory import create_ready_queue_from_state
from graphon.graph_engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.graph_engine.ready_queue.protocol import (
    ReadyQueueState,
    ResumeTask,
    StartTask,
)
from graphon.graph_engine.worker import Worker
from graphon.graph_engine.worker_management import WorkerPool
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import (
    NodeRunExceptionEvent,
    NodeRunFailedEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.node_events.base import NodeRunResult
from graphon.nodes.container_effects import (
    IterationExecutionSucceeded,
    IterationFrameRequest,
    IterationFramesRequested,
    LoopExecutionFailed,
    LoopExecutionSucceeded,
    LoopFrameCompleted,
    LoopFrameRequest,
)
from graphon.nodes.iteration.entities import ErrorHandleMode
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.nodes.loop.loop_node import LoopNode
from graphon.runtime.container_state import (
    ContainerFrameState,
    ContainerRunState,
    FrameRuntimeData,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import StringSegment


def _variable_value(
    runtime_state: GraphRuntimeState,
    selector: list[str],
) -> object:
    variable = runtime_state.variable_pool.get(selector)
    assert variable is not None
    return variable.to_object()


def _execution_frame(
    *,
    frame_id: str,
    graph: Graph,
    graph_runtime_state: object | None = None,
    state_manager: object | None = None,
    edge_processor: object | None = None,
    error_handler: object | None = None,
) -> ExecutionFrame:
    if isinstance(graph_runtime_state, MagicMock):
        graph_runtime_state.has_container_frame.return_value = False
    if edge_processor is None:
        resolved_edge_processor = MagicMock()
        resolved_edge_processor.process_node_success.return_value = ([], [])
        resolved_edge_processor.handle_branch_completion.return_value = ([], [])
    else:
        resolved_edge_processor = edge_processor
    return ExecutionFrame(
        frame_id=frame_id,
        graph=graph,
        graph_runtime_state=cast(Any, graph_runtime_state or MagicMock()),
        state_manager=cast(Any, state_manager or MagicMock()),
        edge_processor=cast(Any, resolved_edge_processor),
        error_handler=cast(Any, error_handler or MagicMock()),
    )


def _event_handler(
    *,
    graph_execution: object,
    event_collector: object,
    frame_registry: FrameRegistry,
) -> EventHandler:
    container_handlers = _container_handlers(
        frame_registry=frame_registry,
        graph_execution=graph_execution,
    )
    return EventHandler(
        graph_execution=cast(Any, graph_execution),
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
        container_handlers=container_handlers,
    )


def _event_handler_with_container(
    *,
    graph_execution: object,
    event_collector: object,
    frame_registry: FrameRegistry,
) -> tuple[EventHandler, dict[str, ContainerHandler]]:
    container_handlers = _container_handlers(
        frame_registry=frame_registry,
        graph_execution=graph_execution,
    )
    return (
        EventHandler(
            graph_execution=cast(Any, graph_execution),
            event_collector=cast(EventManager, event_collector),
            frame_registry=frame_registry,
            container_handlers=container_handlers,
        ),
        container_handlers,
    )


def _container_handlers(
    *,
    frame_registry: FrameRegistry,
    graph_execution: object,
) -> dict[str, ContainerHandler]:
    return {
        "loop": LoopContainerHandler(
            frame_registry=frame_registry,
            graph_execution=cast(Any, graph_execution),
        ),
        "iteration": IterationContainerHandler(
            frame_registry=frame_registry,
            graph_execution=cast(Any, graph_execution),
        ),
    }


def _get_resume_task(ready_queue: InMemoryReadyQueue) -> ResumeTask:
    task = ready_queue.get(timeout=0.01)
    assert isinstance(task, ResumeTask)
    return task


def _start_loop_await(
    container_handler: ContainerHandler,
    runtime_state: GraphRuntimeState,
    *,
    invocation_id: str,
    index: int,
    loop_count: int,
) -> None:
    request = LoopFrameRequest(
        started_at=datetime.now(UTC).replace(tzinfo=None),
        inputs={"loop_count": loop_count},
        loop_count=loop_count,
        root_node_id="loop-start",
        loop_variable_selectors={},
        loop_node_ids=frozenset(),
        index=index,
    )
    phase_data = {
        "inputs": dict(request.inputs),
        "loop_count": request.loop_count,
        "root_node_id": request.root_node_id,
        "loop_variable_selectors": {},
        "loop_node_ids": (),
    }
    with suppress(KeyError):
        existing_run_state = runtime_state.get_container_run(invocation_id)
        phase_data = {**dict(existing_run_state.phase_data), **phase_data}
    runtime_state.put_container_run(
        ContainerRunState(
            invocation_id=invocation_id,
            kind="loop",
            frame_id="root",
            node_id="loop",
            execution_id="loop-run",
            started_at=request.started_at,
            phase_data=phase_data,
        ),
    )
    container_handler.start_await(
        frame_id="root",
        node_id="loop",
        invocation_id=invocation_id,
        request=request,
    )


def _start_iteration_await(
    container_handler: ContainerHandler,
    runtime_state: GraphRuntimeState,
    *,
    invocation_id: str,
    indexes: tuple[int, ...],
    items: tuple[object, ...],
    error_handle_mode: ErrorHandleMode,
    flatten_output: bool,
    parallel_nums: int,
) -> None:
    request = IterationFrameRequest(
        started_at=datetime.now(UTC).replace(tzinfo=None),
        inputs={"iterator_selector": list(items)},
        items=items,
        root_node_id="iteration-start",
        indexes=indexes,
        output_selector=["answer", "text"],
        error_handle_mode=error_handle_mode,
        flatten_output=flatten_output,
        parallel_nums=parallel_nums,
    )
    phase_data = {
        "inputs": dict(request.inputs),
        "items": request.items,
        "root_node_id": request.root_node_id,
        "output_selector": list(request.output_selector),
        "error_handle_mode": request.error_handle_mode,
        "flatten_output": request.flatten_output,
        "parallel_nums": request.parallel_nums,
    }
    with suppress(KeyError):
        existing_run_state = runtime_state.get_container_run(invocation_id)
        phase_data = {**dict(existing_run_state.phase_data), **phase_data}
    runtime_state.put_container_run(
        ContainerRunState(
            invocation_id=invocation_id,
            kind="iteration",
            frame_id="root",
            node_id="iteration",
            execution_id="iteration-run",
            started_at=request.started_at,
            phase_data=phase_data,
        ),
    )
    container_handler.start_await(
        frame_id="root",
        node_id="iteration",
        invocation_id=invocation_id,
        request=request,
    )


def _worker(
    *,
    ready_queue: InMemoryReadyQueue,
    event_queue: queue.Queue[TaskEvent],
    frame_registry: FrameRegistry,
) -> Worker:
    return Worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
        container_handlers={},
    )


@dataclass
class _FrameNode:
    id: str
    node_type: BuiltinNodeTypes

    execution_type: ClassVar[NodeExecutionType] = NodeExecutionType.EXECUTABLE
    error_strategy: ClassVar[None] = None
    retry: ClassVar[bool] = False
    state: ClassVar[NodeState] = NodeState.UNKNOWN


class _FrameFactory:
    def with_runtime_state(
        self,
        graph_runtime_state: GraphRuntimeState,
    ) -> "_FrameFactory":
        _ = graph_runtime_state
        return self

    def create_node(self, node_config: dict[str, object]) -> _FrameNode:
        node_data = cast(dict[str, object], node_config["data"])
        return _FrameNode(
            str(node_config["id"]),
            cast(BuiltinNodeTypes, node_data["type"]),
        )


@pytest.mark.parametrize(
    ("payload", "expected_command_type"),
    [
        (
            {"command_type": CommandType.ABORT.value, "reason": "stop"},
            AbortCommand,
        ),
        (
            {"command_type": CommandType.PAUSE.value, "reason": "wait"},
            PauseCommand,
        ),
        (
            {"command_type": CommandType.UPDATE_VARIABLES.value, "updates": []},
            UpdateVariablesCommand,
        ),
    ],
)
def test_redis_channel_deserializes_command_with_model_map(
    payload: dict[str, object],
    expected_command_type: type,
) -> None:
    channel = RedisChannel(redis_client=MagicMock(), channel_key="test-channel")

    command = channel.deserialize_command(payload)

    assert isinstance(command, expected_command_type)


def test_create_ready_queue_from_state_restores_ready_tasks() -> None:
    queue = create_ready_queue_from_state(
        ReadyQueueState(
            type="InMemoryReadyQueue",
            version="1.0",
            items=[
                StartTask(frame_id="root", node_id="start"),
                StartTask(frame_id="iteration-0", node_id="answer"),
            ],
        ),
    )

    assert queue.get(timeout=0.01) == StartTask(frame_id="root", node_id="start")
    assert queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-0",
        node_id="answer",
    )


def test_graph_state_manager_enqueues_ready_task_for_frame() -> None:
    ready_queue = InMemoryReadyQueue()
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
    )
    graph = SimpleNamespace(
        nodes={"start": SimpleNamespace(state=NodeState.UNKNOWN)},
    )
    manager = GraphStateManager(
        graph=cast(Graph, graph),
        graph_runtime_state=runtime_state,
    )

    manager.enqueue_node(frame_id="root", node_id="start")

    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="root",
        node_id="start",
    )
    assert graph.nodes["start"].state == NodeState.TAKEN


def test_graph_state_manager_defers_ready_task_when_paused() -> None:
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow", paused=True)
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    graph = SimpleNamespace(
        nodes={"start": SimpleNamespace(state=NodeState.UNKNOWN)},
    )
    manager = GraphStateManager(
        graph=cast(Graph, graph),
        graph_runtime_state=runtime_state,
    )

    manager.enqueue_node(frame_id="root", node_id="start")

    assert ready_queue.qsize() == 0
    assert runtime_state.drain_deferred_ready_tasks() == [
        StartTask(frame_id="root", node_id="start"),
    ]
    assert graph.nodes["start"].state == NodeState.TAKEN


def test_graph_state_manager_tracks_executing_tasks_by_frame() -> None:
    ready_queue = InMemoryReadyQueue()
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
    )
    graph = SimpleNamespace(nodes={})
    manager = GraphStateManager(
        graph=cast(Graph, graph),
        graph_runtime_state=runtime_state,
    )

    manager.start_execution(frame_id="iteration-0", node_id="answer")
    manager.start_execution(frame_id="iteration-1", node_id="answer")
    manager.finish_execution(frame_id="iteration-0", node_id="answer")

    assert manager.get_executing_count() == 1
    assert manager.get_executing_nodes() == {
        StartTask(frame_id="iteration-1", node_id="answer"),
    }


def test_graph_state_manager_completion_ignores_other_frame_queue_items() -> None:
    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="other-frame", node_id="answer"))
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
    )
    graph = SimpleNamespace(nodes={})
    manager = GraphStateManager(
        graph=cast(Graph, graph),
        graph_runtime_state=runtime_state,
    )

    assert manager.is_execution_complete() is True


def test_pause_drains_ready_tasks_without_clearing_executing_tasks() -> None:
    ready_queue = InMemoryReadyQueue()
    queued_task = StartTask(frame_id="root", node_id="queued")
    ready_queue.put(queued_task)
    graph_execution = GraphExecution(workflow_id="workflow", paused=True)
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    manager = GraphStateManager(
        graph=cast(Graph, SimpleNamespace(nodes={})),
        graph_runtime_state=runtime_state,
    )
    manager.start_execution(frame_id="root", node_id="running")
    worker_pool = MagicMock()
    coordinator = RealExecutionCoordinator(
        graph_execution=graph_execution,
        state_manager=manager,
        command_processor=MagicMock(),
        worker_pool=worker_pool,
    )

    coordinator.handle_pause_if_needed()

    assert ready_queue.qsize() == 0
    assert runtime_state.drain_deferred_ready_tasks() == [queued_task]
    assert manager.get_executing_nodes() == {
        StartTask(frame_id="root", node_id="running"),
    }
    worker_pool.drain.assert_called_once_with()
    worker_pool.stop.assert_not_called()


def test_pause_drain_removes_only_drained_ready_tasks_from_executing() -> None:
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    graph = SimpleNamespace(
        nodes={
            "active": SimpleNamespace(state=NodeState.UNKNOWN),
            "queued": SimpleNamespace(state=NodeState.UNKNOWN),
        }
    )
    manager = GraphStateManager(
        graph=cast(Graph, graph),
        graph_runtime_state=runtime_state,
    )
    active_task = StartTask(frame_id="root", node_id="active")
    queued_task = StartTask(frame_id="root", node_id="queued")
    assert manager.enqueue_node(frame_id="root", node_id="active") is True
    manager.start_execution(frame_id="root", node_id="active")
    assert manager.enqueue_node(frame_id="root", node_id="queued") is True
    manager.start_execution(frame_id="root", node_id="queued")
    assert ready_queue.get(timeout=0.01) == active_task

    graph_execution.paused = True
    manager.drain_ready_tasks_to_deferred()

    assert runtime_state.drain_deferred_ready_tasks() == [queued_task]
    assert manager.get_executing_nodes() == {active_task}
    manager.finish_execution(frame_id="root", node_id="active")
    assert manager.get_executing_count() == 0


def test_worker_pool_drain_does_not_stop_worker_with_current_task() -> None:
    class WorkerStub:
        def __init__(self, *, has_current_task: bool) -> None:
            self.has_current_task = has_current_task
            self.is_idle = True
            self.stopped = False

        def stop(self) -> None:
            self.stopped = True

    active_worker = WorkerStub(has_current_task=True)
    idle_worker = WorkerStub(has_current_task=False)
    pool = object.__new__(WorkerPool)
    pool._lock = threading.RLock()
    pool._running = True
    pool._workers = [active_worker, idle_worker]

    pool.drain()

    assert active_worker.stopped is False
    assert idle_worker.stopped is True


def test_resume_schedules_deferred_ready_tasks_not_legacy_node_snapshots() -> None:
    ready_queue = InMemoryReadyQueue()
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
    )
    runtime_state.defer_ready_task(StartTask(frame_id="root", node_id="ready"))
    runtime_state.register_paused_node("paused-legacy")
    runtime_state.register_deferred_node("deferred-legacy")
    engine = object.__new__(GraphEngine)
    engine._worker_pool = MagicMock()
    engine._graph_runtime_state = runtime_state
    engine._state_manager = MagicMock()
    engine._dispatcher = MagicMock()

    engine._start_execution(resume=True)

    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="root",
        node_id="ready",
    )
    with pytest.raises(queue.Empty):
        ready_queue.get(timeout=0.01)
    engine._state_manager.start_execution.assert_called_once_with(
        frame_id="root",
        node_id="ready",
    )
    assert runtime_state.consume_paused_nodes() == ["paused-legacy"]
    assert runtime_state.consume_deferred_nodes() == ["deferred-legacy"]


def test_paused_exception_default_path_defers_without_marking_executing() -> None:
    graph = MagicMock()
    graph.nodes = {
        "failed": MagicMock(
            execution_type=NodeExecutionType.EXECUTABLE,
            error_strategy=ErrorStrategy.DEFAULT_VALUE,
        )
    }
    runtime_state = MagicMock()
    runtime_state.variable_pool = MagicMock()
    graph_execution = MagicMock()
    graph_execution.is_paused = True
    graph_execution.get_or_create_node_execution.return_value = MagicMock()
    event_collector = MagicMock()
    edge_processor = MagicMock()
    edge_processor.process_node_success.return_value = (["next"], [])
    state_manager = MagicMock()
    state_manager.enqueue_node.return_value = False
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
            state_manager=state_manager,
            edge_processor=edge_processor,
        ),
    )
    handler = _event_handler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    event = NodeRunExceptionEvent(
        id="run-failed",
        node_id="failed",
        node_type=BuiltinNodeTypes.CODE,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        error="boom",
        node_run_result=NodeRunResult(outputs={"answer": "fallback"}),
    )

    handler.dispatch(TaskEvent(frame_id="root", event=event))

    state_manager.enqueue_node.assert_called_once_with(
        frame_id="root",
        node_id="next",
    )
    state_manager.start_execution.assert_not_called()


def test_graph_execution_tracks_node_executions_by_frame() -> None:
    execution = GraphExecution(workflow_id="workflow")

    first = execution.get_or_create_node_execution(
        frame_id="iteration-0",
        node_id="answer",
    )
    second = execution.get_or_create_node_execution(
        frame_id="iteration-1",
        node_id="answer",
    )

    first.mark_started()
    second.mark_started()

    assert first is not second
    assert (
        execution.node_executions[
            StartTask(frame_id="iteration-0", node_id="answer")
        ].execution_id
        == first.execution_id
    )
    assert (
        execution.node_executions[
            StartTask(frame_id="iteration-1", node_id="answer")
        ].execution_id
        == second.execution_id
    )
    assert first.execution_id != second.execution_id


def test_frame_registry_materializes_child_frame_with_rebound_runtime() -> None:
    @dataclass
    class RuntimeBoundNode:
        id: str
        graph_runtime_state: GraphRuntimeState

        node_type: ClassVar[NodeType] = BuiltinNodeTypes.START
        execution_type: ClassVar[NodeExecutionType] = NodeExecutionType.ROOT
        error_strategy: ClassVar[None] = None
        state: ClassVar[NodeState] = NodeState.UNKNOWN

    class RuntimeBoundFactory:
        def __init__(self, runtime_state: GraphRuntimeState) -> None:
            self.runtime_state = runtime_state

        def with_runtime_state(
            self,
            graph_runtime_state: GraphRuntimeState,
        ) -> "RuntimeBoundFactory":
            return RuntimeBoundFactory(graph_runtime_state)

        def create_node(self, node_config: dict[str, object]) -> RuntimeBoundNode:
            return RuntimeBoundNode(str(node_config["id"]), self.runtime_state)

    graph_config = {
        "nodes": [{"id": "start", "data": {"type": BuiltinNodeTypes.START}}],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=cast(Any, RuntimeBoundFactory(root_runtime_state)),
        root_node_id="start",
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=root_graph,
            graph_runtime_state=root_runtime_state,
        ),
    )
    child_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=2,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )

    child_frame = frame_registry.materialize_child_frame(
        frame_id="child",
        root_node_id="start",
        graph_runtime_state=child_runtime_state,
    )

    assert child_frame.graph is not root_graph
    assert child_frame.graph.nodes["start"] is not root_graph.nodes["start"]
    assert child_frame.graph.nodes["start"].graph_runtime_state is child_runtime_state


def test_frame_registry_materializes_child_frame_from_state() -> None:
    graph_config = {
        "nodes": [
            {"id": "start", "data": {"type": BuiltinNodeTypes.ITERATION_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=cast(Any, _FrameFactory()),
        root_node_id="start",
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=root_graph,
            graph_runtime_state=root_runtime_state,
        ),
    )
    variable_pool = VariablePool()
    variable_pool.add(["child", "value"], "saved")
    frame_state = ContainerFrameState(
        frame_id="child-frame",
        kind="iteration",
        parent_invocation_id="iteration-invocation",
        root_node_id="start",
        runtime_data=FrameRuntimeData(
            variable_pool=variable_pool,
            outputs={"answer": "saved"},
            node_run_steps=2,
            graph_node_states={"start": NodeState.TAKEN},
        ),
    )

    child_frame = frame_registry.materialize_child_frame_from_state(
        frame_state,
        graph_execution=graph_execution,
        ready_queue=ready_queue,
    )

    assert frame_registry.has("child-frame")
    assert child_frame.frame_id == "child-frame"
    assert _variable_value(child_frame.graph_runtime_state, ["child", "value"]) == (
        "saved"
    )
    assert child_frame.graph_runtime_state.outputs == {"answer": "saved"}
    assert child_frame.graph_runtime_state.node_run_steps == 2
    assert child_frame.graph.nodes["start"].state == NodeState.TAKEN


def test_frame_registry_rejects_frame_state_with_missing_graph_state_ids() -> None:
    graph_config = {
        "nodes": [
            {"id": "start", "data": {"type": BuiltinNodeTypes.ITERATION_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=cast(Any, _FrameFactory()),
        root_node_id="start",
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=root_graph,
            graph_runtime_state=root_runtime_state,
        ),
    )
    frame_state = ContainerFrameState(
        frame_id="child-frame",
        kind="iteration",
        parent_invocation_id="iteration-invocation",
        root_node_id="start",
        runtime_data=FrameRuntimeData(
            variable_pool=VariablePool(),
            graph_node_states={"missing-node": NodeState.TAKEN},
            graph_edge_states={"missing-edge": NodeState.TAKEN},
        ),
    )

    with pytest.raises(RuntimeError, match=r"missing-node.*missing-edge"):
        frame_registry.materialize_child_frame_from_state(
            frame_state,
            graph_execution=graph_execution,
            ready_queue=ready_queue,
        )
    assert not frame_registry.has("child-frame")


def test_frame_registry_copies_frame_runtime_data_from_state() -> None:
    graph_config = {
        "nodes": [
            {"id": "start", "data": {"type": BuiltinNodeTypes.ITERATION_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=cast(Any, _FrameFactory()),
        root_node_id="start",
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=root_graph,
            graph_runtime_state=root_runtime_state,
        ),
    )
    variable_pool = VariablePool()
    variable_pool.add(["child", "value"], "saved")
    frame_state = ContainerFrameState(
        frame_id="child-frame",
        kind="iteration",
        parent_invocation_id="iteration-invocation",
        root_node_id="start",
        runtime_data=FrameRuntimeData(
            variable_pool=variable_pool,
            outputs={"nested": {"value": "saved"}},
        ),
    )

    child_frame = frame_registry.materialize_child_frame_from_state(
        frame_state,
        graph_execution=graph_execution,
        ready_queue=ready_queue,
    )
    child_frame.graph_runtime_state.variable_pool.add(["child", "value"], "changed")
    outputs = child_frame.graph_runtime_state.outputs
    cast(dict[str, object], outputs["nested"])["value"] = "changed"
    child_frame.graph_runtime_state.outputs = outputs

    saved_variable = frame_state.runtime_data.variable_pool.get(["child", "value"])
    assert saved_variable is not None
    assert saved_variable.to_object() == "saved"
    assert frame_state.runtime_data.outputs == {"nested": {"value": "saved"}}


def test_worker_executes_node_from_ready_task() -> None:
    class RunnableNode:
        id = "start"
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE
        execution_id = "run-1"

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(self) -> Generator[NodeRunStartedEvent, None, None]:
            yield NodeRunStartedEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title="Start",
                start_at=datetime.now(UTC).replace(tzinfo=None),
            )

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="root", node_id="start"))
    event_queue = queue.Queue()
    graph = SimpleNamespace(nodes={"start": RunnableNode()})
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=GraphExecution(workflow_id="workflow"),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    worker = _worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
    )

    worker.start()
    try:
        event = event_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(event, TaskEvent)
    assert event.frame_id == "root"
    assert isinstance(event.event, NodeRunStartedEvent)
    assert event.event.node_id == "start"


def test_worker_resolves_node_from_task_frame() -> None:
    class RunnableNode:
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE
        execution_id = "run-child"

        def __init__(self, node_id: str, title: str) -> None:
            self.id = node_id
            self.title = title

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(self) -> Generator[NodeRunStartedEvent, None, None]:
            yield NodeRunStartedEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title=self.title,
                start_at=datetime.now(UTC).replace(tzinfo=None),
            )

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="child", node_id="answer"))
    event_queue = queue.Queue()
    root_graph = SimpleNamespace(nodes={"answer": RunnableNode("answer", "Root")})
    child_graph = SimpleNamespace(nodes={"answer": RunnableNode("answer", "Child")})
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, root_graph),
            graph_runtime_state=runtime_state,
        ),
    )
    frame_registry.register(
        _execution_frame(
            frame_id="child",
            graph=cast(Graph, child_graph),
            graph_runtime_state=runtime_state,
        ),
    )
    worker = _worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
    )

    worker.start()
    try:
        event = event_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(event, TaskEvent)
    assert event.frame_id == "child"
    assert isinstance(event.event, NodeRunStartedEvent)
    assert event.event.node_title == "Child"


def test_worker_binds_node_execution_id_from_task_frame() -> None:
    class RunnableNode:
        id = "answer"
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE

        def __init__(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(self) -> Generator[NodeRunStartedEvent, None, None]:
            yield NodeRunStartedEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title="Answer",
                start_at=datetime.now(UTC).replace(tzinfo=None),
            )

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="child", node_id="answer"))
    event_queue = queue.Queue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_execution = graph_execution.get_or_create_node_execution(
        frame_id="root",
        node_id="answer",
    )
    child_execution = graph_execution.get_or_create_node_execution(
        frame_id="child",
        node_id="answer",
    )
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    graph = SimpleNamespace(nodes={"answer": RunnableNode(root_execution.execution_id)})
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="child",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    worker = _worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
    )

    worker.start()
    try:
        event = event_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(event, TaskEvent)
    assert isinstance(event.event, NodeRunStartedEvent)
    assert event.event.id != root_execution.execution_id
    assert event.event.id == child_execution.execution_id


def test_worker_suspends_container_invocation_at_await_request() -> None:
    class ContainerNode:
        id = "loop"
        node_type = BuiltinNodeTypes.LOOP
        execution_type = NodeExecutionType.CONTAINER
        execution_id = "run-loop"

        def __init__(self) -> None:
            self.await_was_reached = False
            self.body_after_await_was_consumed = False

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(
            self,
        ) -> Generator[GraphNodeEventBase | LoopFrameRequest, object, None]:
            started_at = datetime.now(UTC).replace(tzinfo=None)
            yield NodeRunStartedEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title="Loop",
                start_at=started_at,
            )
            self.await_was_reached = True
            _ = yield LoopFrameRequest(
                started_at=started_at,
                inputs={"loop_count": 1},
                loop_count=1,
                root_node_id="loop-start",
                loop_variable_selectors={},
                loop_node_ids=frozenset(),
                index=0,
            )
            self.body_after_await_was_consumed = True

    class RecordingContainerHandler:
        kind = "loop"

        def __init__(self) -> None:
            self.request: LoopFrameRequest

        def start_await(
            self,
            *,
            frame_id: str,
            node_id: str,
            invocation_id: str,
            request: LoopFrameRequest,
        ) -> None:
            _ = invocation_id
            assert frame_id == "root"
            assert node_id == "loop"
            self.request = request

    container_node = ContainerNode()
    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="root", node_id="loop"))
    event_queue = queue.Queue()
    graph_execution = GraphExecution(workflow_id="workflow")
    graph = SimpleNamespace(nodes={"loop": container_node})
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    container_handler = RecordingContainerHandler()
    worker = Worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
        container_handlers={"loop": cast(ContainerHandler, container_handler)},
    )

    worker.start()
    try:
        event = event_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(event, TaskEvent)
    assert isinstance(event.event, NodeRunStartedEvent)
    assert event.event.node_id == "loop"
    assert container_node.await_was_reached is True
    assert container_node.body_after_await_was_consumed is False
    assert container_handler.request.index == 0


def test_dispatcher_preserves_task_event_for_dispatch() -> None:
    event = NodeRunStartedEvent(
        id="run-1",
        node_id="start",
        node_type=BuiltinNodeTypes.CODE,
        node_title="Start",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    task_event = TaskEvent(frame_id="root", event=event)
    event_queue = queue.Queue()
    event_queue.put(task_event)

    class ExecutionCoordinator:
        aborted = False
        paused = False

        def __init__(self) -> None:
            self.event_was_dispatched = False

        @property
        def execution_complete(self) -> bool:
            return self.event_was_dispatched

        def process_commands(self) -> None:
            return

        def check_scaling(self) -> None:
            return

        def mark_complete(self) -> None:
            self.event_was_dispatched = True

    execution_coordinator = ExecutionCoordinator()

    class RecordingEventHandler:
        dispatched_events: list[object]

        def __init__(self) -> None:
            self.dispatched_events = []

        def dispatch(self, event: object) -> None:
            self.dispatched_events.append(event)
            execution_coordinator.event_was_dispatched = True

    event_handler = RecordingEventHandler()
    dispatcher = Dispatcher(
        event_queue=event_queue,
        event_handler=cast(EventHandler, event_handler),
        execution_coordinator=cast(RealExecutionCoordinator, execution_coordinator),
    )

    dispatcher._dispatcher_loop()

    assert event_handler.dispatched_events == [task_event]


def test_event_handler_dispatches_task_event_payload() -> None:
    event = NodeRunStartedEvent(
        id="run-1",
        node_id="start",
        node_type=BuiltinNodeTypes.CODE,
        node_title="Start",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    node_execution = MagicMock(retry_count=0)
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value = node_execution
    event_collector = MagicMock()
    runtime_state = MagicMock()
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, MagicMock()),
            graph_runtime_state=runtime_state,
        ),
    )
    handler = _event_handler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )

    handler.dispatch(TaskEvent(frame_id="root", event=event))

    node_execution.mark_started.assert_called_once_with()
    runtime_state.increment_node_run_steps.assert_called_once_with()
    event_collector.collect.assert_called_once_with(event)


def test_event_handler_processes_tagged_child_frame_success_before_collecting() -> None:
    graph = MagicMock()
    graph.nodes = {"child": MagicMock(execution_type=NodeExecutionType.EXECUTABLE)}
    runtime_state = MagicMock()
    runtime_state.variable_pool = MagicMock()
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value = MagicMock()
    graph_execution.is_paused = False
    event_collector = MagicMock()
    edge_processor = MagicMock()
    edge_processor.process_node_success.return_value = (["next"], [])
    state_manager = MagicMock()
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="iteration-frame",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
            state_manager=state_manager,
            edge_processor=edge_processor,
        ),
    )
    handler = _event_handler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    event = NodeRunSucceededEvent(
        id="run-child",
        node_id="child",
        node_type=BuiltinNodeTypes.CODE,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(outputs={"answer": "ok"}),
        in_iteration_id="iteration",
    )

    handler.dispatch(TaskEvent(frame_id="iteration-frame", event=event))

    edge_processor.process_node_success.assert_called_once_with("child")
    state_manager.enqueue_node.assert_called_once_with(
        frame_id="iteration-frame",
        node_id="next",
    )
    event_collector.collect.assert_called_once_with(event)


def test_event_handler_processes_tagged_root_frame_success_before_collecting() -> None:
    graph = MagicMock()
    graph.nodes = {"child": MagicMock(execution_type=NodeExecutionType.EXECUTABLE)}
    runtime_state = MagicMock()
    runtime_state.variable_pool = MagicMock()
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value = MagicMock()
    graph_execution.is_paused = False
    event_collector = MagicMock()
    edge_processor = MagicMock()
    edge_processor.process_node_success.return_value = (["next"], [])
    state_manager = MagicMock()
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
            state_manager=state_manager,
            edge_processor=edge_processor,
        ),
    )
    handler = _event_handler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    event = NodeRunSucceededEvent(
        id="run-child",
        node_id="child",
        node_type=BuiltinNodeTypes.CODE,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(outputs={"answer": "ok"}),
        in_iteration_id="iteration",
    )

    handler.dispatch(TaskEvent(frame_id="root", event=event))

    edge_processor.process_node_success.assert_called_once_with("child")
    state_manager.enqueue_node.assert_called_once_with(
        frame_id="root",
        node_id="next",
    )
    event_collector.collect.assert_called_once_with(event)


def test_loop_container_handler_starts_loop_frame_from_loop_await() -> None:
    graph_config = {
        "nodes": [
            {"id": "loop-start", "data": {"type": BuiltinNodeTypes.LOOP_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    loop_node = LoopNode.__new__(LoopNode)
    loop_node.init_node_identity("loop")
    loop_node.init_node_data({
        "type": "loop",
        "loop_count": 1,
        "start_node_id": "loop-start",
        "break_conditions": [],
        "logical_operator": "and",
        "outputs": {},
    })
    loop_node.bind_execution_id("loop-run")
    loop_node.graph_runtime_state = runtime_state
    loop_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"loop": loop_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    state_manager = MagicMock()
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
            state_manager=state_manager,
        ),
    )
    container_handler = LoopContainerHandler(
        frame_registry=frame_registry,
        graph_execution=graph_execution,
    )

    _start_loop_await(
        container_handler,
        runtime_state,
        invocation_id="loop-invocation",
        index=0,
        loop_count=1,
    )

    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="loop-run:loop:0",
        node_id="loop-start",
    )


def test_loop_container_handler_records_loop_frame_state() -> None:
    graph_config = {
        "nodes": [
            {"id": "loop-start", "data": {"type": BuiltinNodeTypes.LOOP_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    loop_node = LoopNode.__new__(LoopNode)
    loop_node.init_node_identity("loop")
    loop_node.init_node_data({
        "type": "loop",
        "loop_count": 1,
        "start_node_id": "loop-start",
        "break_conditions": [],
        "logical_operator": "and",
        "outputs": {},
    })
    loop_node.bind_execution_id("loop-run")
    loop_node.graph_runtime_state = runtime_state
    loop_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"loop": loop_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
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
    runtime_state.put_container_run(
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
    handler = LoopContainerHandler(
        frame_registry=frame_registry,
        graph_execution=graph_execution,
    )

    handler.start_await(
        frame_id="root",
        node_id="loop",
        invocation_id="loop-invocation",
        request=request,
    )

    frame_state = runtime_state.get_container_frame("loop-run:loop:0")
    assert frame_state.kind == "loop"
    assert frame_state.parent_invocation_id == "loop-invocation"
    assert frame_state.phase_data["index"] == 0


def test_event_handler_suppresses_loop_start_events_inside_loop_frame() -> None:
    graph_config = {
        "nodes": [
            {"id": "loop-start", "data": {"type": BuiltinNodeTypes.LOOP_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    loop_node = LoopNode.__new__(LoopNode)
    loop_node.init_node_identity("loop")
    loop_node.init_node_data({
        "type": "loop",
        "loop_count": 1,
        "start_node_id": "loop-start",
        "break_conditions": [],
        "logical_operator": "and",
        "outputs": {},
    })
    loop_node.bind_execution_id("loop-run")
    loop_node.graph_runtime_state = runtime_state
    loop_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"loop": loop_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    event_collector = MagicMock()
    handler, container_handlers = _event_handler_with_container(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    _start_loop_await(
        container_handlers["loop"],
        runtime_state,
        invocation_id="loop-invocation",
        index=0,
        loop_count=1,
    )
    _ = ready_queue.get(timeout=0.01)
    event_collector.collect.reset_mock()

    child_started = NodeRunStartedEvent(
        id="loop-start-run",
        node_id="loop-start",
        node_type=BuiltinNodeTypes.LOOP_START,
        node_title="Loop Start",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    child_succeeded = NodeRunSucceededEvent(
        id="loop-start-run",
        node_id="loop-start",
        node_type=BuiltinNodeTypes.LOOP_START,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(),
    )

    handler.dispatch(TaskEvent(frame_id="loop-run:loop:0", event=child_started))
    handler.dispatch(TaskEvent(frame_id="loop-run:loop:0", event=child_succeeded))

    collected_node_ids = [
        call.args[0].node_id
        for call in event_collector.collect.call_args_list
        if isinstance(call.args[0], GraphNodeEventBase)
    ]
    assert "loop-start" not in collected_node_ids
    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, LoopExecutionSucceeded)
    assert resume_task.result.outputs["loop_round"] == 1


def test_event_handler_runs_loop_frames_until_loop_count() -> None:
    graph_config = {
        "nodes": [
            {"id": "loop-start", "data": {"type": BuiltinNodeTypes.LOOP_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    loop_node = LoopNode.__new__(LoopNode)
    loop_node.init_node_identity("loop")
    loop_node.init_node_data({
        "type": "loop",
        "loop_count": 2,
        "start_node_id": "loop-start",
        "break_conditions": [],
        "logical_operator": "and",
        "outputs": {},
    })
    loop_node.bind_execution_id("loop-run")
    loop_node.graph_runtime_state = runtime_state
    loop_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"loop": loop_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    event_collector = MagicMock()
    handler, container_handlers = _event_handler_with_container(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    _start_loop_await(
        container_handlers["loop"],
        runtime_state,
        invocation_id="loop-invocation",
        index=0,
        loop_count=2,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="loop-run:loop:0",
        node_id="loop-start",
    )

    first_succeeded = NodeRunSucceededEvent(
        id="loop-start-run-0",
        node_id="loop-start",
        node_type=BuiltinNodeTypes.LOOP_START,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(),
    )
    handler.dispatch(TaskEvent(frame_id="loop-run:loop:0", event=first_succeeded))

    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, LoopFrameCompleted)
    assert resume_task.result.next_index == 1
    _start_loop_await(
        container_handlers["loop"],
        runtime_state,
        invocation_id=resume_task.invocation_id,
        index=resume_task.result.next_index,
        loop_count=2,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="loop-run:loop:1",
        node_id="loop-start",
    )
    second_succeeded = NodeRunSucceededEvent(
        id="loop-start-run-1",
        node_id="loop-start",
        node_type=BuiltinNodeTypes.LOOP_START,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(),
    )
    handler.dispatch(TaskEvent(frame_id="loop-run:loop:1", event=second_succeeded))

    final_resume_task = _get_resume_task(ready_queue)
    assert isinstance(final_resume_task.result, LoopExecutionSucceeded)
    assert final_resume_task.result.steps == 2
    assert loop_node.node_data.outputs["loop_round"] == 2


def test_event_handler_converts_loop_child_failure_to_loop_failure() -> None:
    graph_config = {
        "nodes": [
            {"id": "loop-start", "data": {"type": BuiltinNodeTypes.LOOP_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    loop_node = LoopNode.__new__(LoopNode)
    loop_node.init_node_identity("loop")
    loop_node.init_node_data({
        "type": "loop",
        "loop_count": 2,
        "start_node_id": "loop-start",
        "break_conditions": [],
        "logical_operator": "and",
        "outputs": {},
    })
    loop_node.bind_execution_id("loop-run")
    loop_node.graph_runtime_state = runtime_state
    loop_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"loop": loop_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    error_handler = MagicMock()
    error_handler.handle_node_failure.return_value = None
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
            error_handler=error_handler,
        ),
    )
    event_collector = MagicMock()
    handler, container_handlers = _event_handler_with_container(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    _start_loop_await(
        container_handlers["loop"],
        runtime_state,
        invocation_id="loop-invocation",
        index=0,
        loop_count=2,
    )
    _ = ready_queue.get(timeout=0.01)

    handler.dispatch(
        TaskEvent(
            frame_id="loop-run:loop:0",
            event=NodeRunFailedEvent(
                id="loop-start-run-0",
                node_id="loop-start",
                node_type=BuiltinNodeTypes.LOOP_START,
                error="loop bad",
                start_at=datetime.now(UTC).replace(tzinfo=None),
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(error="loop bad"),
            ),
        ),
    )

    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, LoopExecutionFailed)
    assert resume_task.result.error == "loop bad"
    assert graph_execution.has_error is False


def test_iteration_container_handler_starts_frame_from_iteration_await() -> None:
    graph_config = {
        "nodes": [
            {
                "id": "iteration-start",
                "data": {"type": BuiltinNodeTypes.ITERATION_START},
            },
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    variable_pool = VariablePool()
    variable_pool.add(["source", "items"], ["a", "b"])
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    iteration_node = IterationNode.__new__(IterationNode)
    iteration_node.init_node_identity("iteration")
    iteration_node.init_node_data({
        "type": "iteration",
        "start_node_id": "iteration-start",
        "iterator_selector": ["source", "items"],
        "output_selector": ["answer", "text"],
        "error_handle_mode": ErrorHandleMode.TERMINATED,
        "is_parallel": False,
    })
    iteration_node.bind_execution_id("iteration-run")
    iteration_node.graph_runtime_state = runtime_state
    iteration_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"iteration": iteration_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    container_handler = IterationContainerHandler(
        frame_registry=frame_registry,
        graph_execution=graph_execution,
    )

    _start_iteration_await(
        container_handler,
        runtime_state,
        invocation_id="iteration-invocation",
        indexes=(0,),
        items=("a", "b"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=True,
        parallel_nums=1,
    )

    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-run:iteration:0",
        node_id="iteration-start",
    )
    child_frame = frame_registry.get("iteration-run:iteration:0")
    assert (
        _variable_value(
            child_frame.graph_runtime_state,
            ["iteration", "index"],
        )
        == 0
    )
    assert (
        _variable_value(
            child_frame.graph_runtime_state,
            ["iteration", "item"],
        )
        == "a"
    )
    frame_state = runtime_state.get_container_frame("iteration-run:iteration:0")
    assert isinstance(frame_state, ContainerFrameState)
    assert frame_state.kind == "iteration"
    assert frame_state.phase_data["index"] == 0
    run_state = runtime_state.get_container_run("iteration-invocation")
    assert run_state.phase_data["scheduled_count"] == 1


def test_event_handler_suppresses_iteration_start_and_aggregates_success() -> None:  # noqa: PLR0914
    graph_config = {
        "nodes": [
            {
                "id": "iteration-start",
                "data": {"type": BuiltinNodeTypes.ITERATION_START},
            },
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    variable_pool = VariablePool()
    variable_pool.add(["source", "items"], ["a"])
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    iteration_node = IterationNode.__new__(IterationNode)
    iteration_node.init_node_identity("iteration")
    iteration_node.init_node_data({
        "type": "iteration",
        "start_node_id": "iteration-start",
        "iterator_selector": ["source", "items"],
        "output_selector": ["answer", "text"],
        "error_handle_mode": ErrorHandleMode.TERMINATED,
        "is_parallel": False,
    })
    iteration_node.bind_execution_id("iteration-run")
    iteration_node.graph_runtime_state = runtime_state
    iteration_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"iteration": iteration_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    event_collector = MagicMock()
    handler, container_handlers = _event_handler_with_container(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    _start_iteration_await(
        container_handlers["iteration"],
        runtime_state,
        invocation_id="iteration-invocation",
        indexes=(0,),
        items=("a",),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=True,
        parallel_nums=1,
    )
    _ = ready_queue.get(timeout=0.01)
    child_frame = frame_registry.get("iteration-run:iteration:0")
    child_frame.graph_runtime_state.variable_pool.add(
        ["answer", "text"],
        StringSegment(value="done"),
    )
    event_collector.collect.reset_mock()

    child_started = NodeRunStartedEvent(
        id="iteration-start-run",
        node_id="iteration-start",
        node_type=BuiltinNodeTypes.ITERATION_START,
        node_title="Iteration Start",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    child_succeeded = NodeRunSucceededEvent(
        id="iteration-start-run",
        node_id="iteration-start",
        node_type=BuiltinNodeTypes.ITERATION_START,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(),
    )

    handler.dispatch(
        TaskEvent(frame_id="iteration-run:iteration:0", event=child_started),
    )
    handler.dispatch(
        TaskEvent(frame_id="iteration-run:iteration:0", event=child_succeeded),
    )

    collected_events = [call.args[0] for call in event_collector.collect.call_args_list]
    assert not any(item.node_id == "iteration-start" for item in collected_events)
    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, IterationExecutionSucceeded)
    assert resume_task.result.outputs == {"output": ["done"]}


def test_event_handler_runs_sequential_iteration_frames_in_order() -> None:  # noqa: PLR0914
    graph_config = {
        "nodes": [
            {
                "id": "iteration-start",
                "data": {"type": BuiltinNodeTypes.ITERATION_START},
            },
            {"id": "body", "data": {"type": BuiltinNodeTypes.CODE}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    variable_pool = VariablePool()
    variable_pool.add(["source", "items"], ["a", "b"])
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    iteration_node = IterationNode.__new__(IterationNode)
    iteration_node.init_node_identity("iteration")
    iteration_node.init_node_data({
        "type": "iteration",
        "start_node_id": "iteration-start",
        "iterator_selector": ["source", "items"],
        "output_selector": ["answer", "text"],
        "error_handle_mode": ErrorHandleMode.TERMINATED,
        "is_parallel": False,
    })
    iteration_node.bind_execution_id("iteration-run")
    iteration_node.graph_runtime_state = runtime_state
    iteration_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"iteration": iteration_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    event_collector = MagicMock()
    handler, container_handlers = _event_handler_with_container(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    _start_iteration_await(
        container_handlers["iteration"],
        runtime_state,
        invocation_id="iteration-invocation",
        indexes=(0,),
        items=("a", "b"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=True,
        parallel_nums=1,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-run:iteration:0",
        node_id="iteration-start",
    )
    first_frame = frame_registry.get("iteration-run:iteration:0")
    first_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "first")

    first_child_succeeded = NodeRunSucceededEvent(
        id="iteration-start-run-0",
        node_id="iteration-start",
        node_type=BuiltinNodeTypes.ITERATION_START,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(),
    )
    handler.dispatch(
        TaskEvent(frame_id="iteration-run:iteration:0", event=first_child_succeeded),
    )

    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, IterationFramesRequested)
    assert resume_task.result.indexes == (1,)
    _start_iteration_await(
        container_handlers["iteration"],
        runtime_state,
        invocation_id=resume_task.invocation_id,
        indexes=resume_task.result.indexes,
        items=("a", "b"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=True,
        parallel_nums=1,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-run:iteration:1",
        node_id="iteration-start",
    )
    second_frame = frame_registry.get("iteration-run:iteration:1")
    second_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "second")

    child_body_started = NodeRunStartedEvent(
        id="body-run-1",
        node_id="body",
        node_type=BuiltinNodeTypes.CODE,
        node_title="Body",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    second_child_succeeded = NodeRunSucceededEvent(
        id="iteration-start-run-1",
        node_id="iteration-start",
        node_type=BuiltinNodeTypes.ITERATION_START,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(),
    )
    handler.dispatch(
        TaskEvent(frame_id="iteration-run:iteration:1", event=child_body_started),
    )
    handler.dispatch(
        TaskEvent(frame_id="iteration-run:iteration:1", event=second_child_succeeded),
    )

    assert child_body_started.in_iteration_id == "iteration"
    assert (
        child_body_started.node_run_result.metadata[
            WorkflowNodeExecutionMetadataKey.ITERATION_INDEX
        ]
        == 1
    )
    final_resume_task = _get_resume_task(ready_queue)
    assert isinstance(final_resume_task.result, IterationExecutionSucceeded)
    assert final_resume_task.result.outputs == {"output": ["first", "second"]}


def test_event_handler_preserves_nested_iteration_outputs_when_flatten_disabled() -> (
    None
):
    graph_config = {
        "nodes": [
            {
                "id": "iteration-start",
                "data": {"type": BuiltinNodeTypes.ITERATION_START},
            },
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    variable_pool = VariablePool()
    variable_pool.add(["source", "items"], ["a", "b"])
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    iteration_node = IterationNode.__new__(IterationNode)
    iteration_node.init_node_identity("iteration")
    iteration_node.init_node_data({
        "type": "iteration",
        "start_node_id": "iteration-start",
        "iterator_selector": ["source", "items"],
        "output_selector": ["answer", "text"],
        "error_handle_mode": ErrorHandleMode.TERMINATED,
        "is_parallel": False,
        "flatten_output": False,
    })
    iteration_node.bind_execution_id("iteration-run")
    iteration_node.graph_runtime_state = runtime_state
    iteration_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"iteration": iteration_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    handler, container_handlers = _event_handler_with_container(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, MagicMock()),
        frame_registry=frame_registry,
    )
    _start_iteration_await(
        container_handlers["iteration"],
        runtime_state,
        invocation_id="iteration-invocation",
        indexes=(0,),
        items=("a", "b"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=False,
        parallel_nums=1,
    )
    _ = ready_queue.get(timeout=0.01)

    first_frame = frame_registry.get("iteration-run:iteration:0")
    first_frame.graph_runtime_state.variable_pool.add(["answer", "text"], ["first"])
    handler.dispatch(
        TaskEvent(
            frame_id="iteration-run:iteration:0",
            event=NodeRunSucceededEvent(
                id="iteration-start-run-0",
                node_id="iteration-start",
                node_type=BuiltinNodeTypes.ITERATION_START,
                start_at=datetime.now(UTC).replace(tzinfo=None),
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(),
            ),
        ),
    )

    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, IterationFramesRequested)
    _start_iteration_await(
        container_handlers["iteration"],
        runtime_state,
        invocation_id=resume_task.invocation_id,
        indexes=resume_task.result.indexes,
        items=("a", "b"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=False,
        parallel_nums=1,
    )
    _ = ready_queue.get(timeout=0.01)
    second_frame = frame_registry.get("iteration-run:iteration:1")
    second_frame.graph_runtime_state.variable_pool.add(["answer", "text"], ["second"])
    handler.dispatch(
        TaskEvent(
            frame_id="iteration-run:iteration:1",
            event=NodeRunSucceededEvent(
                id="iteration-start-run-1",
                node_id="iteration-start",
                node_type=BuiltinNodeTypes.ITERATION_START,
                start_at=datetime.now(UTC).replace(tzinfo=None),
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(),
            ),
        ),
    )

    final_resume_task = _get_resume_task(ready_queue)
    assert isinstance(final_resume_task.result, IterationExecutionSucceeded)
    assert final_resume_task.result.outputs["output"] == [
        ["first"],
        ["second"],
    ]


def test_event_handler_completes_empty_iteration_after_parent_success_event() -> None:
    ready_queue = InMemoryReadyQueue()
    variable_pool = VariablePool()
    variable_pool.add(["source", "items"], [])
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    iteration_node = IterationNode.__new__(IterationNode)
    iteration_node.init_node_identity("iteration")
    iteration_node.init_node_data({
        "type": "iteration",
        "start_node_id": "iteration-start",
        "iterator_selector": ["source", "items"],
        "output_selector": ["answer", "text"],
        "error_handle_mode": ErrorHandleMode.TERMINATED,
        "is_parallel": False,
    })
    iteration_node.graph_runtime_state = runtime_state
    graph = SimpleNamespace(nodes={"iteration": iteration_node})
    frame_registry = FrameRegistry()
    state_manager = MagicMock()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
            state_manager=state_manager,
        ),
    )
    event_collector = MagicMock()
    handler = _event_handler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    iteration_started = NodeRunStartedEvent(
        id="iteration-run",
        node_id="iteration",
        node_type=BuiltinNodeTypes.ITERATION,
        node_title="Iteration",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    iteration_succeeded = NodeRunSucceededEvent(
        id="iteration-run",
        node_id="iteration",
        node_type=BuiltinNodeTypes.ITERATION,
        start_at=iteration_started.start_at,
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(outputs={"output": []}),
    )

    handler.dispatch(TaskEvent(frame_id="root", event=iteration_started))
    handler.dispatch(TaskEvent(frame_id="root", event=iteration_succeeded))

    assert _variable_value(runtime_state, ["iteration", "output"]) == []
    state_manager.finish_execution.assert_called_once_with(
        frame_id="root",
        node_id="iteration",
    )
    collected_events = [call.args[0] for call in event_collector.collect.call_args_list]
    assert any(
        isinstance(item, NodeRunSucceededEvent) and item.node_id == "iteration"
        for item in collected_events
    )


def test_event_handler_continues_iteration_after_child_frame_failure() -> None:
    graph_config = {
        "nodes": [
            {
                "id": "iteration-start",
                "data": {"type": BuiltinNodeTypes.ITERATION_START},
            },
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    variable_pool = VariablePool()
    variable_pool.add(["source", "items"], ["a", "b"])
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    iteration_node = IterationNode.__new__(IterationNode)
    iteration_node.init_node_identity("iteration")
    iteration_node.init_node_data({
        "type": "iteration",
        "start_node_id": "iteration-start",
        "iterator_selector": ["source", "items"],
        "output_selector": ["answer", "text"],
        "error_handle_mode": ErrorHandleMode.CONTINUE_ON_ERROR,
        "is_parallel": False,
    })
    iteration_node.bind_execution_id("iteration-run")
    iteration_node.graph_runtime_state = runtime_state
    iteration_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"iteration": iteration_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    event_collector = MagicMock()
    handler, container_handlers = _event_handler_with_container(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    _start_iteration_await(
        container_handlers["iteration"],
        runtime_state,
        invocation_id="iteration-invocation",
        indexes=(0,),
        items=("a", "b"),
        error_handle_mode=ErrorHandleMode.CONTINUE_ON_ERROR,
        flatten_output=True,
        parallel_nums=1,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-run:iteration:0",
        node_id="iteration-start",
    )

    first_failed = NodeRunFailedEvent(
        id="iteration-start-run-0",
        node_id="iteration-start",
        node_type=BuiltinNodeTypes.ITERATION_START,
        error="bad item",
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(error="bad item"),
    )
    handler.dispatch(
        TaskEvent(frame_id="iteration-run:iteration:0", event=first_failed),
    )

    assert graph_execution.has_error is False
    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, IterationFramesRequested)
    assert resume_task.result.indexes == (1,)
    _start_iteration_await(
        container_handlers["iteration"],
        runtime_state,
        invocation_id=resume_task.invocation_id,
        indexes=resume_task.result.indexes,
        items=("a", "b"),
        error_handle_mode=ErrorHandleMode.CONTINUE_ON_ERROR,
        flatten_output=True,
        parallel_nums=1,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-run:iteration:1",
        node_id="iteration-start",
    )
    second_frame = frame_registry.get("iteration-run:iteration:1")
    second_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "ok")
    handler.dispatch(
        TaskEvent(
            frame_id="iteration-run:iteration:1",
            event=NodeRunSucceededEvent(
                id="iteration-start-run-1",
                node_id="iteration-start",
                node_type=BuiltinNodeTypes.ITERATION_START,
                start_at=datetime.now(UTC).replace(tzinfo=None),
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(),
            ),
        ),
    )

    final_resume_task = _get_resume_task(ready_queue)
    assert isinstance(final_resume_task.result, IterationExecutionSucceeded)
    assert final_resume_task.result.outputs["output"] == [
        None,
        "ok",
    ]


def test_event_handler_limits_parallel_iteration_and_preserves_output_order() -> None:  # noqa: PLR0914
    graph_config = {
        "nodes": [
            {
                "id": "iteration-start",
                "data": {"type": BuiltinNodeTypes.ITERATION_START},
            },
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    variable_pool = VariablePool()
    variable_pool.add(["source", "items"], ["a", "b", "c"])
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    iteration_node = IterationNode.__new__(IterationNode)
    iteration_node.init_node_identity("iteration")
    iteration_node.init_node_data({
        "type": "iteration",
        "start_node_id": "iteration-start",
        "iterator_selector": ["source", "items"],
        "output_selector": ["answer", "text"],
        "error_handle_mode": ErrorHandleMode.TERMINATED,
        "is_parallel": True,
        "parallel_nums": 2,
    })
    iteration_node.bind_execution_id("iteration-run")
    iteration_node.graph_runtime_state = runtime_state
    iteration_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"iteration": iteration_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    event_collector = MagicMock()
    handler, container_handlers = _event_handler_with_container(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )

    _start_iteration_await(
        container_handlers["iteration"],
        runtime_state,
        invocation_id="iteration-invocation",
        indexes=(0, 1),
        items=("a", "b", "c"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=True,
        parallel_nums=2,
    )

    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-run:iteration:0",
        node_id="iteration-start",
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-run:iteration:1",
        node_id="iteration-start",
    )
    assert ready_queue.empty() is True

    second_frame = frame_registry.get("iteration-run:iteration:1")
    second_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "second")
    handler.dispatch(
        TaskEvent(
            frame_id="iteration-run:iteration:1",
            event=NodeRunSucceededEvent(
                id="iteration-start-run-1",
                node_id="iteration-start",
                node_type=BuiltinNodeTypes.ITERATION_START,
                start_at=datetime.now(UTC).replace(tzinfo=None),
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(),
            ),
        ),
    )

    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, IterationFramesRequested)
    assert resume_task.result.indexes == (2,)
    _start_iteration_await(
        container_handlers["iteration"],
        runtime_state,
        invocation_id=resume_task.invocation_id,
        indexes=resume_task.result.indexes,
        items=("a", "b", "c"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=True,
        parallel_nums=2,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-run:iteration:2",
        node_id="iteration-start",
    )

    first_frame = frame_registry.get("iteration-run:iteration:0")
    first_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "first")
    handler.dispatch(
        TaskEvent(
            frame_id="iteration-run:iteration:0",
            event=NodeRunSucceededEvent(
                id="iteration-start-run-0",
                node_id="iteration-start",
                node_type=BuiltinNodeTypes.ITERATION_START,
                start_at=datetime.now(UTC).replace(tzinfo=None),
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(),
            ),
        ),
    )

    third_frame = frame_registry.get("iteration-run:iteration:2")
    third_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "third")
    handler.dispatch(
        TaskEvent(
            frame_id="iteration-run:iteration:2",
            event=NodeRunSucceededEvent(
                id="iteration-start-run-2",
                node_id="iteration-start",
                node_type=BuiltinNodeTypes.ITERATION_START,
                start_at=datetime.now(UTC).replace(tzinfo=None),
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(),
            ),
        ),
    )

    final_resume_task = _get_resume_task(ready_queue)
    assert isinstance(final_resume_task.result, IterationExecutionSucceeded)
    assert final_resume_task.result.outputs["output"] == [
        "first",
        "second",
        "third",
    ]


def test_iteration_frame_completion_updates_run_while_parent_resume_claimed() -> None:
    graph_config = {
        "nodes": [
            {
                "id": "iteration-start",
                "data": {"type": BuiltinNodeTypes.ITERATION_START},
            },
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    variable_pool = VariablePool()
    variable_pool.add(["source", "items"], ["a", "b", "c"])
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    iteration_node = IterationNode.__new__(IterationNode)
    iteration_node.init_node_identity("iteration")
    iteration_node.init_node_data({
        "type": "iteration",
        "start_node_id": "iteration-start",
        "iterator_selector": ["source", "items"],
        "output_selector": ["answer", "text"],
        "error_handle_mode": ErrorHandleMode.TERMINATED,
        "is_parallel": True,
        "parallel_nums": 2,
    })
    iteration_node.bind_execution_id("iteration-run")
    iteration_node.graph_runtime_state = runtime_state
    iteration_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"iteration": iteration_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    container_handler = IterationContainerHandler(
        frame_registry=frame_registry,
        graph_execution=graph_execution,
    )
    _start_iteration_await(
        container_handler,
        runtime_state,
        invocation_id="iteration-invocation",
        indexes=(0, 1),
        items=("a", "b", "c"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=True,
        parallel_nums=2,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-run:iteration:0",
        node_id="iteration-start",
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-run:iteration:1",
        node_id="iteration-start",
    )

    claimed_run = runtime_state.claim_container_run("iteration-invocation")
    sibling_frame = frame_registry.get("iteration-run:iteration:1")
    sibling_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "second")
    sibling_frame.state_manager.finish_execution(
        frame_id=sibling_frame.frame_id,
        node_id="iteration-start",
    )

    assert container_handler.complete_frame(sibling_frame) is True

    run_state = runtime_state.get_container_run("iteration-invocation")
    assert run_state.phase_data["completed_count"] == 1
    assert run_state.phase_data["outputs"] == {"1": "second"}
    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, IterationFramesRequested)
    assert resume_task.result.indexes == (2,)
    runtime_state.update_container_run_phase_data(
        "iteration-invocation",
        {
            "inputs": dict(
                cast(Mapping[str, object], claimed_run.phase_data["inputs"]),
            ),
            "items": claimed_run.phase_data["items"],
            "root_node_id": claimed_run.phase_data["root_node_id"],
            "output_selector": list(
                cast(Sequence[str], claimed_run.phase_data["output_selector"]),
            ),
            "error_handle_mode": claimed_run.phase_data["error_handle_mode"],
            "flatten_output": claimed_run.phase_data["flatten_output"],
            "parallel_nums": claimed_run.phase_data["parallel_nums"],
        },
    )
    run_state = runtime_state.get_container_run("iteration-invocation")
    assert run_state.phase_data["completed_count"] == 1
    assert run_state.phase_data["outputs"] == {"1": "second"}
    runtime_state.release_container_run_claim("iteration-invocation")
    runtime_state.pop_container_run("iteration-invocation")


@pytest.mark.parametrize(
    ("limit_type", "expected_reason"),
    [
        (LimitType.STEP_LIMIT, "Maximum execution steps exceeded: 4 > 3"),
        (LimitType.TIME_LIMIT, "Maximum execution time exceeded:"),
    ],
)
def test_execution_limits_layer_builds_abort_reason_with_match_case(
    limit_type: LimitType,
    expected_reason: str,
) -> None:
    layer = ExecutionLimitsLayer(max_steps=3, max_time=10)
    layer.command_channel = MagicMock()
    layer.on_graph_start()
    layer.step_count = 4
    layer.start_time = time() - 20

    layer.send_abort_command(limit_type)

    abort_command = layer.command_channel.send_command.call_args.args[0]
    assert isinstance(abort_command, AbortCommand)
    assert abort_command.reason is not None
    assert abort_command.reason.startswith(expected_reason)


def test_execution_limits_layer_matches_subclassed_node_start_event() -> None:
    class CustomNodeRunStartedEvent(NodeRunStartedEvent):
        pass

    layer = ExecutionLimitsLayer(max_steps=3, max_time=10)
    layer.on_graph_start()

    layer.on_event(
        CustomNodeRunStartedEvent(
            id="node-run-1",
            node_id="node-1",
            node_type=BuiltinNodeTypes.CODE,
            node_title="Code",
            start_at=datetime.now(UTC).replace(tzinfo=None),
        ),
    )

    assert layer.step_count == 1
