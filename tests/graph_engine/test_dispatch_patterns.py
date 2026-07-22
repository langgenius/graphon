import queue
import threading
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime
from time import time
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock

import pytest

from graphon.entities.graph_init_params import GraphInitParams
from graphon.entities.pause_reason import HitlRequired
from graphon.enums import (
    BuiltinNodeTypes,
    ErrorHandleMode,
    NodeExecutionType,
    NodeState,
    NodeType,
)
from graphon.graph.graph import Graph
from graphon.graph_engine.command_channels.redis_channel import RedisChannel
from graphon.graph_engine.config import GraphEngineConfig
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
from graphon.graph_engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.graph_engine.ready_queue.protocol import (
    ReadyTask,
    ResumeTask,
    StartTask,
)
from graphon.graph_engine.worker import Worker
from graphon.graph_engine.worker_management import WorkerPool
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import (
    NodeRunPauseRequestedEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.base import NodeRunResult
from graphon.nodes.container_effects import (
    ContainerExecutionResult,
    IterationFrameRequest,
    LoopFrameRequest,
    build_container_value,
)
from graphon.nodes.human_input.entities import PauseRequested
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.runtime.container_state import (
    FrameRuntimeData,
    IterationFrameState,
    IterationRunState,
    create_container_run_state,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool


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
) -> dict[str, ContainerHandler]:
    return {
        "loop": LoopContainerHandler(
            frame_registry=frame_registry,
        ),
        "iteration": IterationContainerHandler(
            frame_registry=frame_registry,
        ),
    }


def _get_resume_task(ready_queue: InMemoryReadyQueue) -> ResumeTask:
    task = ready_queue.get(timeout=0.01)
    assert isinstance(task, ResumeTask)
    return task


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
        items=tuple(build_container_value(item) for item in items),
        root_node_id="iteration-start",
        indexes=indexes,
        output_selector=("answer", "text"),
        error_handle_mode=error_handle_mode,
        flatten_output=flatten_output,
        parallel_nums=parallel_nums,
    )
    runtime_state.put_container_run(
        create_container_run_state(
            invocation_id=invocation_id,
            frame_id="root",
            node_id="iteration",
            started_at=datetime.now(UTC).replace(tzinfo=None),
            request=request,
        ),
    )
    container_handler.start_await(
        invocation_id=invocation_id,
        request=request,
    )


def _worker(
    *,
    ready_queue: InMemoryReadyQueue,
    event_queue: queue.Queue[TaskEvent],
    frame_registry: FrameRegistry,
) -> Worker:
    task_claiming = threading.Event()
    task_claiming.set()
    return Worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
        container_handlers={},
        task_claim_lock=threading.Lock(),
        task_claiming=task_claiming,
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
        frame_id="root",
    )

    manager.enqueue_node("start")

    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="root",
        node_id="start",
    )
    assert graph.nodes["start"].state == NodeState.TAKEN
    assert not manager.is_execution_complete()


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
        frame_id="root",
    )

    manager.enqueue_node("start")

    assert ready_queue.qsize() == 0
    assert runtime_state.drain_deferred_ready_tasks() == [
        StartTask(frame_id="root", node_id="start"),
    ]
    assert graph.nodes["start"].state == NodeState.TAKEN
    assert not manager.is_execution_complete()


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
        frame_id="root",
    )

    assert manager.is_execution_complete() is True


def test_pause_defers_queued_tasks_without_losing_frame_progress() -> None:
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
        frame_id="root",
    )
    manager.enqueue_node("active")
    manager.enqueue_node("queued")
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="root",
        node_id="active",
    )
    graph_execution.paused = True
    worker_pool = MagicMock()
    worker_pool.drain.side_effect = ready_queue.drain
    dispatcher = Dispatcher(
        event_queue=queue.Queue(),
        event_handler=MagicMock(),
        graph_execution=graph_execution,
        state_manager=manager,
        command_processor=MagicMock(),
        worker_pool=worker_pool,
        event_emitter=MagicMock(),
    )

    assert dispatcher._run_until_exit()

    assert ready_queue.qsize() == 0
    assert runtime_state.drain_deferred_ready_tasks() == [
        StartTask(frame_id="root", node_id="queued")
    ]
    assert not manager.is_execution_complete()
    manager.finish_execution("active")
    assert not manager.is_execution_complete()
    manager.finish_execution("queued")
    assert manager.is_execution_complete()
    worker_pool.drain.assert_called_once_with()
    worker_pool.stop.assert_not_called()


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
    pool._task_claim_lock = threading.Lock()
    pool._task_claiming = threading.Event()
    pool._task_claiming.set()
    pool._ready_queue = InMemoryReadyQueue()
    pool._running = True
    pool._workers = [active_worker, idle_worker]

    pool.drain()

    assert active_worker.stopped is False
    assert idle_worker.stopped is True


def test_worker_pool_drain_observes_task_claimed_during_pause() -> None:  # noqa: C901
    class BlockingReadyQueue:
        def __init__(self) -> None:
            self._queue = InMemoryReadyQueue()
            self.task_removed = threading.Event()
            self.release_claim = threading.Event()

        def put(self, item: ReadyTask) -> None:
            self._queue.put(item)

        def get(self, timeout: float | None = None) -> ReadyTask:
            task = self._queue.get(timeout)
            self.task_removed.set()
            if not self.release_claim.wait(timeout=1):
                msg = "task claim was not released"
                raise TimeoutError(msg)
            return task

        def task_done(self) -> None:
            self._queue.task_done()

        def qsize(self) -> int:
            return self._queue.qsize()

        def drain(self) -> list[ReadyTask]:
            return self._queue.drain()

        def dumps(self) -> str:
            return self._queue.dumps()

        def loads(self, data: str) -> None:
            self._queue.loads(data)

    class BlockingNode:
        id = "node"
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE
        execution_id = "pending"

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(self) -> Generator[NodeRunSucceededEvent, None, None]:
            node_started.set()
            if not finish_node.wait(timeout=1):
                msg = "node was not released"
                raise TimeoutError(msg)
            now = datetime.now(UTC).replace(tzinfo=None)
            yield NodeRunSucceededEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                start_at=now,
                finished_at=now,
                node_run_result=NodeRunResult(),
            )

    ready_queue = BlockingReadyQueue()
    ready_queue.put(StartTask(frame_id="root", node_id="node"))
    node_started = threading.Event()
    finish_node = threading.Event()
    event_queue: queue.Queue[TaskEvent] = queue.Queue()
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
            graph=cast(Graph, SimpleNamespace(nodes={"node": BlockingNode()})),
            graph_runtime_state=runtime_state,
        ),
    )
    pool = WorkerPool(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
        config=GraphEngineConfig(max_workers=1),
        container_handlers={},
    )
    drained_tasks: list[ReadyTask] = []
    drain_done = threading.Event()

    def drain_pool() -> None:
        drained_tasks.extend(pool.drain())
        drain_done.set()

    pool.start()
    drain_thread = threading.Thread(target=drain_pool)
    try:
        assert ready_queue.task_removed.wait(timeout=1)
        drain_thread.start()
        assert not drain_done.wait(timeout=0.05)
        ready_queue.release_claim.set()
        assert node_started.wait(timeout=1)
        assert drain_done.wait(timeout=1)
        assert drained_tasks == []
        assert pool.has_current_tasks()
    finally:
        ready_queue.release_claim.set()
        finish_node.set()
        drain_thread.join(timeout=1)
        pool.stop()


def test_worker_pool_scales_for_one_queued_sibling() -> None:
    class ParallelNode:
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE
        execution_id = "pending"

        def __init__(self, node_id: str) -> None:
            self.id = node_id

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(self) -> Generator[NodeRunSucceededEvent, None, None]:
            first_node_started.set()
            barrier.wait(timeout=1)
            now = datetime.now(UTC).replace(tzinfo=None)
            yield NodeRunSucceededEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                start_at=now,
                finished_at=now,
                node_run_result=NodeRunResult(),
            )

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="root", node_id="first"))
    ready_queue.put(StartTask(frame_id="root", node_id="second"))
    event_queue: queue.Queue[TaskEvent] = queue.Queue()
    first_node_started = threading.Event()
    barrier = threading.Barrier(2)
    graph = SimpleNamespace(
        nodes={
            "first": ParallelNode("first"),
            "second": ParallelNode("second"),
        },
    )
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
    pool = WorkerPool(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
        config=GraphEngineConfig(max_workers=2),
        container_handlers={},
    )

    pool.start()
    try:
        assert first_node_started.wait(timeout=1)
        pool.check_and_scale()
        events = [event_queue.get(timeout=1), event_queue.get(timeout=1)]
    finally:
        pool.stop()

    assert {event.event.node_id for event in events} == {"first", "second"}
    assert all(isinstance(event.event, NodeRunSucceededEvent) for event in events)


def test_worker_with_current_task_is_not_idle() -> None:
    worker = object.__new__(Worker)
    worker._has_current_task = threading.Event()
    worker._has_current_task.set()
    worker._last_task_time = 0

    assert not worker.is_idle


def test_resume_tracks_live_and_deferred_start_tasks_before_starting_workers() -> None:
    ready_queue = InMemoryReadyQueue()
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
    )
    live_task = StartTask(frame_id="root", node_id="live")
    deferred_task = StartTask(frame_id="root", node_id="deferred")
    ready_queue.put(live_task)
    runtime_state.defer_ready_task(deferred_task)
    engine = object.__new__(GraphEngine)
    engine._worker_pool = MagicMock()
    engine._graph_runtime_state = runtime_state
    state_manager = MagicMock()
    engine._frame_registry = MagicMock()
    engine._frame_registry.get.return_value.state_manager = state_manager
    engine._dispatcher = MagicMock()

    engine._start_execution(resume=True)

    assert ready_queue.get(timeout=0.01) == live_task
    assert ready_queue.get(timeout=0.01) == deferred_task
    with pytest.raises(queue.Empty):
        ready_queue.get(timeout=0.01)
    assert state_manager.track_unfinished.call_args_list == [
        ((live_task.node_id,), {}),
        ((deferred_task.node_id,), {}),
    ]


def test_pause_requested_event_defers_current_task_for_resume() -> None:
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    child_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
        deferred_ready_queue=root_runtime_state.deferred_ready_queue,
        graph_execution=graph_execution,
    )
    graph_init_params = GraphInitParams(
        workflow_id="workflow",
        graph_config={},
        run_context={},
        call_depth=0,
    )
    iteration_node = IterationNode(
        node_id="iteration",
        data=IterationNode.validate_node_data({
            "type": "iteration",
            "start_node_id": "human",
            "iterator_selector": ["start", "items"],
            "output_selector": ["human", "output"],
            "is_parallel": False,
            "parallel_nums": 1,
            "error_handle_mode": ErrorHandleMode.TERMINATED,
            "flatten_output": False,
        }),
        graph_init_params=graph_init_params,
        graph_runtime_state=root_runtime_state,
    )
    root_graph = Graph(
        root_node=iteration_node,
        nodes={"iteration": iteration_node},
    )
    human_node = HumanInputNode(
        node_id="human",
        data=HumanInputNode.validate_node_data({
            "type": "human-input",
            "title": "Human",
        }),
        graph_init_params=graph_init_params,
        graph_runtime_state=child_runtime_state,
        hitl_callback=lambda _context: PauseRequested(session_id="unused"),
    )
    child_graph = Graph(
        root_node=human_node,
        nodes={"human": human_node},
    )
    child_runtime_state.attach_graph(child_graph)
    request = IterationFrameRequest(
        items=(build_container_value("input"),),
        root_node_id="human",
        indexes=(0,),
        output_selector=("iteration", "item"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=False,
        parallel_nums=1,
    )
    root_runtime_state.put_container_run(
        create_container_run_state(
            invocation_id="iteration-invocation",
            frame_id="root",
            node_id="iteration",
            started_at=datetime.now(UTC).replace(tzinfo=None),
            request=request,
        ),
    )
    root_runtime_state.put_container_frame(
        IterationFrameState(
            frame_id="child-frame",
            parent_invocation_id="iteration-invocation",
            root_node_id="human",
            index=0,
            started_at=datetime.now(UTC).replace(tzinfo=None),
            runtime_data=child_runtime_state.snapshot_frame(),
        )
    )
    state_manager = GraphStateManager(
        graph=child_graph,
        graph_runtime_state=child_runtime_state,
        frame_id="child-frame",
    )
    frame_registry = FrameRegistry()
    event_collector = MagicMock()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=root_graph,
            graph_runtime_state=root_runtime_state,
        ),
    )
    frame_registry.register(
        _execution_frame(
            frame_id="child-frame",
            graph=child_graph,
            graph_runtime_state=child_runtime_state,
            state_manager=state_manager,
        ),
    )
    state_manager.track_unfinished("human")
    handler = _event_handler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )

    handler.dispatch(
        TaskEvent(
            frame_id="child-frame",
            event=NodeRunPauseRequestedEvent(
                id="human-run",
                node_id="human",
                node_type=BuiltinNodeTypes.HUMAN_INPUT,
                reason=HitlRequired(
                    session_id="session-1",
                    node_id="human",
                    node_title="Human",
                ),
            ),
        )
    )

    assert graph_execution.paused
    assert not state_manager.is_execution_complete()
    assert root_runtime_state.drain_deferred_ready_tasks() == [
        StartTask(frame_id="child-frame", node_id="human")
    ]
    assert (
        root_runtime_state.get_container_frame("child-frame").frame_id == "child-frame"
    )


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

    assert first is not second
    assert (
        execution.node_executions["iteration-0", "answer"].execution_id
        == first.execution_id
    )
    assert (
        execution.node_executions["iteration-1", "answer"].execution_id
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
    frame_state = IterationFrameState(
        frame_id="child-frame",
        parent_invocation_id="iteration-invocation",
        root_node_id="start",
        index=0,
        started_at=datetime.now(UTC).replace(tzinfo=None),
        runtime_data=FrameRuntimeData(
            variable_pool=variable_pool,
            outputs={"answer": "saved"},
            llm_usage=LLMUsage.empty_usage(),
            node_run_steps=2,
            graph_node_states={"start": NodeState.TAKEN},
            graph_edge_states={},
        ),
    )

    child_frame = frame_registry.materialize_child_frame_from_state(
        frame_state,
        variable_pool=cast(
            VariablePool,
            frame_state.runtime_data.variable_pool,
        ).model_copy(deep=True),
    )

    assert frame_registry.has("child-frame")
    assert child_frame.frame_id == "child-frame"
    assert _variable_value(child_frame.graph_runtime_state, ["child", "value"]) == (
        "saved"
    )
    assert child_frame.graph_runtime_state.outputs == {"answer": "saved"}
    assert child_frame.graph_runtime_state.node_run_steps == 2
    assert child_frame.graph.nodes["start"].state == NodeState.TAKEN

    graph_execution.pause(
        HitlRequired(
            session_id="session-1",
            node_id="start",
            node_title="Start",
        )
    )
    deferred_task = StartTask(frame_id="child-frame", node_id="start")
    child_frame.graph_runtime_state.enqueue_ready_task(deferred_task)
    assert root_runtime_state.drain_deferred_ready_tasks() == [deferred_task]


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
    frame_state = IterationFrameState(
        frame_id="child-frame",
        parent_invocation_id="iteration-invocation",
        root_node_id="start",
        index=0,
        started_at=datetime.now(UTC).replace(tzinfo=None),
        runtime_data=FrameRuntimeData(
            variable_pool=VariablePool(),
            outputs={},
            llm_usage=LLMUsage.empty_usage(),
            node_run_steps=0,
            graph_node_states={"missing-node": NodeState.TAKEN},
            graph_edge_states={"missing-edge": NodeState.TAKEN},
        ),
    )

    with pytest.raises(RuntimeError, match=r"missing-node.*missing-edge"):
        frame_registry.materialize_child_frame_from_state(
            frame_state,
            variable_pool=cast(
                VariablePool,
                frame_state.runtime_data.variable_pool,
            ).model_copy(deep=True),
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
    frame_state = IterationFrameState(
        frame_id="child-frame",
        parent_invocation_id="iteration-invocation",
        root_node_id="start",
        index=0,
        started_at=datetime.now(UTC).replace(tzinfo=None),
        runtime_data=FrameRuntimeData(
            variable_pool=variable_pool,
            outputs={"nested": {"value": "saved"}},
            llm_usage=LLMUsage.empty_usage(),
            node_run_steps=0,
            graph_node_states={},
            graph_edge_states={},
        ),
    )

    child_frame = frame_registry.materialize_child_frame_from_state(
        frame_state,
        variable_pool=cast(
            VariablePool,
            frame_state.runtime_data.variable_pool,
        ).model_copy(deep=True),
    )
    child_frame.graph_runtime_state.variable_pool.add(["child", "value"], "changed")
    child_frame.graph_runtime_state.set_output("nested", {"value": "changed"})

    saved_variable = cast(
        VariablePool,
        frame_state.runtime_data.variable_pool,
    ).get(["child", "value"])
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
                inputs={"loop_count": build_container_value(1)},
                outputs={},
                loop_count=1,
                root_node_id="loop-start",
                loop_variable_selectors={},
                loop_node_ids=frozenset(),
                index=0,
            )
            self.body_after_await_was_consumed = True

    class RecordingContainerHandler:
        def __init__(self) -> None:
            self.request: LoopFrameRequest

        def start_await(
            self,
            *,
            invocation_id: str,
            request: LoopFrameRequest,
        ) -> None:
            _ = invocation_id
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
    task_claiming = threading.Event()
    task_claiming.set()
    worker = Worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
        container_handlers={"loop": cast(ContainerHandler, container_handler)},
        task_claim_lock=threading.Lock(),
        task_claiming=task_claiming,
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

    class RecordingEventHandler:
        dispatched_events: list[object]

        def __init__(self) -> None:
            self.dispatched_events = []

        def dispatch(self, event: object) -> None:
            self.dispatched_events.append(event)

    event_handler = RecordingEventHandler()
    graph_execution = MagicMock(
        aborted=False,
        paused=False,
        error=None,
        completed=False,
    )
    state_manager = MagicMock()
    state_manager.is_execution_complete.side_effect = lambda: bool(
        event_handler.dispatched_events
    )
    dispatcher = Dispatcher(
        event_queue=event_queue,
        event_handler=cast(EventHandler, event_handler),
        graph_execution=graph_execution,
        state_manager=state_manager,
        command_processor=MagicMock(),
        worker_pool=MagicMock(),
        event_emitter=MagicMock(),
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

    runtime_state.increment_node_run_steps.assert_called_once_with()
    event_collector.collect.assert_called_once_with(event)


def test_event_handler_processes_tagged_root_frame_success_before_collecting() -> None:
    graph = MagicMock()
    graph.nodes = {"child": MagicMock(execution_type=NodeExecutionType.EXECUTABLE)}
    runtime_state = MagicMock()
    runtime_state.variable_pool = MagicMock()
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value = MagicMock()
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
    state_manager.enqueue_node.assert_called_once_with("next")
    event_collector.collect.assert_called_once_with(event)


def test_parallel_iteration_preserves_aggregate_and_response_order() -> None:  # noqa: PLR0914
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
        frame_id="iteration-invocation:iteration:0",
        node_id="iteration-start",
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-invocation:iteration:1",
        node_id="iteration-start",
    )
    assert ready_queue.qsize() == 0

    second_frame = frame_registry.get("iteration-invocation:iteration:1")
    second_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "second")
    second_frame.graph_runtime_state.set_output("answer", "second")
    handler.dispatch(
        TaskEvent(
            frame_id="iteration-invocation:iteration:1",
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
    assert isinstance(resume_task.result, IterationFrameRequest)
    assert resume_task.result.indexes == (2,)
    container_handlers["iteration"].start_await(
        invocation_id=resume_task.invocation_id,
        request=resume_task.result,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-invocation:iteration:2",
        node_id="iteration-start",
    )

    third_frame = frame_registry.get("iteration-invocation:iteration:2")
    third_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "third")
    third_frame.graph_runtime_state.set_output("answer", "third")
    handler.dispatch(
        TaskEvent(
            frame_id="iteration-invocation:iteration:2",
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

    first_frame = frame_registry.get("iteration-invocation:iteration:0")
    first_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "first")
    first_frame.graph_runtime_state.set_output("answer", "first")
    handler.dispatch(
        TaskEvent(
            frame_id="iteration-invocation:iteration:0",
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

    final_resume_task = _get_resume_task(ready_queue)
    assert isinstance(final_resume_task.result, ContainerExecutionResult)
    assert final_resume_task.result.node_run_result.outputs["output"].to_object() == [
        "first",
        "second",
        "third",
    ]
    assert runtime_state.outputs["answer"] == "third"


def test_terminated_iteration_waits_for_all_scheduled_frames() -> None:
    ready_queue = InMemoryReadyQueue()
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
    )
    run_state = IterationRunState(
        invocation_id="iteration-invocation",
        frame_id="root",
        node_id="iteration",
        started_at=datetime.now(UTC).replace(tzinfo=None),
        items=(build_container_value("a"), build_container_value("b")),
        root_node_id="iteration-start",
        output_selector=("answer", "text"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=False,
        parallel_nums=2,
        scheduled_count=2,
        completed_count=1,
        errors=("bad item",),
    )
    runtime_state.put_container_run(run_state)
    frame_registry = MagicMock()
    frame_registry.get.return_value.graph_runtime_state = runtime_state
    handler = IterationContainerHandler(frame_registry=frame_registry)
    parent_frame = cast(
        ExecutionFrame,
        SimpleNamespace(graph_runtime_state=runtime_state),
    )

    assert handler._finish_failed_iteration_if_ready(
        parent_frame=parent_frame,
        run_state=run_state,
    )
    assert ready_queue.qsize() == 0

    run_state = run_state.model_copy(
        update={"completed_count": 2},
    )
    runtime_state.put_container_run(run_state)
    assert handler._finish_failed_iteration_if_ready(
        parent_frame=parent_frame,
        run_state=run_state,
    )
    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, ContainerExecutionResult)
    assert resume_task.result.node_run_result.error == "bad item"


def test_iteration_frame_completion_requests_next_index() -> None:
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
        frame_id="iteration-invocation:iteration:0",
        node_id="iteration-start",
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-invocation:iteration:1",
        node_id="iteration-start",
    )

    sibling_frame = frame_registry.get("iteration-invocation:iteration:1")
    sibling_frame.graph_runtime_state.variable_pool.add(["answer", "text"], "second")
    sibling_frame.state_manager.finish_execution("iteration-start")

    container_handler.complete_frame(sibling_frame)

    run_state = runtime_state.get_container_run("iteration-invocation")
    assert isinstance(run_state, IterationRunState)
    assert run_state.completed_count == 1
    assert run_state.outputs["1"].to_object() == "second"
    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, IterationFrameRequest)
    assert resume_task.result.indexes == (2,)


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
