import queue
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime
from time import time
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock

import pytest

from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    NodeState,
    NodeType,
    WorkflowNodeExecutionMetadataKey,
)
from graphon.graph.graph import Graph
from graphon.graph_engine.command_channels.redis_channel import RedisChannel
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
from graphon.graph_engine.graph_state_manager import GraphStateManager
from graphon.graph_engine.layers.execution_limits import (
    ExecutionLimitsLayer,
    LimitType,
)
from graphon.graph_engine.orchestration.dispatcher import Dispatcher
from graphon.graph_engine.orchestration.execution_coordinator import (
    ExecutionCoordinator as RealExecutionCoordinator,
)
from graphon.graph_engine.ready_queue.factory import create_ready_queue_from_state
from graphon.graph_engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.graph_engine.ready_queue.protocol import ReadyQueueState, ReadyTask
from graphon.graph_engine.worker import Worker
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.iteration import (
    NodeRunIterationNextEvent,
    NodeRunIterationStartedEvent,
    NodeRunIterationSucceededEvent,
)
from graphon.graph_events.loop import (
    NodeRunLoopFailedEvent,
    NodeRunLoopNextEvent,
    NodeRunLoopStartedEvent,
    NodeRunLoopSucceededEvent,
)
from graphon.graph_events.node import (
    NodeRunFailedEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.node_events.base import NodeRunResult
from graphon.nodes.iteration.entities import ErrorHandleMode
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.nodes.loop.loop_node import LoopNode
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
                ReadyTask(frame_id="root", node_id="start"),
                ReadyTask(frame_id="iteration-0", node_id="answer"),
            ],
        ),
    )

    assert queue.get(timeout=0.01) == ReadyTask(frame_id="root", node_id="start")
    assert queue.get(timeout=0.01) == ReadyTask(
        frame_id="iteration-0",
        node_id="answer",
    )


def test_graph_state_manager_enqueues_ready_task_for_frame() -> None:
    ready_queue = InMemoryReadyQueue()
    graph = SimpleNamespace(
        nodes={"start": SimpleNamespace(state=NodeState.UNKNOWN)},
    )
    manager = GraphStateManager(graph=cast(Graph, graph), ready_queue=ready_queue)

    manager.enqueue_node(frame_id="root", node_id="start")

    assert ready_queue.get(timeout=0.01) == ReadyTask(
        frame_id="root",
        node_id="start",
    )
    assert graph.nodes["start"].state == NodeState.TAKEN


def test_graph_state_manager_tracks_executing_tasks_by_frame() -> None:
    ready_queue = InMemoryReadyQueue()
    graph = SimpleNamespace(nodes={})
    manager = GraphStateManager(graph=cast(Graph, graph), ready_queue=ready_queue)

    manager.start_execution(frame_id="iteration-0", node_id="answer")
    manager.start_execution(frame_id="iteration-1", node_id="answer")
    manager.finish_execution(frame_id="iteration-0", node_id="answer")

    assert manager.get_executing_count() == 1
    assert manager.get_executing_nodes() == {
        ReadyTask(frame_id="iteration-1", node_id="answer"),
    }


def test_graph_state_manager_completion_ignores_other_frame_queue_items() -> None:
    ready_queue = InMemoryReadyQueue()
    ready_queue.put(ReadyTask(frame_id="other-frame", node_id="answer"))
    graph = SimpleNamespace(nodes={})
    manager = GraphStateManager(graph=cast(Graph, graph), ready_queue=ready_queue)

    assert manager.is_execution_complete() is True


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

    first.mark_started("run-0")
    second.mark_started("run-1")

    assert first is not second
    assert (
        execution.node_executions[
            ReadyTask(frame_id="iteration-0", node_id="answer")
        ].execution_id
        == "run-0"
    )
    assert (
        execution.node_executions[
            ReadyTask(frame_id="iteration-1", node_id="answer")
        ].execution_id
        == "run-1"
    )


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


def test_worker_executes_node_from_ready_task() -> None:
    class RunnableNode:
        id = "start"
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE
        execution_id = "run-1"

        def ensure_execution_id(self) -> str:
            return self.execution_id

        def run(self) -> Generator[NodeRunStartedEvent, None, None]:
            yield NodeRunStartedEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title="Start",
                start_at=datetime.now(UTC).replace(tzinfo=None),
            )

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(ReadyTask(frame_id="root", node_id="start"))
    event_queue = queue.Queue()
    graph = SimpleNamespace(nodes={"start": RunnableNode()})
    frame_registry = FrameRegistry()
    frame_registry.register(_execution_frame(frame_id="root", graph=cast(Graph, graph)))
    worker = Worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
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

        def ensure_execution_id(self) -> str:
            return self.execution_id

        def run(self) -> Generator[NodeRunStartedEvent, None, None]:
            yield NodeRunStartedEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title=self.title,
                start_at=datetime.now(UTC).replace(tzinfo=None),
            )

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(ReadyTask(frame_id="child", node_id="answer"))
    event_queue = queue.Queue()
    root_graph = SimpleNamespace(nodes={"answer": RunnableNode("answer", "Root")})
    child_graph = SimpleNamespace(nodes={"answer": RunnableNode("answer", "Child")})
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(frame_id="root", graph=cast(Graph, root_graph)),
    )
    frame_registry.register(
        _execution_frame(frame_id="child", graph=cast(Graph, child_graph)),
    )
    worker = Worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
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


def test_worker_container_task_only_emits_start_event() -> None:
    class ContainerNode:
        id = "loop"
        node_type = BuiltinNodeTypes.LOOP
        execution_type = NodeExecutionType.CONTAINER
        execution_id = "run-loop"

        def __init__(self) -> None:
            self.body_was_consumed = False

        def ensure_execution_id(self) -> str:
            return self.execution_id

        def run(self) -> Generator[NodeRunStartedEvent, None, None]:
            yield NodeRunStartedEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title="Loop",
                start_at=datetime.now(UTC).replace(tzinfo=None),
            )
            self.body_was_consumed = True
            msg = "container body must be interpreted by the engine"
            raise AssertionError(msg)

    container_node = ContainerNode()
    ready_queue = InMemoryReadyQueue()
    ready_queue.put(ReadyTask(frame_id="root", node_id="loop"))
    event_queue = queue.Queue()
    graph = SimpleNamespace(nodes={"loop": container_node})
    frame_registry = FrameRegistry()
    frame_registry.register(_execution_frame(frame_id="root", graph=cast(Graph, graph)))
    worker = Worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
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
    assert container_node.body_was_consumed is False


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
    handler = EventHandler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )

    handler.dispatch(TaskEvent(frame_id="root", event=event))

    node_execution.mark_started.assert_called_once_with("run-1")
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
    handler = EventHandler(
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


def test_event_handler_enters_loop_frame_from_container_start() -> None:
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
    event_collector = MagicMock()
    handler = EventHandler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    event = NodeRunStartedEvent(
        id="loop-run",
        node_id="loop",
        node_type=BuiltinNodeTypes.LOOP,
        node_title="Loop",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )

    handler.dispatch(TaskEvent(frame_id="root", event=event))

    collected_events = [call.args[0] for call in event_collector.collect.call_args_list]
    assert any(isinstance(item, NodeRunLoopStartedEvent) for item in collected_events)
    assert ready_queue.get(timeout=0.01) == ReadyTask(
        frame_id="loop-run:loop:0",
        node_id="loop-start",
    )


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
    handler = EventHandler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    loop_started = NodeRunStartedEvent(
        id="loop-run",
        node_id="loop",
        node_type=BuiltinNodeTypes.LOOP,
        node_title="Loop",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    handler.dispatch(TaskEvent(frame_id="root", event=loop_started))
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
    collected_events = [call.args[0] for call in event_collector.collect.call_args_list]
    assert any(isinstance(item, NodeRunLoopSucceededEvent) for item in collected_events)
    assert any(
        isinstance(item, NodeRunSucceededEvent) and item.node_id == "loop"
        for item in collected_events
    )


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
    handler = EventHandler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    loop_started = NodeRunStartedEvent(
        id="loop-run",
        node_id="loop",
        node_type=BuiltinNodeTypes.LOOP,
        node_title="Loop",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    handler.dispatch(TaskEvent(frame_id="root", event=loop_started))
    assert ready_queue.get(timeout=0.01) == ReadyTask(
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

    assert ready_queue.get(timeout=0.01) == ReadyTask(
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

    collected_events = [call.args[0] for call in event_collector.collect.call_args_list]
    assert any(
        isinstance(item, NodeRunLoopNextEvent) and item.index == 1
        for item in collected_events
    )
    assert any(
        isinstance(item, NodeRunLoopSucceededEvent) and item.steps == 2
        for item in collected_events
    )
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
    handler = EventHandler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    handler.dispatch(
        TaskEvent(
            frame_id="root",
            event=NodeRunStartedEvent(
                id="loop-run",
                node_id="loop",
                node_type=BuiltinNodeTypes.LOOP,
                node_title="Loop",
                start_at=datetime.now(UTC).replace(tzinfo=None),
            ),
        ),
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

    collected_events = [call.args[0] for call in event_collector.collect.call_args_list]
    assert any(
        isinstance(item, NodeRunLoopFailedEvent) and item.node_id == "loop"
        for item in collected_events
    )
    assert any(
        isinstance(item, NodeRunFailedEvent) and item.node_id == "loop"
        for item in collected_events
    )
    assert graph_execution.error_message == "loop bad"


def test_event_handler_enters_iteration_frame_from_container_start() -> None:
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
    handler = EventHandler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    event = NodeRunStartedEvent(
        id="iteration-run",
        node_id="iteration",
        node_type=BuiltinNodeTypes.ITERATION,
        node_title="Iteration",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )

    handler.dispatch(TaskEvent(frame_id="root", event=event))

    collected_events = [call.args[0] for call in event_collector.collect.call_args_list]
    assert any(
        isinstance(item, NodeRunIterationStartedEvent) for item in collected_events
    )
    assert ready_queue.get(timeout=0.01) == ReadyTask(
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
    handler = EventHandler(
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
    handler.dispatch(TaskEvent(frame_id="root", event=iteration_started))
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
    assert any(
        isinstance(item, NodeRunIterationSucceededEvent)
        and item.outputs == {"output": ["done"]}
        for item in collected_events
    )
    assert any(
        isinstance(item, NodeRunSucceededEvent)
        and item.node_id == "iteration"
        and item.node_run_result.outputs == {"output": ["done"]}
        for item in collected_events
    )
    assert _variable_value(runtime_state, ["iteration", "output"]) == [
        "done",
    ]


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
    handler = EventHandler(
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
    handler.dispatch(TaskEvent(frame_id="root", event=iteration_started))
    assert ready_queue.get(timeout=0.01) == ReadyTask(
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

    assert ready_queue.get(timeout=0.01) == ReadyTask(
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

    collected_events = [call.args[0] for call in event_collector.collect.call_args_list]
    next_indices = [
        item.index
        for item in collected_events
        if isinstance(item, NodeRunIterationNextEvent) and item.node_id == "iteration"
    ]
    assert next_indices == [0, 1]
    assert child_body_started.in_iteration_id == "iteration"
    assert (
        child_body_started.node_run_result.metadata[
            WorkflowNodeExecutionMetadataKey.ITERATION_INDEX
        ]
        == 1
    )
    assert _variable_value(runtime_state, ["iteration", "output"]) == [
        "first",
        "second",
    ]


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
    handler = EventHandler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, MagicMock()),
        frame_registry=frame_registry,
    )
    handler.dispatch(
        TaskEvent(
            frame_id="root",
            event=NodeRunStartedEvent(
                id="iteration-run",
                node_id="iteration",
                node_type=BuiltinNodeTypes.ITERATION,
                node_title="Iteration",
                start_at=datetime.now(UTC).replace(tzinfo=None),
            ),
        ),
    )

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

    assert _variable_value(runtime_state, ["iteration", "output"]) == [
        ["first"],
        ["second"],
    ]


def test_event_handler_completes_empty_iteration_through_parent_success_path() -> None:
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
    handler = EventHandler(
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

    handler.dispatch(TaskEvent(frame_id="root", event=iteration_started))

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
    handler = EventHandler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )
    handler.dispatch(
        TaskEvent(
            frame_id="root",
            event=NodeRunStartedEvent(
                id="iteration-run",
                node_id="iteration",
                node_type=BuiltinNodeTypes.ITERATION,
                node_title="Iteration",
                start_at=datetime.now(UTC).replace(tzinfo=None),
            ),
        ),
    )
    assert ready_queue.get(timeout=0.01) == ReadyTask(
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
    assert ready_queue.get(timeout=0.01) == ReadyTask(
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

    assert _variable_value(runtime_state, ["iteration", "output"]) == [
        None,
        "ok",
    ]


def test_event_handler_limits_parallel_iteration_and_preserves_output_order() -> None:
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
    handler = EventHandler(
        graph_execution=graph_execution,
        event_collector=cast(EventManager, event_collector),
        frame_registry=frame_registry,
    )

    handler.dispatch(
        TaskEvent(
            frame_id="root",
            event=NodeRunStartedEvent(
                id="iteration-run",
                node_id="iteration",
                node_type=BuiltinNodeTypes.ITERATION,
                node_title="Iteration",
                start_at=datetime.now(UTC).replace(tzinfo=None),
            ),
        ),
    )

    assert ready_queue.get(timeout=0.01) == ReadyTask(
        frame_id="iteration-run:iteration:0",
        node_id="iteration-start",
    )
    assert ready_queue.get(timeout=0.01) == ReadyTask(
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
    assert ready_queue.get(timeout=0.01) == ReadyTask(
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

    assert _variable_value(runtime_state, ["iteration", "output"]) == [
        "first",
        "second",
        "third",
    ]


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
