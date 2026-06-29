import queue
from collections.abc import Generator
from datetime import UTC, datetime
from threading import Event, Thread
from types import SimpleNamespace
from typing import Any, cast

import pytest

from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    NodeState,
    WorkflowNodeExecutionStatus,
)
from graphon.graph.graph import Graph
from graphon.graph_engine.container_handlers import ContainerHandler
from graphon.graph_engine.domain.graph_execution import GraphExecution
from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_engine.frames import ExecutionFrame, FrameRegistry
from graphon.graph_engine.graph_state_manager import GraphStateManager
from graphon.graph_engine.layers.base import GraphEngineLayer
from graphon.graph_engine.loop_container_handler import LoopContainerHandler
from graphon.graph_engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.graph_engine.ready_queue.protocol import (
    ResumeTask,
    StartTask,
)
from graphon.graph_engine.worker import Worker
from graphon.graph_events.base import GraphEngineEvent, GraphNodeEventBase
from graphon.graph_events.iteration import NodeRunIterationNextEvent
from graphon.graph_events.loop import (
    NodeRunLoopNextEvent,
    NodeRunLoopSucceededEvent,
)
from graphon.graph_events.node import (
    NodeRunFailedEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.node_events.base import NodeRunResult
from graphon.nodes.container_effects import (
    ContainerRunResult,
    IterationFrameRequest,
    IterationFramesRequested,
    LoopExecutionSucceeded,
    LoopFrameCompleted,
    LoopFrameRequest,
)
from graphon.nodes.iteration.entities import ErrorHandleMode, IterationNodeData
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.nodes.loop.entities import LoopNodeData
from graphon.nodes.loop.loop_node import LoopNode
from graphon.runtime.container_state import ContainerRunState
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from tests.helpers import build_graph_init_params


def _execution_frame(
    *,
    frame_id: str,
    graph: Graph,
    graph_runtime_state: GraphRuntimeState,
) -> ExecutionFrame:
    ready_queue = graph_runtime_state.ready_queue
    state_manager = GraphStateManager(graph, ready_queue)
    return ExecutionFrame(
        frame_id=frame_id,
        graph=graph,
        graph_runtime_state=graph_runtime_state,
        state_manager=state_manager,
        edge_processor=cast(Any, SimpleNamespace()),
        error_handler=cast(Any, SimpleNamespace()),
    )


class _RecordingLayer(GraphEngineLayer):
    def __init__(self) -> None:
        super().__init__()
        self.end_events: list[GraphNodeEventBase | None] = []

    def on_graph_start(self) -> None:
        return

    def on_event(self, event: GraphEngineEvent) -> None:
        _ = event

    def on_graph_end(self, error: Exception | None) -> None:
        _ = error

    def on_node_run_end(
        self,
        node: object,
        error: Exception | None,
        result_event: GraphNodeEventBase | None = None,
    ) -> None:
        _ = node
        _ = error
        self.end_events.append(result_event)


class _FrameFactory:
    def with_runtime_state(
        self,
        graph_runtime_state: GraphRuntimeState,
    ) -> "_FrameFactory":
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


def test_ready_queue_round_trips_start_and_resume_tasks() -> None:
    queue_ = InMemoryReadyQueue()
    result = LoopExecutionSucceeded(
        started_at=datetime.now(UTC).replace(tzinfo=None),
        inputs={"loop_count": 1},
        outputs={"answer": "ok"},
        metadata={},
        steps=1,
        node_run_result=NodeRunResult(outputs={"answer": "ok"}),
    )
    queue_.put(StartTask(frame_id="root", node_id="loop"))
    queue_.put(ResumeTask(invocation_id="invocation-1", result=result))

    restored = InMemoryReadyQueue()
    restored.loads(queue_.dumps())

    assert restored.get(timeout=0.01) == StartTask(frame_id="root", node_id="loop")
    assert restored.get(timeout=0.01) == ResumeTask(
        invocation_id="invocation-1",
        result=result,
    )


def test_ready_queue_drain_returns_items_and_empties_queue() -> None:
    queue_ = InMemoryReadyQueue()
    first = StartTask(frame_id="root", node_id="a")
    second = StartTask(frame_id="child", node_id="b")
    queue_.put(first)
    queue_.put(second)

    assert queue_.drain() == [first, second]
    assert queue_.empty()
    restored = InMemoryReadyQueue()
    restored.loads(queue_.dumps())
    assert restored.empty()


def test_ready_queue_drain_notifies_waiting_bounded_queue_producers() -> None:
    queue_ = InMemoryReadyQueue(maxsize=1)
    first = StartTask(frame_id="root", node_id="a")
    second = StartTask(frame_id="child", node_id="b")
    put_done = Event()
    queue_.put(first)

    def put_waiting() -> None:
        queue_.put(second)
        put_done.set()

    producer = Thread(target=put_waiting, daemon=True)
    producer.start()
    assert not put_done.wait(0.01)

    drained = queue_.drain()
    unblocked = put_done.wait(1)
    if not unblocked:
        with queue_._queue.not_full:
            queue_._queue.not_full.notify_all()
    producer.join(timeout=1)

    assert drained == [first]
    assert unblocked
    assert queue_.get(timeout=0.01) == second


def _loop_node() -> LoopNode:
    node = LoopNode(
        node_id="loop",
        data=LoopNodeData.model_validate({
            "type": "loop",
            "title": "Loop",
            "loop_count": 3,
            "start_node_id": "loop-start",
            "break_conditions": [],
            "logical_operator": "and",
            "outputs": {"total": 1},
        }),
        graph_init_params=build_graph_init_params(),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=VariablePool(),
            start_at=1,
        ),
    )
    node.bind_execution_id("loop-run")
    return node


def _iteration_node() -> IterationNode:
    node = IterationNode(
        node_id="iteration",
        data=IterationNodeData.model_validate({
            "type": "iteration",
            "title": "Iteration",
            "start_node_id": "iteration-start",
            "iterator_selector": ["source", "items"],
            "output_selector": ["answer", "text"],
            "error_handle_mode": ErrorHandleMode.TERMINATED,
            "is_parallel": False,
        }),
        graph_init_params=build_graph_init_params(),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=VariablePool(),
            start_at=1,
        ),
    )
    node.bind_execution_id("iteration-run")
    return node


def test_loop_resume_requests_next_frame_after_completed_frame() -> None:
    started_at = datetime.now(UTC).replace(tzinfo=None)
    events = list(
        _loop_node().resume_container(
            phase_data={
                "inputs": {"loop_count": 3},
                "loop_count": 3,
                "root_node_id": "loop-start",
                "loop_variable_selectors": {"acc": ["loop", "acc"]},
                "loop_node_ids": frozenset({"loop-child"}),
            },
            result=LoopFrameCompleted(next_index=1),
            started_at=started_at,
        ),
    )

    assert len(events) == 2
    assert isinstance(events[0], NodeRunLoopNextEvent)
    assert events[0].id == "loop-run"
    assert events[0].index == 1
    assert events[0].pre_loop_output == {"total": 1}
    assert isinstance(events[1], LoopFrameRequest)
    assert events[1].kind == "loop"
    assert events[1].index == 1
    assert events[1].started_at == started_at


def test_loop_resume_finishes_successful_execution() -> None:
    started_at = datetime.now(UTC).replace(tzinfo=None)
    events = list(
        _loop_node().resume_container(
            phase_data={},
            result=LoopExecutionSucceeded(
                started_at=started_at,
                inputs={"loop_count": 3},
                outputs={"answer": "ok"},
                metadata={},
                steps=3,
                node_run_result=NodeRunResult(
                    status=WorkflowNodeExecutionStatus.SUCCEEDED,
                    outputs={"answer": "ok"},
                ),
            ),
            started_at=started_at,
        ),
    )

    assert len(events) == 2
    assert isinstance(events[0], NodeRunLoopSucceededEvent)
    assert events[0].id == "loop-run"
    assert events[0].outputs == {"answer": "ok"}
    assert isinstance(events[1], NodeRunSucceededEvent)
    assert events[1].id == "loop-run"
    assert events[1].node_run_result.outputs == {"answer": "ok"}


def test_iteration_resume_requests_more_frames() -> None:
    started_at = datetime.now(UTC).replace(tzinfo=None)
    events = list(
        _iteration_node().resume_container(
            phase_data={
                "inputs": {"iterator_selector": ["a", "b", "c"]},
                "items": ("a", "b", "c"),
                "root_node_id": "iteration-start",
                "output_selector": ["answer", "text"],
                "error_handle_mode": ErrorHandleMode.TERMINATED,
                "flatten_output": True,
                "parallel_nums": 2,
            },
            result=IterationFramesRequested(indexes=(1, 2)),
            started_at=started_at,
        ),
    )

    assert len(events) == 3
    assert isinstance(events[0], NodeRunIterationNextEvent)
    assert events[0].id == "iteration-run"
    assert events[0].index == 1
    assert isinstance(events[1], NodeRunIterationNextEvent)
    assert events[1].index == 2
    assert isinstance(events[2], IterationFrameRequest)
    assert events[2].kind == "iteration"
    assert events[2].indexes == (1, 2)
    assert events[2].started_at == started_at


def test_worker_suspends_and_resumes_container_invocation() -> None:
    class ContainerNode:
        id = "loop"
        node_type = BuiltinNodeTypes.LOOP
        execution_type = NodeExecutionType.CONTAINER
        execution_id = "run-loop"

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
            result = yield LoopFrameRequest(
                started_at=started_at,
                inputs={"loop_count": 1},
                loop_count=1,
                root_node_id="loop-start",
                loop_variable_selectors={},
                loop_node_ids=frozenset(),
                index=0,
            )
            assert isinstance(result, LoopExecutionSucceeded)
            yield NodeRunSucceededEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                start_at=started_at,
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=result.node_run_result,
            )

        def resume_container(
            self,
            *,
            phase_data: dict[str, object],
            result: ContainerRunResult,
            started_at: datetime,
        ) -> Generator[GraphNodeEventBase | LoopFrameRequest, None, None]:
            _ = phase_data
            assert isinstance(result, LoopExecutionSucceeded)
            yield NodeRunSucceededEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                start_at=started_at,
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=result.node_run_result,
            )

    class RecordingContainerHandler:
        kind = "loop"

        def __init__(self) -> None:
            self.invocation_id = ""
            self.requests: list[LoopFrameRequest] = []

        def start_await(
            self,
            *,
            frame_id: str,
            node_id: str,
            invocation_id: str,
            request: LoopFrameRequest,
        ) -> None:
            assert frame_id == "root"
            assert node_id == "loop"
            self.invocation_id = invocation_id
            self.requests.append(request)

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="root", node_id="loop"))
    event_queue = queue.Queue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    graph = SimpleNamespace(nodes={"loop": ContainerNode()})
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            graph_runtime_state=runtime_state,
        ),
    )
    container_handler = RecordingContainerHandler()
    layer = _RecordingLayer()
    worker = Worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[layer],
        container_handlers={"loop": cast(ContainerHandler, container_handler)},
    )

    worker.start()
    try:
        started = event_queue.get(timeout=1)
        assert isinstance(started, TaskEvent)
        assert isinstance(started.event, NodeRunStartedEvent)
        assert len(container_handler.requests) == 1
        run_state = runtime_state.get_container_run(container_handler.invocation_id)
        assert run_state.frame_id == "root"
        assert run_state.node_id == "loop"
        assert run_state.execution_id == started.event.id
        assert run_state.started_at == started.event.start_at
        assert run_state.phase_data == {
            "inputs": {"loop_count": 1},
            "loop_count": 1,
            "root_node_id": "loop-start",
            "loop_variable_selectors": {},
            "loop_node_ids": (),
        }
        assert layer.end_events == []

        ready_queue.put(
            ResumeTask(
                invocation_id=container_handler.invocation_id,
                result=LoopExecutionSucceeded(
                    started_at=started.event.start_at,
                    inputs={"loop_count": 1},
                    outputs={"answer": "ok"},
                    metadata={},
                    steps=1,
                    node_run_result=NodeRunResult(outputs={"answer": "ok"}),
                ),
            ),
        )
        succeeded = event_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(succeeded, TaskEvent)
    assert isinstance(succeeded.event, NodeRunSucceededEvent)
    assert succeeded.event.node_run_result.outputs == {"answer": "ok"}
    with pytest.raises(KeyError):
        runtime_state.get_container_run(container_handler.invocation_id)
    assert layer.end_events == [succeeded.event]


def test_worker_resumes_terminal_loop_result_from_loop_handler() -> None:
    graph_config = {
        "nodes": [
            {"id": "loop-start", "data": {"type": BuiltinNodeTypes.LOOP_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    event_queue = queue.Queue()
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
    with pytest.raises(KeyError):
        runtime_state.get_container_frame("loop-run:loop:0")
    resume_task = ready_queue.get(timeout=0.01)
    assert isinstance(resume_task, ResumeTask)
    assert isinstance(resume_task.result, LoopExecutionSucceeded)
    ready_queue.put(resume_task)

    worker = Worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
        container_handlers={"loop": loop_handler},
    )
    worker.start()
    try:
        loop_succeeded = event_queue.get(timeout=1)
        node_succeeded = event_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(loop_succeeded, TaskEvent)
    assert isinstance(loop_succeeded.event, NodeRunLoopSucceededEvent)
    assert loop_succeeded.event.id == "loop-run"
    assert isinstance(node_succeeded, TaskEvent)
    assert isinstance(node_succeeded.event, NodeRunSucceededEvent)
    assert node_succeeded.event.id == "loop-run"
    with pytest.raises(KeyError):
        runtime_state.get_container_run("loop-invocation")


def test_worker_reports_resume_failure_on_suspended_invocation_frame() -> None:
    class ContainerNode:
        id = "loop"
        node_type = BuiltinNodeTypes.LOOP
        execution_type = NodeExecutionType.CONTAINER
        execution_id = "run-loop"

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def resume_container(
            self,
            *,
            phase_data: dict[str, object],
            result: ContainerRunResult,
            started_at: datetime,
        ) -> Generator[GraphNodeEventBase | LoopFrameRequest, None, None]:
            _ = phase_data
            _ = result
            _ = started_at
            if False:
                yield
            msg = "resume bad"
            raise RuntimeError(msg)

    ready_queue = InMemoryReadyQueue()
    event_queue = queue.Queue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    frame_registry = FrameRegistry()
    parent_graph = SimpleNamespace(nodes={"loop": ContainerNode()})
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, parent_graph),
            graph_runtime_state=runtime_state,
        ),
    )
    frame_registry.register(
        _execution_frame(
            frame_id="parent-frame",
            graph=cast(Graph, parent_graph),
            graph_runtime_state=runtime_state,
        ),
    )
    started_at = datetime.now(UTC).replace(tzinfo=None)
    runtime_state.put_container_run(
        ContainerRunState(
            invocation_id="invocation-1",
            kind="loop",
            frame_id="parent-frame",
            node_id="loop",
            execution_id="run-loop",
            started_at=started_at,
            phase_data={
                "inputs": {"loop_count": 1},
                "loop_count": 1,
                "root_node_id": "loop-start",
                "loop_variable_selectors": {},
                "loop_node_ids": (),
            },
        )
    )
    ready_queue.put(
        ResumeTask(
            invocation_id="invocation-1",
            result=LoopExecutionSucceeded(
                started_at=started_at,
                inputs={"loop_count": 1},
                outputs={"answer": "ok"},
                metadata={},
                steps=1,
                node_run_result=NodeRunResult(outputs={"answer": "ok"}),
            ),
        ),
    )
    worker = Worker(
        ready_queue=ready_queue,
        event_queue=event_queue,
        frame_registry=frame_registry,
        layers=[],
        container_handlers={},
    )

    worker.start()
    try:
        failed = event_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert failed.frame_id == "parent-frame"
    assert isinstance(failed.event, NodeRunFailedEvent)
    assert failed.event.error == "resume bad"
    with pytest.raises(KeyError):
        runtime_state.get_container_run("invocation-1")
