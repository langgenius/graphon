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
    WorkflowNodeExecutionStatus,
)
from graphon.graph.graph import Graph
from graphon.graph_engine.container_handlers import ContainerHandler
from graphon.graph_engine.domain.graph_execution import GraphExecution
from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_engine.frames import ExecutionFrame, FrameRegistry
from graphon.graph_engine.graph_state_manager import GraphStateManager
from graphon.graph_engine.layers.base import GraphEngineLayer
from graphon.graph_engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.graph_engine.ready_queue.protocol import (
    ResumeTask,
    StartTask,
)
from graphon.graph_engine.worker import Worker
from graphon.graph_events.base import GraphEngineEvent, GraphNodeEventBase
from graphon.graph_events.node import (
    NodeRunFailedEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.node_events.base import NodeRunResult
from graphon.nodes.container_effects import (
    ContainerExecutionResult,
    ContainerRunResult,
    LoopFrameRequest,
)
from graphon.runtime.container_state import ContainerRunState
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool


def _execution_frame(
    *,
    frame_id: str,
    graph: Graph,
    graph_runtime_state: GraphRuntimeState,
) -> ExecutionFrame:
    state_manager = GraphStateManager(graph, graph_runtime_state, frame_id)
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


def test_ready_queue_round_trips_start_and_resume_tasks() -> None:
    queue_ = InMemoryReadyQueue()
    result = ContainerExecutionResult(
        metadata={},
        steps=1,
        node_run_result=NodeRunResult(
            status=WorkflowNodeExecutionStatus.SUCCEEDED,
            inputs={"loop_count": 1},
            outputs={"answer": "ok"},
        ),
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
    assert queue_.qsize() == 0
    restored = InMemoryReadyQueue()
    restored.loads(queue_.dumps())
    assert restored.qsize() == 0


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
            yield LoopFrameRequest(
                inputs={"loop_count": 1},
                outputs={},
                loop_count=1,
                root_node_id="loop-start",
                loop_variable_selectors={},
                loop_node_ids=frozenset(),
                index=0,
            )

        def resume_container(
            self,
            *,
            result: ContainerRunResult,
            started_at: datetime,
        ) -> Generator[GraphNodeEventBase | LoopFrameRequest, None, None]:
            assert isinstance(result, ContainerExecutionResult)
            yield NodeRunSucceededEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                start_at=started_at,
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=result.node_run_result,
            )

    class RecordingContainerHandler:
        def __init__(self) -> None:
            self.invocation_id = ""
            self.requests: list[LoopFrameRequest] = []
            self.request_received = Event()

        def start_await(
            self,
            *,
            invocation_id: str,
            request: LoopFrameRequest,
        ) -> None:
            self.invocation_id = invocation_id
            self.requests.append(request)
            self.request_received.set()

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
        assert container_handler.request_received.wait(timeout=1)
        assert len(container_handler.requests) == 1
        run_state = runtime_state.get_container_run(container_handler.invocation_id)
        assert run_state.frame_id == "root"
        assert run_state.node_id == "loop"
        node_execution = runtime_state.graph_execution.get_or_create_node_execution(
            frame_id=run_state.frame_id,
            node_id=run_state.node_id,
        )
        assert node_execution.execution_id == started.event.id
        assert run_state.started_at == started.event.start_at
        assert layer.end_events == []

        ready_queue.put(
            ResumeTask(
                invocation_id=container_handler.invocation_id,
                result=ContainerExecutionResult(
                    metadata={},
                    steps=1,
                    node_run_result=NodeRunResult(
                        status=WorkflowNodeExecutionStatus.SUCCEEDED,
                        inputs={"loop_count": 1},
                        outputs={"answer": "ok"},
                    ),
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
            result: ContainerRunResult,
            started_at: datetime,
        ) -> Generator[GraphNodeEventBase | LoopFrameRequest, None, None]:
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
            frame_id="parent-frame",
            node_id="loop",
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
            result=ContainerExecutionResult(
                metadata={},
                steps=1,
                node_run_result=NodeRunResult(
                    status=WorkflowNodeExecutionStatus.SUCCEEDED,
                    inputs={"loop_count": 1},
                    outputs={"answer": "ok"},
                ),
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
