import queue
from collections.abc import Generator
from datetime import UTC, datetime
from threading import Event, Lock, Thread
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest

from graphon.engine.event.node_failure import NodeFailureHandler
from graphon.engine.frame import ExecutionFrame, FrameRegistry
from graphon.engine.layer import Layer
from graphon.engine.ready_queue.entities import (
    ResumeTask,
    StartTask,
)
from graphon.engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.engine.scheduler import Scheduler
from graphon.engine.worker import (
    ContainerAwaitTask,
    DispatchTask,
    NodeEventTask,
    Worker,
)
from graphon.engine_events.base import EngineEvent, NodeEvent
from graphon.engine_events.node import (
    NodeRunExceptionEvent,
    NodeRunFailedEvent,
    NodeRunRetryEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.enums import (
    BuiltinNodeTypes,
    ErrorStrategy,
    NodeExecutionType,
    WorkflowNodeExecutionStatus,
)
from graphon.graph.graph import Graph
from graphon.node_events.base import NodeRunResult
from graphon.nodes.base.node import Node
from graphon.nodes.container_effects import (
    ContainerExecutionResult,
    ContainerNodeRunResult,
    ContainerRunResult,
    LoopFrameRequest,
    build_container_value,
)
from graphon.runtime.container_state import create_container_run_state
from graphon.runtime.execution import GraphExecution
from graphon.runtime.graph_runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool


def _execution_frame(
    *,
    frame_id: str,
    graph: Graph,
    graph_runtime_state: RuntimeState,
) -> ExecutionFrame:
    scheduler = Scheduler(graph, graph_runtime_state, frame_id)
    return ExecutionFrame(
        frame_id=frame_id,
        graph=graph,
        state=graph_runtime_state,
        scheduler=scheduler,
        failure_handler=NodeFailureHandler(graph, graph_runtime_state.graph_execution),
    )


def _container_result() -> ContainerExecutionResult:
    return ContainerExecutionResult(
        metadata={},
        steps=1,
        node_run_result=ContainerNodeRunResult(
            status=WorkflowNodeExecutionStatus.SUCCEEDED,
            inputs={"loop_count": build_container_value(1)},
            outputs={"answer": build_container_value("ok")},
        ),
    )


class _RecordingLayer(Layer):
    def __init__(self) -> None:
        super().__init__()
        self.end_events: list[NodeEvent | None] = []

    def on_graph_start(self) -> None:
        return

    def on_event(self, event: EngineEvent) -> None:
        _ = event

    def on_graph_end(self, error: Exception | None) -> None:
        _ = error

    def on_node_run_end(
        self,
        node: object,
        error: Exception | None,
        result_event: NodeEvent | None = None,
    ) -> None:
        _ = node
        _ = error
        self.end_events.append(result_event)


def test_error_handler_preserves_node_execution_but_not_event_id() -> None:
    node = SimpleNamespace(
        retry=True,
        retry_config=SimpleNamespace(max_retries=1, retry_interval_seconds=0),
        error_strategy=None,
        title="Code",
    )
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value.retry_count = 0
    handler = NodeFailureHandler(
        cast(Graph, SimpleNamespace(nodes={"node": node})),
        graph_execution,
    )
    failed = NodeRunFailedEvent(
        id="failed-event",
        node_execution_id="node-run",
        node_id="node",
        node_type=BuiltinNodeTypes.CODE,
        error="failed",
        start_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(
            status=WorkflowNodeExecutionStatus.FAILED,
            error="failed",
        ),
    )

    retry = handler.handle(frame_id="root", event=failed)
    assert isinstance(retry, NodeRunRetryEvent)

    node.retry = False
    node.error_strategy = ErrorStrategy.FAIL_BRANCH
    exception = handler.handle(frame_id="root", event=failed)
    assert isinstance(exception, NodeRunExceptionEvent)

    assert retry.node_execution_id == exception.node_execution_id == "node-run"
    assert len({failed.id, retry.id, exception.id}) == 3


def test_ready_queue_round_trips_start_and_resume_tasks() -> None:
    queue_ = InMemoryReadyQueue()
    result = _container_result()
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
    producer.join(timeout=1)

    assert drained == [first]
    assert unblocked
    assert not producer.is_alive()
    assert queue_.get(timeout=0.01) == second


def test_ready_queue_uses_only_public_queue_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdlib_queue = queue.Queue
    backends: list[queue.Queue[object]] = []

    def public_queue(maxsize: int = 0) -> SimpleNamespace:
        backend: queue.Queue[object] = stdlib_queue(maxsize=maxsize)
        backends.append(backend)
        return SimpleNamespace(
            get=backend.get,
            get_nowait=backend.get_nowait,
            put=backend.put,
            qsize=backend.qsize,
            task_done=backend.task_done,
        )

    monkeypatch.setattr(queue, "Queue", public_queue)
    queue_ = InMemoryReadyQueue()
    first = StartTask(frame_id="root", node_id="a")
    second = StartTask(frame_id="child", node_id="b")
    queue_.put(first)
    queue_.put(second)

    snapshot = queue_.dumps()

    assert queue_.drain() == [first, second]
    queue_.loads(snapshot)
    assert queue_.drain() == [first, second]

    join_thread = Thread(target=backends[0].join, daemon=True)
    join_thread.start()
    join_thread.join(timeout=1)
    assert not join_thread.is_alive()


def test_worker_suspends_and_resumes_container_invocation() -> None:
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
        ) -> Generator[NodeEvent | LoopFrameRequest, object, None]:
            started_at = datetime.now(UTC).replace(tzinfo=None)
            yield NodeRunStartedEvent(
                node_execution_id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title="Loop",
                start_at=started_at,
            )
            self.await_was_reached = True
            yield LoopFrameRequest(
                inputs={"loop_count": build_container_value(1)},
                outputs={},
                loop_count=1,
                root_node_id="loop-start",
                loop_variable_selectors={},
                loop_node_ids=frozenset(),
                index=0,
            )
            self.body_after_await_was_consumed = True

        def resume_container(
            self,
            *,
            result: ContainerRunResult,
            started_at: datetime,
        ) -> Generator[NodeEvent | LoopFrameRequest, None, None]:
            assert isinstance(result, ContainerExecutionResult)
            node_run_result = NodeRunResult(
                status=result.node_run_result.status,
                inputs={
                    key: value.to_object()
                    for key, value in result.node_run_result.inputs.items()
                },
                outputs={
                    key: value.to_object()
                    for key, value in result.node_run_result.outputs.items()
                },
            )
            yield NodeRunSucceededEvent(
                node_execution_id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                start_at=started_at,
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=node_run_result,
            )

    container_node = ContainerNode()
    graph = Graph(
        nodes={"loop": cast(Node, container_node)},
        root_node=cast(Node, container_node),
    )
    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="root", node_id="loop"))
    dispatch_queue: queue.Queue[DispatchTask] = queue.Queue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=graph,
            graph_runtime_state=runtime_state,
        ),
    )
    layer = _RecordingLayer()
    task_claiming = Event()
    task_claiming.set()
    worker = Worker(
        ready_queue=ready_queue,
        dispatch_queue=dispatch_queue,
        frame_registry=frame_registry,
        layers=[layer],
        task_claim_lock=Lock(),
        task_claiming=task_claiming,
    )

    worker.start()
    try:
        started = dispatch_queue.get(timeout=1)
        assert isinstance(started, NodeEventTask)
        assert isinstance(started.event, NodeRunStartedEvent)
        await_task = dispatch_queue.get(timeout=1)
        assert isinstance(await_task, ContainerAwaitTask)
        assert container_node.await_was_reached
        assert not container_node.body_after_await_was_consumed
        run_state = runtime_state.get_container_run(await_task.invocation_id)
        assert run_state.frame_id == "root"
        assert run_state.node_id == "loop"
        node_execution = runtime_state.graph_execution.get_or_create_node_execution(
            frame_id=run_state.frame_id,
            node_id=run_state.node_id,
        )
        assert node_execution.execution_id == started.event.node_execution_id
        assert run_state.started_at == started.event.start_at
        assert layer.end_events == []

        ready_queue.put(
            ResumeTask(
                invocation_id=await_task.invocation_id,
                result=_container_result(),
            ),
        )
        succeeded = dispatch_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(succeeded, NodeEventTask)
    assert isinstance(succeeded.event, NodeRunSucceededEvent)
    assert succeeded.event.node_run_result.outputs == {"answer": "ok"}
    with pytest.raises(KeyError):
        runtime_state.get_container_run(await_task.invocation_id)
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
        ) -> Generator[NodeEvent | LoopFrameRequest, None, None]:
            _ = result
            _ = started_at
            if False:
                yield
            msg = "resume bad"
            raise RuntimeError(msg)

    ready_queue = InMemoryReadyQueue()
    dispatch_queue: queue.Queue[DispatchTask] = queue.Queue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    frame_registry = FrameRegistry()
    container_node = ContainerNode()
    graph_node = cast(Node, container_node)
    parent_graph = Graph(
        nodes={"loop": graph_node},
        root_node=graph_node,
    )
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=parent_graph,
            graph_runtime_state=runtime_state,
        ),
    )
    frame_registry.register(
        _execution_frame(
            frame_id="parent-frame",
            graph=parent_graph,
            graph_runtime_state=runtime_state,
        ),
    )
    started_at = datetime.now(UTC).replace(tzinfo=None)
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
            invocation_id="invocation-1",
            frame_id="parent-frame",
            node_id="loop",
            started_at=started_at,
            request=request,
        ),
    )
    ready_queue.put(
        ResumeTask(
            invocation_id="invocation-1",
            result=_container_result(),
        ),
    )
    task_claiming = Event()
    task_claiming.set()
    worker = Worker(
        ready_queue=ready_queue,
        dispatch_queue=dispatch_queue,
        frame_registry=frame_registry,
        layers=[],
        task_claim_lock=Lock(),
        task_claiming=task_claiming,
    )

    worker.start()
    try:
        failed = dispatch_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(failed, NodeEventTask)
    assert failed.frame_id == "parent-frame"
    assert isinstance(failed.event, NodeRunFailedEvent)
    assert failed.event.error == "resume bad"
    with pytest.raises(KeyError):
        runtime_state.get_container_run("invocation-1")
